use std::{env, error::Error, fs, ops::Add, time::Duration, vec};

use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use futures::future::join_all;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    text::Line,
    widgets::{Paragraph, Row, Table},
};
use reqwest::{
    Client, Response,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot::{self, Sender},
};
use tui_spinner::FluxSpinner;

const URL: &str = "https://api.music.apple.com/v1";

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, required = true)]
    file: String,

    #[arg(short, long)]
    prefix: Option<String>,

    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    let file_content = fs::read_to_string(args.file)?;

    setup_tui(file_content, args.name, args.prefix)
}

fn setup_tui(
    file_content: String,
    playlist_name: String,
    prefix: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let terminal = ratatui::init();

    let (tx, rx) = unbounded_channel::<PlaylistData>();

    let song_lines = parse_song_lines(file_content, prefix);

    let app_result = App {
        loading_songs: true,
        playlist_rx: Some(rx),
        playlist_tx: Some(tx),
        song_lines,
        playlist_name,
        ..Default::default()
    }
    .run(terminal);
    ratatui::restore();

    app_result
}

#[derive(Default)]
struct App {
    should_quit: bool,
    loading_songs: bool,
    creating_playlist: bool,
    playlist_created: Option<PlaylistData>,
    tick: u64,
    songs: Vec<Song>,
    playlist_rx: Option<UnboundedReceiver<PlaylistData>>,
    playlist_tx: Option<UnboundedSender<PlaylistData>>,
    song_lines: Vec<String>,
    playlist_name: String,
}

impl App {
    fn run(mut self, mut terminal: DefaultTerminal) -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = oneshot::channel::<Vec<Song>>();
        let client = get_client()?;

        let client_clone = client.clone();
        let lines_clone = self.song_lines.clone();
        tokio::spawn(async move {
            fetch_songs(lines_clone, client_clone, tx).await;
        });

        while !self.should_quit {
            if let Some(playlist_rx) = self.playlist_rx.as_mut() {
                while let Ok(playlist) = playlist_rx.try_recv() {
                    self.creating_playlist = false;
                    self.playlist_created = Some(playlist);
                }
            }

            if let Ok(songs) = rx.try_recv() {
                self.loading_songs = false;
                self.songs = songs;
            }
            if event::poll(Duration::from_millis(80))? {
                self.handle_events()?;
            }
            self.tick = self.tick.wrapping_add(1);
            terminal.draw(|frame| self.draw(frame))?;
        }
        Ok(())
    }

    fn get_header_lines<'a>(&'a self) -> Vec<Line<'a>> {
        if self.loading_songs || self.creating_playlist {
            let spinner = FluxSpinner::new(self.tick).width(24).color(Color::Cyan);
            let mut spinner_line = Line::from({
                if self.loading_songs {
                    "Loading songs"
                } else {
                    "Creating playlist"
                }
            });
            spinner_line.extend(spinner.to_lines().into_iter().flat_map(|l| l.spans));
            return [spinner_line].to_vec();
        }

        if let Some(playlist) = &self.playlist_created {
            let playlist_link = format!("https://music.apple.com/library/playlist/{}", playlist.id);
            return [Line::from(format!(
                "Playlist {} created -> {}",
                playlist.attributes.name, playlist_link
            ))]
            .to_vec();
        }

        [
            Line::from(format!("Playlist Summary: {}", self.playlist_name)),
            Line::from("Press Enter to create the Playlist"),
        ]
        .to_vec()
    }

    fn draw(&self, frame: &mut Frame) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![Constraint::Length(3), Constraint::Percentage(90)]);

        let [header, body] = frame.area().layout(&layout);

        frame.render_widget(
            Paragraph::new(self.get_header_lines()).alignment(Alignment::Center),
            header,
        );

        if !self.loading_songs {
            frame.render_widget(render_songs_table(&self.songs), body);
        }
    }

    fn handle_events(&mut self) -> Result<(), Box<dyn Error>> {
        if let Event::Key(key) = event::read()? {
            if key.kind == KeyEventKind::Press
                && (key.code == KeyCode::Char('q')
                    || (key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL)))
            {
                self.should_quit = true;
            }

            if key.kind == KeyEventKind::Press && key.code == KeyCode::Enter && !self.loading_songs
            {
                self.creating_playlist = true;
                let tx_clone = self.playlist_tx.clone();
                let songs_clone = self.songs.clone();
                let playlist_name = self.playlist_name.clone();
                tokio::spawn(async move {
                    let playlist_res = create_playlist(songs_clone, playlist_name).await.unwrap();
                    if let Some(playlist_data) = playlist_res
                        && let Some(tx) = tx_clone
                    {
                        tx.send(playlist_data).unwrap();
                    }
                });
            }
        }
        Ok(())
    }
}

async fn fetch_songs(song_lines: Vec<String>, client: Client, tx: Sender<Vec<Song>>) {
    let log = song_lines.join("\n");
    let _ = fs::write("tried_queries.log", log);

    let songs_futures = song_lines.iter().map(|song_line| {
        let client_clone = client.clone();
        async move { fetch_single_song(&client_clone, song_line).await }
    });

    let results = join_all(songs_futures).await;

    let mut songs = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(Some(song)) => songs.push(song),
            Ok(None) => {}
            Err(e) => errors.push(e.to_string()),
        }
    }

    if !errors.is_empty() {
        let log = errors.join("\n");
        let _ = fs::write("fetch_errors.log", log);
    }

    tx.send(songs).unwrap()
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct SongAttributes {
    name: String,
    artist_name: String,
    album_name: String,
}

#[derive(Deserialize, Debug, Clone)]
struct Song {
    id: String,
    attributes: SongAttributes,
}

#[derive(Deserialize, Debug)]
struct SongsData {
    data: Vec<Song>,
}

#[derive(Deserialize, Debug)]
struct ApiResonseResults {
    songs: Option<SongsData>,
}

#[derive(Deserialize, Debug)]
struct ApiResponse {
    results: ApiResonseResults,
}

#[derive(Deserialize, Debug)]
struct PlaylistAttributes {
    name: String,
}

#[derive(Deserialize, Debug)]
struct PlaylistData {
    id: String,
    attributes: PlaylistAttributes,
    #[allow(dead_code)]
    href: String,
}

#[derive(Deserialize, Debug)]
struct PlaylistResponse {
    data: Vec<PlaylistData>,
}

#[derive(Serialize, Debug)]
struct SingleTrackData {
    id: String,
    r#type: String,
}

#[derive(Serialize, Debug)]
struct PlaylistRequestTracks {
    data: Vec<SingleTrackData>,
}

#[derive(Serialize, Debug)]
struct PlaylistRequestRelationships {
    tracks: PlaylistRequestTracks,
}

#[derive(Serialize, Debug)]
struct PlaylistRequestAttributes {
    description: String,
    name: String,
}

#[derive(Serialize, Debug)]
struct PlaylistRequest {
    attributes: PlaylistRequestAttributes,
    relationships: PlaylistRequestRelationships,
}

fn get_client() -> Result<Client, Box<dyn Error>> {
    let developer_token = env::var("DEVELOPER_TOKEN").expect("DATABASE_URL must be set");
    let music_user_token = env::var("MUSIC_USER_TOKEN").expect("API_KEY must be set");
    let mut headers = HeaderMap::new();

    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {developer_token}")).unwrap(),
    );
    headers.insert(
        "Music-User-Token",
        HeaderValue::from_str(&music_user_token).unwrap(),
    );

    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .build()?)
}

async fn fetch_single_song(
    client: &Client,
    query: &str,
) -> Result<Option<Song>, Box<dyn Error + Send + Sync>> {
    let response = client
        .get(format!("{URL}/catalog/mx/search"))
        .query(&[
            ("types", "songs"),
            ("term", query),
            ("limit", "1"),
            ("l", "en_US"),
        ])
        .send()
        .await
        .map_err(|e| format!("\"{query}\": request failed: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("\"{query}\": failed to read response body: {e}"))?;

    // println!("{body}");

    if !status.is_success() {
        return Err(format!("\"{query}\": API returned {status}: {body}").into());
    }

    let res: ApiResponse = serde_json::from_str(&body)
        .map_err(|e| format!("\"{query}\": failed to parse response: {e} (body: {body})"))?;

    if let Some(songs) = res.results.songs {
        return Ok(songs.data.into_iter().next());
    }
    Ok(None)
}

#[allow(dead_code)]
async fn debug_response(response: Response) -> Result<(), Box<dyn Error>> {
    let text = response.text().await?;
    match serde_json::from_str::<serde_json::Value>(&text) {
        Ok(json) => println!("{}", serde_json::to_string_pretty(&json)?),
        Err(e) => println!("Some error: {e}, {text}"),
    }

    Ok(())
}

fn render_songs_table<'a>(songs: &'a [Song]) -> Table<'a> {
    let header = Row::new(["Name", "Artist", "Album"])
        .style(Style::new().bold())
        .bottom_margin(1);

    let rows = songs.iter().enumerate().map(|(idx, song)| {
        Row::new([
            idx.add(1).to_string(),
            song.attributes.name.to_string(),
            song.attributes.artist_name.to_string(),
            song.attributes.album_name.to_string(),
        ])
    });

    let widths = [
        Constraint::Length(3),
        Constraint::Fill(1),
        Constraint::Fill(1),
        Constraint::Fill(1),
    ];

    Table::new(rows, widths).header(header)
}

async fn create_playlist(
    songs: Vec<Song>,
    name: String,
) -> Result<Option<PlaylistData>, Box<dyn Error>> {
    let client = get_client()?;

    let playlist_res = client
        .post(format!("{URL}/me/library/playlists"))
        .json(&PlaylistRequest {
            attributes: PlaylistRequestAttributes {
                name,
                description: "generated via CLI".to_string(),
            },
            relationships: PlaylistRequestRelationships {
                tracks: PlaylistRequestTracks {
                    data: songs
                        .iter()
                        .map(|song| SingleTrackData {
                            id: song.id.to_string(),
                            r#type: "songs".to_string(),
                        })
                        .collect(),
                },
            },
        })
        .send()
        .await?
        .json::<PlaylistResponse>()
        .await?;

    Ok(playlist_res.data.into_iter().next())
}

const POSSIBLE_WORDS_SPLITS: &[char] = &[',', ' '];

fn parse_song_lines(file_content: String, prefix: Option<String>) -> Vec<String> {
    let mut song_lines: Vec<String> = Vec::new();
    for line in file_content.lines().filter(|line| !line.is_empty()) {
        // Minor strings cleanup
        let mut splits: Vec<&str> = line
            .split(POSSIBLE_WORDS_SPLITS)
            .filter(|split| !split.is_empty())
            .collect();

        if let Some(prefix) = &prefix {
            splits.insert(0, prefix);
        };

        // Apple music api docs indicates that we should use + to join words,
        // but in practice we get better (if at all) results by using regular
        // spaces
        song_lines.push(splits.join(" "));
    }

    song_lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_song_lines_parsing() {
        let contents = "\
miku, , anamanaguchi
everybody  ,   laughs  

";

        assert_eq!(
            vec!["miku anamanaguchi", "everybody laughs"],
            parse_song_lines(contents.to_string(), None),
        );
    }

    #[test]
    fn prefixed_query() {
        let contents = "\
miku                
prom night
anyway
";

        assert_eq!(
            vec![
                "anamanaguchi miku",
                "anamanaguchi prom night",
                "anamanaguchi anyway"
            ],
            parse_song_lines(contents.to_string(), Some("anamanaguchi".to_string())),
        );
    }
}
