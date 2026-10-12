use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use std::{
    io::{self, stdout},
    time::Duration,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

const RPC_WS_URL: &str = "wss://api.mainnet.solana.com";

struct AppState {
    latest_slot: Option<u64>,

    tps: Option<f64>,

    connection_status: String,

    rpc_endpoint: String,

    recent_events: Vec<String>,
}

impl AppState {
    fn new() -> Self {
        Self {
            latest_slot: None,
            tps: None,
            connection_status: String::from("Connecting..."),
            rpc_endpoint: String::from(RPC_WS_URL),
            recent_events: Vec::new(),
        }
    }
}

enum MonitorEvent {
    Connected,
    Slot(u64),
    Error(String),
    Disconnected,
}

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    enable_raw_mode()?;

    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = AppState::new();

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<MonitorEvent>();
    runtime.spawn(run_slot_monitor(sender));

    let result = run_app(&mut terminal, &mut app, &mut receiver);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    drop(runtime);

    result
}

async fn run_slot_monitor(sender: UnboundedSender<MonitorEvent>) {
    let client = match PubsubClient::new(RPC_WS_URL).await {
        Ok(client) => client,
        Err(error) => {
            let _ = sender.send(MonitorEvent::Error(format!(
                "WebSocket connection failed: {error}"
            )));
            return;
        }
    };

    let (mut slot_stream, unsubscribe) = match client.slot_subscribe().await {
        Ok(subscription) => subscription,
        Err(error) => {
            let _ = sender.send(MonitorEvent::Error(format!(
                "Slot subscription failed: {error}"
            )));
            return;
        }
    };

    let _ = sender.send(MonitorEvent::Connected);

    while let Some(slot_info) = slot_stream.next().await {
        if sender.send(MonitorEvent::Slot(slot_info.slot)).is_err() {
            break;
        }
    }

    drop(slot_stream);
    unsubscribe().await;

    let _ = sender.send(MonitorEvent::Disconnected);
}

fn add_recent_event(app: &mut AppState, message: String) {
    app.recent_events.insert(0, message);

    app.recent_events.truncate(8);
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut AppState,
    receiver: &mut UnboundedReceiver<MonitorEvent>,
) -> Result<()> {
    loop {
        while let Ok(event) = receiver.try_recv() {
            match event {
                MonitorEvent::Connected => {
                    app.connection_status = String::from("Connected");

                    add_recent_event(app, String::from("Slot subscription established"));
                }

                MonitorEvent::Slot(slot) => {
                    app.latest_slot = Some(slot);

                    add_recent_event(app, format!("Slot received: {slot}"));
                }

                MonitorEvent::Error(message) => {
                    app.connection_status = String::from("Connection error");

                    add_recent_event(app, message);
                }

                MonitorEvent::Disconnected => {
                    app.connection_status = String::from("Disconnected");

                    add_recent_event(app, String::from("Slot subscription ended"));
                }
            }
        }

        terminal.draw(|frame| ui(frame, app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn ui(frame: &mut Frame, app: &AppState) {
    let main_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(frame.size());

    let header = Paragraph::new("SOLANA BLOCK MONITOR")
        .style(
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::ALL));

    frame.render_widget(header, main_layout[0]);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(main_layout[1]);

    let left_panels = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(columns[0]);

    let right_panels = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(columns[1]);

    let slot_text = match app.latest_slot {
        Some(slot) => format!("Latest slot: {slot}"),
        None => String::from("Waiting for slot updates..."),
    };

    let slot_panel = Paragraph::new(slot_text).block(
        Block::default()
            .title(" Latest Slot ")
            .borders(Borders::ALL),
    );

    frame.render_widget(slot_panel, left_panels[0]);

    let tps_text = match app.tps {
        Some(tps) => format!("TPS: {tps:.2}"),
        None => String::from("TPS: Not measured yet"),
    };

    let tps_panel = Paragraph::new(tps_text).block(
        Block::default()
            .title(" Network Throughput ")
            .borders(Borders::ALL),
    );

    frame.render_widget(tps_panel, left_panels[1]);

    let activity_text = if app.recent_events.is_empty() {
        String::from("Waiting for network activity...")
    } else {
        app.recent_events.join("\n")
    };

    let activity_panel = Paragraph::new(activity_text).block(
        Block::default()
            .title(" Recent Activity ")
            .borders(Borders::ALL),
    );

    frame.render_widget(activity_panel, right_panels[0]);

    let connection_text = format!(
        "Status: {}\nRPC: {}",
        app.connection_status, app.rpc_endpoint
    );

    let connection_panel = Paragraph::new(connection_text)
        .block(Block::default().title(" Connection ").borders(Borders::ALL));

    frame.render_widget(connection_panel, right_panels[1]);

    let footer = Paragraph::new(" Q: Quit ").block(Block::default().borders(Borders::ALL));

    frame.render_widget(footer, main_layout[2]);
}
