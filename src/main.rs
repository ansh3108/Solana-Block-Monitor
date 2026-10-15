use anyhow::{Context, Result};
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
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::{RpcAccountInfoConfig, RpcBlockConfig},
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use std::{
    collections::VecDeque,
    hash::{Hash, Hasher},
    io::{self, stdout},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{
    Semaphore,
    mpsc::{UnboundedReceiver, UnboundedSender},
};

const RPC_WS_URL: &str = "wss://api.mainnet-beta.solana.com";
const RPC_HTTP_URL: &str = "https://api.mainnet-beta.solana.com";
const TPS_WINDOW_SECS: u64 = 10;

struct AppState {
    latest_slot: Option<u64>,

    tps: Option<f64>,
    tps_samples: VecDeque<(Instant, usize)>,
    first_tps_sample_at: Option<Instant>,

    connection_status: String,
    rpc_endpoint: String,

    recent_events: Vec<String>,

    target_pda: Option<String>,
    pda_status: String,
    pda_lamports: Option<u64>,
    pda_owner: Option<String>,
    pda_data_len: Option<usize>,
    pda_executable: Option<bool>,
    pda_last_slot: Option<u64>,
    pda_data_hash: Option<u64>,
}

impl AppState {
    fn new() -> Self {
        Self {
            latest_slot: None,
            tps: None,
            tps_samples: VecDeque::new(),
            first_tps_sample_at: None,
            connection_status: String::from("Connecting..."),
            rpc_endpoint: String::from(RPC_WS_URL),
            recent_events: Vec::new(),
            target_pda: None,
            pda_status: String::from("Not configured"),
            pda_lamports: None,
            pda_owner: None,
            pda_data_len: None,
            pda_executable: None,
            pda_last_slot: None,
            pda_data_hash: None,
        }
    }
}

enum MonitorEvent {
    Connecting {
        attempt: u32,
    },
    Connected,
    Slot(u64),
    Error(String),
    Reconnecting {
        delay_secs: u64,
    },
    Disconnected,
    BlockTransactions {
        slot: u64,
        count: usize,
    },
    PdaStatus(String),
    PdaUpdate {
        slot: u64,
        lamports: u64,
        owner: String,
        data_len: usize,
        executable: bool,
        data_hash: u64,
    },
}

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let pda_config = configured_pda()?;

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut app = AppState::new();

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<MonitorEvent>();

    runtime.spawn(run_slot_monitor(sender.clone()));

    if let Some((pda, bump)) = pda_config {
        app.target_pda = Some(pda.to_string());
        app.pda_status = String::from("Connecting...");
        add_recent_event(&mut app, format!("Derived PDA: {pda} (bump: {bump})"));
        runtime.spawn(run_pda_monitor(pda, sender.clone()));
    }

    let result = run_app(&mut terminal, &mut app, &mut receiver);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    drop(runtime);
    result
}

fn configured_pda() -> Result<Option<(Pubkey, u8)>> {
    let program_id = match std::env::var("PDA_PROGRAM_ID") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => {
            if std::env::var("PDA_SEED").is_ok() {
                anyhow::bail!("PDA_SEED is set, but PDA_PROGRAM_ID is missing");
            }
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };

    let seed =
        std::env::var("PDA_SEED").context("PDA_PROGRAM_ID is set, but PDA_SEED is missing")?;

    if seed.as_bytes().len() > 32 {
        anyhow::bail!("PDA_SEED must be 32 bytes or fewer");
    }

    let program_id = program_id
        .parse::<Pubkey>()
        .context("PDA_PROGRAM_ID is not a valid public key")?;

    let (pda, bump) = Pubkey::find_program_address(&[seed.as_bytes()], &program_id);
    Ok(Some((pda, bump)))
}

async fn run_slot_monitor(sender: UnboundedSender<MonitorEvent>) {
    let rpc_client = Arc::new(RpcClient::new(RPC_HTTP_URL.to_string()));
    let block_fetch_semaphore = Arc::new(Semaphore::new(4));

    let mut retry_delay_secs = 1_u64;
    let mut attempt = 0_u32;

    loop {
        attempt = attempt.saturating_add(1);

        if sender.send(MonitorEvent::Connecting { attempt }).is_err() {
            return;
        }

        let result = monitor_once(
            &sender,
            Arc::clone(&rpc_client),
            Arc::clone(&block_fetch_semaphore),
        )
        .await;

        match result {
            Ok(()) => return,
            Err(error) => {
                if sender
                    .send(MonitorEvent::Error(format!("{error:#}")))
                    .is_err()
                {
                    return;
                }
            }
        }

        if sender
            .send(MonitorEvent::Reconnecting {
                delay_secs: retry_delay_secs,
            })
            .is_err()
        {
            return;
        }

        tokio::time::sleep(Duration::from_secs(retry_delay_secs)).await;
        retry_delay_secs = (retry_delay_secs * 2).min(30);
    }
}

async fn monitor_once(
    sender: &UnboundedSender<MonitorEvent>,
    rpc_client: Arc<RpcClient>,
    block_fetch_semaphore: Arc<Semaphore>,
) -> Result<()> {
    let client = PubsubClient::new(RPC_WS_URL)
        .await
        .context("WebSocket connection failed")?;

    let (mut slot_stream, unsubscribe) = client
        .slot_subscribe()
        .await
        .context("Slot subscription failed")?;

    if sender.send(MonitorEvent::Connected).is_err() {
        drop(slot_stream);
        unsubscribe().await;
        return Ok(());
    }

    while let Some(slot_info) = slot_stream.next().await {
        let slot = slot_info.slot;

        if sender.send(MonitorEvent::Slot(slot)).is_err() {
            break;
        }

        let rpc = Arc::clone(&rpc_client);
        let semaphore = Arc::clone(&block_fetch_semaphore);
        let event_sender = sender.clone();

        tokio::spawn(async move {
            let _permit = match semaphore.acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            };

            for retry in 0..5_u64 {
                if retry > 0 {
                    tokio::time::sleep(Duration::from_millis(250 * retry)).await;
                }

                let config = RpcBlockConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    transaction_details: Some(TransactionDetails::Signatures),
                    rewards: Some(false),
                    commitment: Some(CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                };

                match rpc.get_block_with_config(slot, config).await {
                    Ok(block) => {
                        let count = match block.signatures.as_ref() {
                            Some(signatures) => signatures.len(),
                            None => match block.transactions.as_ref() {
                                Some(transactions) => transactions.len(),
                                None => continue,
                            },
                        };

                        let _ = event_sender.send(MonitorEvent::BlockTransactions { slot, count });
                        return;
                    }
                    Err(_) => {}
                }
            }
        });
    }

    drop(slot_stream);
    unsubscribe().await;
    let _ = sender.send(MonitorEvent::Disconnected);
    anyhow::bail!("Slot subscription stream ended unexpectedly")
}

async fn run_pda_monitor(pda: Pubkey, sender: UnboundedSender<MonitorEvent>) {
    let mut retry_delay_secs = 1_u64;

    loop {
        if sender
            .send(MonitorEvent::PdaStatus(String::from("Connecting...")))
            .is_err()
        {
            return;
        }

        match monitor_pda_once(&pda, &sender).await {
            Ok(()) => return,
            Err(error) => {
                let status = format!("Retrying in {retry_delay_secs}s: {error:#}");
                if sender.send(MonitorEvent::PdaStatus(status)).is_err() {
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(retry_delay_secs)).await;
        retry_delay_secs = (retry_delay_secs * 2).min(30);
    }
}

async fn monitor_pda_once(pda: &Pubkey, sender: &UnboundedSender<MonitorEvent>) -> Result<()> {
    let client = PubsubClient::new(RPC_WS_URL)
        .await
        .context("PDA WebSocket connection failed")?;

    let config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        data_slice: None,
        commitment: Some(CommitmentConfig::confirmed()),
        min_context_slot: None,
    };

    let (mut stream, unsubscribe) = client
        .account_subscribe(pda, Some(config))
        .await
        .context("PDA account subscription failed")?;

    if sender
        .send(MonitorEvent::PdaStatus(String::from("Subscribed")))
        .is_err()
    {
        drop(stream);
        unsubscribe().await;
        return Ok(());
    }

    while let Some(response) = stream.next().await {
        let account = response.value;
        let data = account.data.decode().unwrap_or_default();
        let data_len = data.len();

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        data.hash(&mut hasher);
        let data_hash = hasher.finish();

        let update = MonitorEvent::PdaUpdate {
            slot: response.context.slot,
            lamports: account.lamports,
            owner: account.owner,
            data_len,
            executable: account.executable,
            data_hash,
        };

        if sender.send(update).is_err() {
            drop(stream);
            unsubscribe().await;
            return Ok(());
        }
    }

    drop(stream);
    unsubscribe().await;
    anyhow::bail!("PDA account subscription ended unexpectedly")
}

fn add_recent_event(app: &mut AppState, message: String) {
    app.recent_events.insert(0, message);
    app.recent_events.truncate(8);
}

fn refresh_tps(app: &mut AppState) {
    let now = Instant::now();
    let window = Duration::from_secs(TPS_WINDOW_SECS);

    while let Some((sample_time, _)) = app.tps_samples.front() {
        if now.duration_since(*sample_time) <= window {
            break;
        }
        app.tps_samples.pop_front();
    }

    let Some(first_sample_at) = app.first_tps_sample_at else {
        app.tps = None;
        return;
    };

    let elapsed_secs = now
        .duration_since(first_sample_at)
        .as_secs_f64()
        .min(window.as_secs_f64())
        .max(1.0);

    let total_transactions: usize = app.tps_samples.iter().map(|(_, count)| *count).sum();

    app.tps = Some(total_transactions as f64 / elapsed_secs);
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut AppState,
    receiver: &mut UnboundedReceiver<MonitorEvent>,
) -> Result<()> {
    loop {
        while let Ok(event) = receiver.try_recv() {
            match event {
                MonitorEvent::Connecting { attempt } => {
                    app.connection_status = format!("Connecting (attempt {attempt})");
                }
                MonitorEvent::Connected => {
                    app.connection_status = String::from("Connected");
                    add_recent_event(app, String::from("Slot subscription established"));
                }
                MonitorEvent::Slot(slot) => {
                    app.latest_slot = Some(slot);
                }
                MonitorEvent::Error(message) => {
                    add_recent_event(app, format!("Connection error: {message}"));
                }
                MonitorEvent::Reconnecting { delay_secs } => {
                    app.connection_status = format!("Reconnecting in {delay_secs}s");
                    add_recent_event(app, format!("Retry scheduled in {delay_secs}s"));
                }
                MonitorEvent::Disconnected => {
                    app.connection_status = String::from("Disconnected");
                    add_recent_event(app, String::from("Slot listener stopped"));
                }
                MonitorEvent::BlockTransactions { slot, count } => {
                    let now = Instant::now();
                    if app.first_tps_sample_at.is_none() {
                        app.first_tps_sample_at = Some(now);
                    }
                    app.tps_samples.push_back((now, count));
                    add_recent_event(app, format!("Block {slot}: {count} transactions"));
                }
                MonitorEvent::PdaStatus(status) => {
                    app.pda_status = status;
                }
                MonitorEvent::PdaUpdate {
                    slot,
                    lamports,
                    owner,
                    data_len,
                    executable,
                    data_hash,
                } => {
                    let is_first_update = app.pda_last_slot.is_none();
                    let changed = app.pda_lamports != Some(lamports)
                        || app.pda_owner.as_deref() != Some(owner.as_str())
                        || app.pda_data_len != Some(data_len)
                        || app.pda_executable != Some(executable)
                        || app.pda_data_hash != Some(data_hash);

                    app.pda_lamports = Some(lamports);
                    app.pda_owner = Some(owner);
                    app.pda_data_len = Some(data_len);
                    app.pda_executable = Some(executable);
                    app.pda_last_slot = Some(slot);
                    app.pda_data_hash = Some(data_hash);

                    if changed {
                        let label = if is_first_update {
                            "Initial PDA state"
                        } else {
                            "PDA state changed"
                        };
                        add_recent_event(
                            app,
                            format!(
                                "{label} at slot {slot}: {lamports} lamports, {data_len} bytes"
                            ),
                        );
                    }
                }
            }
        }

        refresh_tps(app);
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

    // Three right-side panels: activity, connection and PDA state.
    let right_panels = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(35),
            Constraint::Percentage(25),
            Constraint::Percentage(40),
        ])
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
        Some(tps) => format!("Observed TPS: {tps:.2}\nRolling window: up to 10s"),
        None => String::from("Waiting for block transaction data..."),
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

    let pda_text = format!(
        "Address: {}\nStatus: {}\nLamports: {}\nData size: {} bytes\nOwner: {}\nExecutable: {}\nLast update slot: {}",
        app.target_pda.as_deref().unwrap_or("Not configured"),
        app.pda_status,
        app.pda_lamports
            .map(|v| v.to_string())
            .unwrap_or_else(|| "--".into()),
        app.pda_data_len
            .map(|v| v.to_string())
            .unwrap_or_else(|| "--".into()),
        app.pda_owner.as_deref().unwrap_or("--"),
        app.pda_executable
            .map(|v| v.to_string())
            .unwrap_or_else(|| "--".into()),
        app.pda_last_slot
            .map(|v| v.to_string())
            .unwrap_or_else(|| "--".into()),
    );
    let pda_panel = Paragraph::new(pda_text).block(
        Block::default()
            .title(" PDA Account ")
            .borders(Borders::ALL),
    );
    frame.render_widget(pda_panel, right_panels[2]);

    let footer = Paragraph::new(" Q: Quit ").block(Block::default().borders(Borders::ALL));
    frame.render_widget(footer, main_layout[2]);
}
