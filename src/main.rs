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
    widgets::{Block, Borders, Paragraph, Sparkline, Wrap},
};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::RpcAccountInfoConfig,
};
use solana_program::program_pack::Pack;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use spl_token::state::{Account as SplTokenAccount, Mint as SplTokenMint};
use std::{
    collections::VecDeque,
    hash::{Hash, Hasher},
    io::{self, stdout},
    time::Duration,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

// ============================================================
// SECTION 1: RPC CONFIGURATION
// ============================================================

const RPC_WS_URL: &str = "wss://api.mainnet-beta.solana.com";
const RPC_HTTP_URL: &str = "https://api.mainnet-beta.solana.com";

// Solana performance samples usually represent about 60 seconds.
const PERFORMANCE_SAMPLE_LIMIT: usize = 10;
const TPS_HISTORY_LIMIT: usize = 30;
const TPS_POLL_INTERVAL_SECS: u64 = 15;

// ============================================================
// SECTION 2: APPLICATION STATE
// The UI reads this state; background tasks send events to update it.
// ============================================================

struct AppState {
    // Latest slot received from the WebSocket.
    latest_slot: Option<u64>,

    // TPS from Solana performance samples, plus chart history.
    tps: Option<f64>,
    non_vote_tps: Option<f64>,
    tps_history: VecDeque<u64>,
    tps_status: String,
    tps_sample_slot: Option<u64>,
    tps_sample_transactions: Option<u64>,
    tps_sample_period_secs: Option<u16>,

    // Slot WebSocket connection information.
    connection_status: String,
    rpc_endpoint: String,

    // Recent events shown in the dashboard.
    recent_events: Vec<String>,

    // Tracked PDA account state.
    target_pda: Option<String>,
    pda_status: String,
    pda_lamports: Option<u64>,
    pda_owner: Option<String>,
    pda_data_len: Option<usize>,
    pda_executable: Option<bool>,
    pda_last_slot: Option<u64>,
    pda_data_hash: Option<u64>,

    // Interactive account selection and raw data preview.
    pda_input_mode: bool,
    pda_input: String,
    pda_data_preview: String,

    // Human-readable fields for supported account layouts.
    pda_decoded_summary: String,
}

impl AppState {
    fn new() -> Self {
        Self {
            latest_slot: None,
            tps: None,
            non_vote_tps: None,
            tps_history: VecDeque::new(),
            tps_status: String::from("Waiting for RPC sample"),
            tps_sample_slot: None,
            tps_sample_transactions: None,
            tps_sample_period_secs: None,
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
            pda_input_mode: false,
            pda_input: String::new(),
            pda_data_preview: String::from("No account data received"),
            pda_decoded_summary: String::from("No decoder available for this account owner"),
        }
    }
}

// ============================================================
// SECTION 3: EVENTS FROM BACKGROUND TASKS TO THE UI
// ============================================================

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
    PerformanceSample {
        slot: u64,
        transactions: u64,
        non_vote_transactions: Option<u64>,
        period_secs: u16,
    },
    PerformanceError(String),
    PdaStatus(String),
    PdaUpdate {
        address: String,
        slot: u64,
        lamports: u64,
        owner: String,
        data_len: usize,
        executable: bool,
        data_hash: u64,
        data_preview: String,
        decoded_summary: String,
    },
}

// ============================================================
// SECTION 4: APPLICATION ENTRY POINT
// ============================================================

fn main() -> Result<()> {
    // Tokio runs the network monitors independently of terminal input.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    // Validate optional PDA settings before taking over the terminal.
    let pda_config = configured_pda()?;

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut app = AppState::new();

    // All network tasks send UI events through this channel.
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<MonitorEvent>();

    // The UI sends new account addresses to this command channel.
    let (pda_command_sender, pda_command_receiver) =
        tokio::sync::mpsc::unbounded_channel::<Pubkey>();

    // Start the live slot monitor and TPS sampler.
    runtime.spawn(run_slot_monitor(sender.clone()));
    runtime.spawn(run_tps_monitor(sender.clone()));

    // Preserve an optional PDA configured through environment variables.
    let initial_pda = if let Some((pda, bump)) = pda_config {
        app.target_pda = Some(pda.to_string());
        app.pda_status = String::from("Connecting...");
        add_recent_event(&mut app, format!("Derived PDA: {pda} (bump: {bump})"));
        Some(pda)
    } else {
        None
    };

    // This manager can switch account subscriptions when the user enters
    // a new address inside the TUI.
    runtime.spawn(run_pda_manager(
        initial_pda,
        pda_command_receiver,
        sender.clone(),
    ));

    let result = run_app(&mut terminal, &mut app, &mut receiver, &pda_command_sender);

    // Restore the terminal after the UI loop exits.
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    drop(runtime);
    result
}

// ============================================================
// SECTION 5: CONFIGURE THE PDA
// This sample supports one UTF-8 seed of at most 32 bytes.
// It must match the seed scheme used by the target program.
// ============================================================

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

// ============================================================
// SECTION 6: SLOT MONITOR WITH RECONNECTION
// This task only handles WebSocket slot notifications. TPS is
// fetched independently so we don't make an HTTP request per slot.
// ============================================================

async fn run_slot_monitor(sender: UnboundedSender<MonitorEvent>) {
    let mut retry_delay_secs = 1_u64;
    let mut attempt = 0_u32;

    loop {
        attempt = attempt.saturating_add(1);

        if sender.send(MonitorEvent::Connecting { attempt }).is_err() {
            return;
        }

        match monitor_slots_once(&sender).await {
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

// One lifecycle for the slot WebSocket subscription.
async fn monitor_slots_once(sender: &UnboundedSender<MonitorEvent>) -> Result<()> {
    let client = PubsubClient::new(RPC_WS_URL)
        .await
        .context("Slot WebSocket connection failed")?;

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
        if sender.send(MonitorEvent::Slot(slot_info.slot)).is_err() {
            drop(slot_stream);
            unsubscribe().await;
            return Ok(());
        }
    }

    drop(slot_stream);
    unsubscribe().await;
    let _ = sender.send(MonitorEvent::Disconnected);
    anyhow::bail!("Slot subscription stream ended unexpectedly")
}

// ============================================================
// SECTION 7: TPS SAMPLER
// getRecentPerformanceSamples returns historical performance samples,
// generally about 60 seconds each. Polling retrieves new samples without
// making one getBlock request for every slot.
// ============================================================

async fn run_tps_monitor(sender: UnboundedSender<MonitorEvent>) {
    let client = RpcClient::new(RPC_HTTP_URL.to_string());
    let mut last_sample_slot: Option<u64> = None;

    loop {
        match client
            .get_recent_performance_samples(Some(PERFORMANCE_SAMPLE_LIMIT))
            .await
        {
            Ok(mut samples) => {
                // RPC returns samples newest first; process them oldest first.
                samples.reverse();

                for sample in samples {
                    if sample.sample_period_secs == 0 {
                        continue;
                    }

                    // Avoid adding the same historical sample on every poll.
                    if last_sample_slot.is_some_and(|last| sample.slot <= last) {
                        continue;
                    }

                    let event = MonitorEvent::PerformanceSample {
                        slot: sample.slot,
                        transactions: sample.num_transactions,
                        non_vote_transactions: sample.num_non_vote_transactions,
                        period_secs: sample.sample_period_secs,
                    };

                    if sender.send(event).is_err() {
                        return;
                    }

                    last_sample_slot = Some(sample.slot);
                }
            }
            Err(error) => {
                if sender
                    .send(MonitorEvent::PerformanceError(format!("{error:#}")))
                    .is_err()
                {
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(TPS_POLL_INTERVAL_SECS)).await;
    }
}

// ============================================================
// SECTION 8: PDA ACCOUNT SELECTION MANAGER
//
// The manager owns the current subscription task. When the user enters
// another account address, the old task is cancelled and a new one starts.
// ============================================================

async fn run_pda_manager(
    initial_pda: Option<Pubkey>,
    mut commands: UnboundedReceiver<Pubkey>,
    sender: UnboundedSender<MonitorEvent>,
) {
    let mut current_task = initial_pda.map(|pda| {
        let event_sender = sender.clone();
        tokio::spawn(run_pda_monitor(pda, event_sender))
    });

    while let Some(pda) = commands.recv().await {
        if let Some(task) = current_task.take() {
            task.abort();
        }

        if sender
            .send(MonitorEvent::PdaStatus(String::from(
                "Switching account...",
            )))
            .is_err()
        {
            return;
        }

        current_task = Some(tokio::spawn(run_pda_monitor(pda, sender.clone())));
    }

    if let Some(task) = current_task {
        task.abort();
    }
}

// ============================================================
// SECTION 9: PDA MONITOR WITH RECONNECTION
// ============================================================

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
        let owner = account.owner;
        let decoded_summary = decode_account_data(&owner, &data);

        // Show the first 32 bytes as hex. Known classic SPL Token layouts are
        // decoded below; other programs retain this generic raw-data preview.
        let preview_bytes = data
            .iter()
            .take(32)
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        let data_preview = if data.is_empty() {
            String::from("(empty account data)")
        } else if data.len() > 32 {
            format!("{preview_bytes} ...")
        } else {
            preview_bytes
        };

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        data.hash(&mut hasher);
        let data_hash = hasher.finish();

        let update = MonitorEvent::PdaUpdate {
            address: pda.to_string(),
            slot: response.context.slot,
            lamports: account.lamports,
            owner,
            data_len,
            executable: account.executable,
            data_hash,
            data_preview,
            decoded_summary,
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

// ============================================================
// SECTION 9: CLASSIC SPL TOKEN ACCOUNT DECODER
//
// This first decoder supports accounts owned by the original SPL Token
// program: token accounts and mint accounts. Other program layouts (and
// Token-2022 extensions) remain available as raw hex until we add a decoder.
// ============================================================

fn decode_account_data(program_owner: &str, data: &[u8]) -> String {
    if program_owner != spl_token::id().to_string() {
        return String::from(
            "No decoder registered for this program.\nRaw account bytes are shown above.",
        );
    }

    // Classic SPL Token account layout: 165 bytes.
    if data.len() == SplTokenAccount::LEN {
        return match SplTokenAccount::unpack_unchecked(data) {
            Ok(account) => format!(
                "Type: Classic SPL Token Account\nMint: {}\nToken authority: {}\nAmount: {} base units\nState: {:?}",
                account.mint, account.owner, account.amount, account.state,
            ),
            Err(error) => format!("SPL Token account decode failed: {error}"),
        };
    }

    // Classic SPL Token mint layout: 82 bytes.
    if data.len() == SplTokenMint::LEN {
        return match SplTokenMint::unpack_unchecked(data) {
            Ok(mint) => format!(
                "Type: Classic SPL Token Mint\nDecimals: {}\nSupply: {}\nInitialized: {}\nMint authority present: {}\nFreeze authority present: {}",
                mint.decimals,
                format_token_amount(mint.supply, mint.decimals),
                mint.is_initialized,
                mint.mint_authority.is_some(),
                mint.freeze_authority.is_some(),
            ),
            Err(error) => format!("SPL Token mint decode failed: {error}"),
        };
    }

    format!(
        "Classic SPL Token program, unsupported layout ({} bytes).",
        data.len()
    )
}

// Format a base-unit token amount without floating-point rounding.
fn format_token_amount(amount: u64, decimals: u8) -> String {
    let decimals = decimals as usize;
    if decimals == 0 {
        return amount.to_string();
    }

    let digits = amount.to_string();
    if digits.len() <= decimals {
        let leading_zeroes = "0".repeat(decimals - digits.len());
        format!("0.{leading_zeroes}{digits}")
    } else {
        let split = digits.len() - decimals;
        format!("{}.{}", &digits[..split], &digits[split..])
    }
}

// ============================================================
// SECTION 10: APPLICATION STATE HELPERS
// ============================================================

fn add_recent_event(app: &mut AppState, message: String) {
    app.recent_events.insert(0, message);
    app.recent_events.truncate(8);
}

// ============================================================
// SECTION 11: MAIN UI LOOP
// ============================================================

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut AppState,
    receiver: &mut UnboundedReceiver<MonitorEvent>,
    pda_command_sender: &UnboundedSender<Pubkey>,
) -> Result<()> {
    loop {
        // Process all messages currently waiting in the channel.
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
                    add_recent_event(app, format!("Slot connection error: {message}"));
                }
                MonitorEvent::Reconnecting { delay_secs } => {
                    app.connection_status = format!("Reconnecting in {delay_secs}s");
                    add_recent_event(app, format!("Slot retry scheduled in {delay_secs}s"));
                }
                MonitorEvent::Disconnected => {
                    app.connection_status = String::from("Disconnected");
                    add_recent_event(app, String::from("Slot listener stopped"));
                }
                MonitorEvent::PerformanceSample {
                    slot,
                    transactions,
                    non_vote_transactions,
                    period_secs,
                } => {
                    let period = f64::from(period_secs);
                    app.tps = Some(transactions as f64 / period);
                    app.non_vote_tps = non_vote_transactions.map(|count| count as f64 / period);
                    app.tps_sample_slot = Some(slot);
                    app.tps_sample_transactions = Some(transactions);
                    app.tps_sample_period_secs = Some(period_secs);
                    app.tps_status = String::from("RPC performance sample received");

                    let rounded_tps = app.tps.unwrap_or_default().round() as u64;
                    app.tps_history.push_back(rounded_tps);
                    while app.tps_history.len() > TPS_HISTORY_LIMIT {
                        app.tps_history.pop_front();
                    }

                    add_recent_event(
                        app,
                        format!(
                            "TPS sample: {transactions} tx / {period_secs}s = {:.1} TPS",
                            app.tps.unwrap_or_default()
                        ),
                    );
                }
                MonitorEvent::PerformanceError(message) => {
                    app.tps_status = String::from("RPC sample request failed");
                    add_recent_event(app, format!("TPS RPC error: {message}"));
                }
                MonitorEvent::PdaStatus(status) => {
                    app.pda_status = status;
                }
                MonitorEvent::PdaUpdate {
                    address,
                    slot,
                    lamports,
                    owner,
                    data_len,
                    executable,
                    data_hash,
                    data_preview,
                    decoded_summary,
                } => {
                    // Ignore queued updates from an account we have switched away from.
                    if app.target_pda.as_deref() != Some(address.as_str()) {
                        continue;
                    }

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
                    app.pda_data_preview = data_preview;
                    app.pda_decoded_summary = decoded_summary;

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

        terminal.draw(|frame| ui(frame, app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if app.pda_input_mode {
                    // In input mode, every typed character belongs to the address.
                    match key.code {
                        KeyCode::Esc => {
                            app.pda_input_mode = false;
                            app.pda_input.clear();
                            app.pda_status = String::from("Input cancelled");
                        }
                        KeyCode::Backspace => {
                            app.pda_input.pop();
                        }
                        KeyCode::Enter => {
                            let entered = app.pda_input.trim().to_string();
                            match entered.parse::<Pubkey>() {
                                Ok(pda) => {
                                    let address = pda.to_string();
                                    if pda_command_sender.send(pda).is_ok() {
                                        app.target_pda = Some(address.clone());
                                        app.pda_status = String::from("Switching account...");
                                        app.pda_lamports = None;
                                        app.pda_owner = None;
                                        app.pda_data_len = None;
                                        app.pda_executable = None;
                                        app.pda_last_slot = None;
                                        app.pda_data_hash = None;
                                        app.pda_data_preview =
                                            String::from("Waiting for account data...");
                                        app.pda_decoded_summary =
                                            String::from("Waiting for account data...");
                                        add_recent_event(
                                            app,
                                            format!("Requested account watch: {address}"),
                                        );
                                        app.pda_input.clear();
                                        app.pda_input_mode = false;
                                    } else {
                                        app.pda_status =
                                            String::from("Account monitor unavailable");
                                    }
                                }
                                Err(_) => {
                                    app.pda_status =
                                        String::from("Invalid Solana address; try again");
                                }
                            }
                        }
                        KeyCode::Char(character) if app.pda_input.len() < 44 => {
                            app.pda_input.push(character);
                        }
                        _ => {}
                    }
                } else {
                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('a') => {
                            app.pda_input.clear();
                            app.pda_input_mode = true;
                            app.pda_status = String::from("Enter account address");
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(())
}

// ============================================================
// SECTION 12: DASHBOARD UI
// ============================================================

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
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(20),
            Constraint::Percentage(55),
        ])
        .split(columns[1]);

    // Latest slot panel.
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

    // Split the throughput panel into a numeric summary and a sparkline.
    let tps_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(3)])
        .split(left_panels[1]);

    let total_tps = app
        .tps
        .map(|value| format!("{value:.1} TPS"))
        .unwrap_or_else(|| String::from("Waiting for sample"));
    let non_vote_tps = app
        .non_vote_tps
        .map(|value| format!("{value:.1} TPS"))
        .unwrap_or_else(|| String::from("Unavailable"));
    let sample_details = match (app.tps_sample_transactions, app.tps_sample_period_secs) {
        (Some(transactions), Some(period)) => format!("Sample: {transactions} tx / {period}s"),
        _ => app.tps_status.clone(),
    };
    let tps_summary = Paragraph::new(format!(
        "Total: {total_tps}\nNon-vote: {non_vote_tps}\n{sample_details}"
    ))
    .block(
        Block::default()
            .title(" Network Throughput ")
            .borders(Borders::ALL),
    );
    frame.render_widget(tps_summary, tps_layout[0]);

    // Ratatui Sparkline visualizes recent RPC TPS samples.
    let history: Vec<u64> = app.tps_history.iter().copied().collect();
    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .title(" TPS History (about 60s per sample) ")
                .borders(Borders::ALL),
        )
        .data(history.as_slice())
        .style(Style::default().fg(Color::Green));
    frame.render_widget(sparkline, tps_layout[1]);

    // Recent activity panel.
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

    // Connection panel.
    let connection_text = format!(
        "Status: {}\nWebSocket: {}",
        app.connection_status, app.rpc_endpoint
    );
    let connection_panel = Paragraph::new(connection_text)
        .block(Block::default().title(" Connection ").borders(Borders::ALL));
    frame.render_widget(connection_panel, right_panels[1]);

    // PDA metadata panel. Give the PDA area more vertical space for
    // the decoded account fields below it.
    let pda_lamports = app
        .pda_lamports
        .map(|lamports| {
            format!(
                "{:.9} SOL ({lamports} lamports)",
                lamports as f64 / 1_000_000_000.0
            )
        })
        .unwrap_or_else(|| String::from("--"));

    let pda_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(5)])
        .split(right_panels[2]);

    let pda_metadata = format!(
        "Address: {}\nStatus: {}\nBalance: {}\nProgram owner: {}\nData: {} bytes | Executable: {}\nLast update slot: {}",
        app.target_pda.as_deref().unwrap_or("Not configured"),
        app.pda_status,
        pda_lamports,
        app.pda_owner.as_deref().unwrap_or("--"),
        app.pda_data_len
            .map(|value| value.to_string())
            .unwrap_or_else(|| "--".into()),
        app.pda_executable
            .map(|value| value.to_string())
            .unwrap_or_else(|| "--".into()),
        app.pda_last_slot
            .map(|value| value.to_string())
            .unwrap_or_else(|| "--".into()),
    );
    let pda_panel = Paragraph::new(pda_metadata)
        .block(
            Block::default()
                .title(" PDA Account ")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(pda_panel, pda_layout[0]);

    // Decoded fields are shown when the account belongs to a supported
    // program and its data matches a known layout. Raw bytes remain visible.
    let decoded_text = format!(
        "{}\n\nRaw data (first 32 bytes): {}",
        app.pda_decoded_summary, app.pda_data_preview,
    );
    let decoded_panel = Paragraph::new(decoded_text)
        .block(
            Block::default()
                .title(" Decoded Account Data ")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(decoded_panel, pda_layout[1]);

    let footer_text = if app.pda_input_mode {
        format!(
            "Account address: {} | Enter: Watch | Esc: Cancel",
            app.pda_input
        )
    } else {
        String::from(" A: Watch account/PDA address | Q: Quit ")
    };
    let footer = Paragraph::new(footer_text).block(Block::default().borders(Borders::ALL));
    frame.render_widget(footer, main_layout[2]);
}
