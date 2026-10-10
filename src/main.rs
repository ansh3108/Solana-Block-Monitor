use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};
use std::{
    io::{self, stdout},
    time::Duration,
};

fn main() -> Result<()> {
    enable_raw_mode()?;

    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    loop {
        terminal.draw(|frame| ui(frame))?;

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

fn ui(frame: &mut Frame) {
    let main_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(frame.size());

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
    let slot_panel = Paragraph::new("Waiting for slot updates....")
        .block(Block::default().title("Latest Slot").borders(Borders::ALL));

    frame.render_widget(slot_panel, left_panels[0]);

    let tps_panel = Paragraph::new("TPS: ---").block(
        Block::default()
            .title("Network Throughput")
            .borders(Borders::ALL),
    );

    frame.render_widget(tps_panel, left_panels[1]);

    let activity_panel = Paragraph::new("No activity yet. \n\nSolana events will happen here.")
        .block(
            Block::default()
                .title("Recent Activity")
                .borders(Borders::ALL),
        );

    frame.render_widget(activity_panel, right_panels[0]);

    let connection_panel = Paragraph::new("Status: Not Connected\nRPC: Not configured")
        .block(Block::default().title(" Connection ").borders(Borders::ALL));

    frame.render_widget(connection_panel, right_panels[1]);

    let footer = Paragraph::new("Q: Quit").block(Block::default().borders(Borders::ALL));

    frame.render_widget(footer, main_layout[2]);
}
