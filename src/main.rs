use anyhow::{Ok, Result};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, Paragraph},
    Frame, Terminal,
};
use std::io::{self, stdout};

fn main() -> Result<()> {
    enable_raw_mode()?;

    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let res = run_app(&mut terminal);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("Application error: {:?}", err);
    }

    Ok(());
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f))?;

        event::read()?;

        if(Event::Key(key) == KeyCode::Char('q')) {
            break;
        } 
    }
}


fn ui(frame: &mut Frame) {
    Paragraph::new("Waiting for Solana data...").block(Block::default().title("Solana Block Monitor").borders(Borders::ALL));

    frame.render_widget(my_paragraph, frame.size());

}