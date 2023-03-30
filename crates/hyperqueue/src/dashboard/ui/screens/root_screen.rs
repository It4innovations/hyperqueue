use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::autoalloc_screen::AutoAllocScreen;
use crate::dashboard::ui::screens::job_screen::JobScreen;
use crate::dashboard::ui::screens::overview_screen::WorkerOverviewScreen;
use crate::dashboard::ui::terminal::{Backend, DashboardFrame, DashboardTerminal};
use crate::dashboard::ui::widgets::text::draw_text;
use chrono::{Local, SecondsFormat};
use std::ops::ControlFlow;
use termion::event::Key;
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Block, Borders, Paragraph, Tabs, Wrap};
use tui::Frame;

#[derive(Default)]
pub struct RootScreen {
    job_overview_screen: JobScreen,
    worker_overview_screen: WorkerOverviewScreen,
    auto_allocator_screen: AutoAllocScreen,

    current_screen: SelectedScreen,
}

pub struct RootChunks {
    pub screen_tabs: Rect,
    pub timeline: Rect,
    pub screen: Rect,
}

#[derive(Clone, Copy, Default)]
pub enum SelectedScreen {
    #[default]
    JobOverview,
    WorkerOverview,
    AutoAllocator,
}

impl RootScreen {
    pub fn draw(&mut self, terminal: &mut DashboardTerminal, data: &DashboardData) {
        terminal
            .draw(|frame| {
                let screen_state = self.current_screen;
                let screen = self.get_current_screen_mut();
                screen.update(data);

                let chunks = get_root_screen_chunks(frame);
                render_screen_tabs(screen_state, chunks.screen_tabs, frame);
                render_timeline(data, chunks.timeline, frame);
                screen.draw(chunks.screen, frame);
            })
            .expect("An error occurred while drawing the dashboard");
    }

    pub fn handle_key(&mut self, input: Key) -> ControlFlow<anyhow::Result<()>> {
        if input == Key::Char('q') {
            return ControlFlow::Break(Ok(()));
        }
        match input {
            Key::Char('j') => {
                self.current_screen = SelectedScreen::JobOverview;
            }
            Key::Char('a') => {
                self.current_screen = SelectedScreen::AutoAllocator;
            }
            Key::Char('w') => {
                self.current_screen = SelectedScreen::WorkerOverview;
            }

            _ => {
                self.get_current_screen_mut().handle_key(input);
            }
        }
        ControlFlow::Continue(())
    }

    fn get_current_screen_mut(&mut self) -> &mut dyn Screen {
        match self.current_screen {
            SelectedScreen::WorkerOverview => &mut self.worker_overview_screen,
            SelectedScreen::AutoAllocator => &mut self.auto_allocator_screen,
            SelectedScreen::JobOverview => &mut self.job_overview_screen,
        }
    }
}

pub fn get_root_screen_chunks(frame: &DashboardFrame) -> RootChunks {
    let root_screen_chunks = Layout::default()
        .constraints(vec![Constraint::Length(3), Constraint::Percentage(100)])
        .direction(Direction::Vertical)
        .split(frame.size());

    let top = root_screen_chunks[0];
    let screen = root_screen_chunks[1];

    let top_chunks = Layout::default()
        .constraints(vec![Constraint::Percentage(80), Constraint::Percentage(20)])
        .direction(Direction::Horizontal)
        .split(top);

    RootChunks {
        screen_tabs: top_chunks[0],
        timeline: top_chunks[1],
        screen,
    }
}

/// Renders the top `tab bar` of the dashboard.
pub fn render_screen_tabs(current_screen: SelectedScreen, rect: Rect, frame: &mut DashboardFrame) {
    let screen_names = vec!["Jobs", "AutoAllocator", "Workers"];
    let screen_titles = screen_names
        .iter()
        .map(|t| {
            let (first, rest) = t.split_at(1);
            Spans::from(vec![
                Span::styled(first, Style::default().fg(Color::Yellow)),
                Span::styled(rest, Style::default().fg(Color::Green)),
            ])
        })
        .collect();

    let selected_screen = match current_screen {
        SelectedScreen::JobOverview => 0,
        SelectedScreen::AutoAllocator => 1,
        SelectedScreen::WorkerOverview => 2,
    };

    let screen_tabs = Tabs::new(screen_titles)
        .block(Block::default().borders(Borders::ALL).title("Screens"))
        .select(selected_screen)
        .style(Style::default().fg(Color::Cyan))
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(Color::White)
                .bg(Color::Black),
        );

    frame.render_widget(screen_tabs, rect);
}

fn render_timeline(data: &DashboardData, rect: Rect, frame: &mut Frame<Backend>) {
    let range = data.current_time_range();
    let start: chrono::DateTime<Local> = range.start.into();
    let end: chrono::DateTime<Local> = range.end.into();

    const FORMAT: &str = "%d.%m. %H:%M:%S";

    let formatted = format!("{} - {}", start.format(FORMAT), end.format(FORMAT));
    let paragraph = Paragraph::new(vec![Spans::from(formatted)])
        .style(Style::default())
        .block(Block::default())
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, rect);
}
