use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::autoalloc_screen::AutoAllocScreen;
use crate::dashboard::ui::screens::job_screen::JobScreen;
use crate::dashboard::ui::screens::overview_screen::WorkerOverviewScreen;
use crate::dashboard::ui::terminal::{DashboardFrame, DashboardTerminal};
use crate::dashboard::{DEFAULT_LIVE_DURATION, TIMELINE_MOVE_OFFSET};
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Alignment, Constraint, Direction, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Paragraph, Tabs, Wrap};
use ratatui::Frame;
use std::fmt::Write;
use std::ops::ControlFlow;

const KEY_TIMELINE_SOONER: char = 'o';
const KEY_TIMELINE_LATER: char = 'p';
const KEY_TIMELINE_LIVE: char = 'r';

#[derive(Default)]
pub struct RootScreen {
    job_overview_screen: JobScreen,
    worker_overview_screen: WorkerOverviewScreen,
    auto_allocator_screen: AutoAllocScreen,

    current_screen: SelectedScreen,
}

struct RootChunks {
    // Tab selection, in the top-left corner
    screen_tabs: Rect,
    // Timeline information and shortcuts, in the top-right corner
    timeline: Rect,
    screen: Rect,
}

#[derive(Clone, Copy, Default)]
enum SelectedScreen {
    JobOverview,
    #[default]
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

    pub fn handle_key(
        &mut self,
        input: KeyEvent,
        data: &mut DashboardData,
    ) -> ControlFlow<anyhow::Result<()>> {
        if input.code == KeyCode::Char('q') {
            return ControlFlow::Break(Ok(()));
        }
        match input.code {
            KeyCode::Char('j') => {
                self.current_screen = SelectedScreen::JobOverview;
            }
            KeyCode::Char('a') => {
                self.current_screen = SelectedScreen::AutoAllocator;
            }
            KeyCode::Char('w') => {
                self.current_screen = SelectedScreen::WorkerOverview;
            }
            KeyCode::Char(c) if c == KEY_TIMELINE_SOONER => {
                data.set_time_range(data.current_time_range().sooner(TIMELINE_MOVE_OFFSET))
            }
            KeyCode::Char(c) if c == KEY_TIMELINE_LATER => {
                data.set_time_range(data.current_time_range().later(TIMELINE_MOVE_OFFSET))
            }
            KeyCode::Char(c) if c == KEY_TIMELINE_LIVE => {
                data.set_live_time_mode(DEFAULT_LIVE_DURATION)
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

fn get_root_screen_chunks(frame: &DashboardFrame) -> RootChunks {
    let root_screen_chunks = Layout::default()
        .constraints(vec![Constraint::Length(4), Constraint::Fill(1)])
        .direction(Direction::Vertical)
        .split(frame.area());

    let top = root_screen_chunks[0];
    let screen = root_screen_chunks[1];

    let top_chunks = Layout::default()
        .constraints(vec![Constraint::Min(28), Constraint::Min(20)])
        .flex(Flex::SpaceBetween)
        .direction(Direction::Horizontal)
        .split(top);

    RootChunks {
        screen_tabs: top_chunks[0],
        timeline: top_chunks[1],
        screen,
    }
}

/// Renders the top `tab bar` of the dashboard.
fn render_screen_tabs(current_screen: SelectedScreen, rect: Rect, frame: &mut DashboardFrame) {
    let screen_names = ["Jobs", "AutoAllocator", "Workers"];
    let screen_titles: Vec<Line> = screen_names
        .iter()
        .map(|t| {
            let (first, rest) = t.split_at(1);
            Line::from(vec![
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

fn render_timeline(data: &DashboardData, rect: Rect, frame: &mut Frame) {
    let chunks = Layout::default()
        .constraints(vec![Constraint::Length(1), Constraint::Fill(1)])
        .direction(Direction::Vertical)
        .split(rect);

    let range = data.current_time_range();
    let start: chrono::DateTime<Local> = range.start.into();
    let end: chrono::DateTime<Local> = range.end.into();

    let skip_day = chunks[0].width < 35
        || (start.date_naive() == end.date_naive()
            && start.date_naive() == Local::now().date_naive());
    let date_format: &str = if skip_day {
        "%H:%M:%S"
    } else {
        "%d.%m. %H:%M:%S"
    };

    let range = format!(
        "{} - {}",
        start.format(date_format),
        end.format(date_format)
    );
    let range_paragraph = Paragraph::new(Text::from(range))
        .alignment(Alignment::Right)
        .wrap(Wrap { trim: true });

    frame.render_widget(range_paragraph, chunks[0]);

    let mut shortcuts = format!(
        r#"<{KEY_TIMELINE_SOONER}> -5 minutes
<{KEY_TIMELINE_LATER}> +5 minutes"#
    );
    if !data.is_live_time_mode() {
        write!(shortcuts, "\n<{KEY_TIMELINE_LIVE}> live view").unwrap();
    }

    let shortcuts_paragraph = Paragraph::new(Text::from(shortcuts))
        .alignment(Alignment::Right)
        .wrap(Wrap { trim: true });
    frame.render_widget(shortcuts_paragraph, chunks[1]);
}
