use crate::dashboard::DEFAULT_LIVE_DURATION;
use crate::dashboard::data::{DashboardData, TimeRange};
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::autoalloc::AutoAllocScreen;
use crate::dashboard::ui::screens::cluster::WorkerOverviewScreen;
use crate::dashboard::ui::screens::jobs::JobScreen;
use crate::dashboard::ui::terminal::{DashboardFrame, DashboardTerminal};
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Tabs, Wrap};
use std::fmt::Write;
use std::ops::ControlFlow;
use std::time::Duration;

const KEY_TIMELINE_SOONER: char = 'o';
const KEY_TIMELINE_LATER: char = 'p';
const KEY_TIMELINE_ZOOM_IN: char = 'k';
const KEY_TIMELINE_ZOOM_OUT: char = 'l';
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

                // Draw warning popup
                if let Some(warning) = data.get_warning() {
                    let block = Block::bordered()
                        .title("Warning [press any key to hide]")
                        .bold()
                        .on_blue();
                    let paragraph = Paragraph::new(warning)
                        .block(block)
                        .not_bold()
                        .wrap(Wrap { trim: true });

                    let area = popup_area(frame.area(), 50, 50);
                    frame.render_widget(Clear, area);
                    frame.render_widget(paragraph, area);
                }
            })
            .expect("An error occurred while drawing the dashboard");
    }

    pub fn handle_key(
        &mut self,
        input: KeyEvent,
        data: &mut DashboardData,
    ) -> ControlFlow<anyhow::Result<()>> {
        if data.get_warning().is_some() {
            data.set_warning(None);
            return ControlFlow::Continue(());
        }

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
            KeyCode::Char(KEY_TIMELINE_SOONER) => {
                data.set_time_range(data.current_time_range().sooner(offset_duration(data)))
            }
            KeyCode::Char(KEY_TIMELINE_LATER) => {
                data.set_time_range(data.current_time_range().later(offset_duration(data)))
            }
            KeyCode::Char(KEY_TIMELINE_ZOOM_IN) => {
                data.set_time_range(zoom_in(data.current_time_range()))
            }
            KeyCode::Char(KEY_TIMELINE_ZOOM_OUT) => {
                data.set_time_range(zoom_out(data.current_time_range()))
            }
            KeyCode::Char(KEY_TIMELINE_LIVE) if data.stream_enabled() => {
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

fn zoom_in(range: TimeRange) -> TimeRange {
    let duration = range.duration();
    if duration.as_secs() <= 10 {
        return range;
    }
    let start = range.start() + duration / 4;
    let end = range.end() - duration / 4;
    TimeRange::new(start, end)
}

fn zoom_out(range: TimeRange) -> TimeRange {
    let duration = range.duration();
    if duration.as_secs() >= 3600 * 24 * 7 {
        return range;
    }
    let start = range.start() - duration / 2;
    let end = range.end() + duration / 2;
    TimeRange::new(start, end)
}

fn get_root_screen_chunks(frame: &DashboardFrame) -> RootChunks {
    let root_screen_chunks = Layout::default()
        .constraints(vec![Constraint::Length(4), Constraint::Fill(1)])
        .direction(Direction::Vertical)
        .split(frame.area());

    let top = root_screen_chunks[0];
    let screen = root_screen_chunks[1];

    let top_chunks = Layout::default()
        .constraints(vec![Constraint::Max(40), Constraint::Min(20)])
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
    let start: chrono::DateTime<Local> = range.start().into();
    let end: chrono::DateTime<Local> = range.end().into();

    let skip_day = chunks[0].width < 35
        || (start.date_naive() == end.date_naive()
            && start.date_naive() == Local::now().date_naive());
    let date_format: &str = if skip_day {
        "%H:%M:%S"
    } else {
        "%d.%m. %H:%M:%S"
    };

    let range = format!(
        "{} - {} ({})",
        start.format(date_format),
        end.format(date_format),
        humantime::format_duration(range.duration())
    );
    let range_paragraph = Paragraph::new(Text::from(range))
        .alignment(Alignment::Right)
        .wrap(Wrap { trim: true });

    frame.render_widget(range_paragraph, chunks[0]);

    let mode = match data.stream_enabled() {
        true => "[stream]",
        false => "[replay]",
    };
    let mut text = format!(
        "{mode}\n<{KEY_TIMELINE_SOONER}> -{offset}m, <{KEY_TIMELINE_LATER}> +{offset}m, <{KEY_TIMELINE_ZOOM_IN}> zoom in, <{KEY_TIMELINE_ZOOM_OUT}> zoom out",
        offset = offset_duration(data).as_secs() / 60
    );
    if !data.is_live_time_mode() && data.stream_enabled() {
        write!(text, "\n<{KEY_TIMELINE_LIVE}> live view").unwrap();
    }

    let shortcuts_paragraph = Paragraph::new(Text::from(text))
        .alignment(Alignment::Right)
        .wrap(Wrap { trim: true });
    frame.render_widget(shortcuts_paragraph, chunks[1]);
}

fn offset_duration(data: &DashboardData) -> Duration {
    data.current_time_range().duration() / 4
}

/// Helper function to create a centered rect using up certain percentage of the available rect
/// `area`
/// Taken from https://ratatui.rs/examples/apps/popup/.
fn popup_area(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let vertical = Layout::vertical([Constraint::Percentage(percent_y)]).flex(Flex::Center);
    let horizontal = Layout::horizontal([Constraint::Percentage(percent_x)]).flex(Flex::Center);
    let [area] = vertical.areas(area);
    let [area] = horizontal.areas(area);
    area
}
