use crate::dashboard::data::{DashboardData, FetchedTimeRange};
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::autoalloc_screen::AutoAllocScreen;
use crate::dashboard::ui::screens::job_screen::JobScreen;
use crate::dashboard::ui::screens::overview_screen::OverviewScreen;
use crate::dashboard::ui::terminal::{DashboardFrame, DashboardTerminal};
use chrono::{DateTime, Local};
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};
use termion::event::Key;
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Block, Borders, Paragraph, Tabs, Wrap};

pub struct RootScreen {
    cluster_overview_screen: OverviewScreen,
    auto_allocator_screen: AutoAllocScreen,
    job_overview_screen: JobScreen,

    current_screen: DashboardScreenState,
    timeline_scrubber: TimelineScrubber,
}

pub struct RootChunks {
    pub screen_tabs: Rect,
    pub screen: Rect,
    pub timeline_scrubber: Rect,
}

#[derive(Clone, Copy)]
pub struct TimelineScrubber {
    pub is_live: bool,
    pub display_time: SystemTime,
    pub time_range: FetchedTimeRange,

    pub scrub_duration: Duration,
}

#[derive(Clone, Copy)]
pub enum DashboardScreenState {
    WorkerOverview,
    AutoAllocator,
    JobOverview,
}

impl TimelineScrubber {
    pub fn update(&mut self, time_range: FetchedTimeRange) {
        self.time_range = time_range;
        if self.is_live {
            self.display_time = time_range.fetched_until;
        }
    }
}

impl RootScreen {
    pub fn draw(&mut self, terminal: &mut DashboardTerminal, data: &DashboardData) {
        self.timeline_scrubber.update(*data.query_time_range());
        let timeline_scrubber = self.timeline_scrubber;
        terminal
            .draw(|frame| {
                let screen_state = self.current_screen;
                let screen = self.get_current_screen_mut();
                screen.update(data);

                render_screen_tabs(
                    screen_state,
                    get_root_screen_chunks(frame).screen_tabs,
                    frame,
                );

                render_timeline_scrubber(
                    &timeline_scrubber,
                    get_root_screen_chunks(frame).timeline_scrubber,
                    frame,
                );
                screen.draw(get_root_screen_chunks(frame).screen, frame);
            })
            .expect("An error occurred while drawing the dashboard");
    }

    pub fn handle_key(&mut self, input: Key) -> ControlFlow<anyhow::Result<()>> {
        if input == Key::Char('q') {
            return ControlFlow::Break(Ok(()));
        }
        match input {
            Key::Char('j') => {
                self.current_screen = DashboardScreenState::JobOverview;
            }
            Key::Char('a') => {
                self.current_screen = DashboardScreenState::AutoAllocator;
            }
            Key::Char('w') => {
                self.current_screen = DashboardScreenState::WorkerOverview;
            }

            _ => {
                self.get_current_screen_mut().handle_key(input);
            }
        }
        ControlFlow::Continue(())
    }

    fn get_current_screen_mut(&mut self) -> &mut dyn Screen {
        match self.current_screen {
            DashboardScreenState::WorkerOverview => &mut self.cluster_overview_screen,
            DashboardScreenState::AutoAllocator => &mut self.auto_allocator_screen,
            DashboardScreenState::JobOverview => &mut self.job_overview_screen,
        }
    }
}

pub fn get_root_screen_chunks(frame: &DashboardFrame) -> RootChunks {
    let root_screen_chunks = Layout::default()
        .constraints(vec![
            Constraint::Min(3),
            Constraint::Percentage(75),
            Constraint::Min(3),
        ])
        .direction(Direction::Vertical)
        .split(frame.size());

    RootChunks {
        screen_tabs: root_screen_chunks[0],
        screen: root_screen_chunks[1],
        timeline_scrubber: root_screen_chunks[2],
    }
}

/// Renders the top `tab bar` of the dashboard.
fn render_screen_tabs(
    current_screen: DashboardScreenState,
    rect: Rect,
    frame: &mut DashboardFrame,
) {
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
        DashboardScreenState::JobOverview => 0,
        DashboardScreenState::AutoAllocator => 1,
        DashboardScreenState::WorkerOverview => 2,
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

fn render_timeline_scrubber(scrubber: &TimelineScrubber, rect: Rect, frame: &mut DashboardFrame) {
    let current_time: DateTime<Local> = scrubber.display_time.into();
    let begin_time: DateTime<Local> = scrubber.time_range.fetched_from.into();
    let end_time: DateTime<Local> = scrubber.time_range.fetched_until.into();

    let scrubber_body = vec![Spans::from(vec![
        Span::raw("Current Time: "),
        Span::styled(
            current_time.format("%b %e, %T").to_string(),
            Style::default()
                .add_modifier(Modifier::ITALIC)
                .fg(Color::Green),
        ),
        Span::raw(", \t"),
        Span::raw("Range Fetched: ("),
        Span::styled(
            begin_time.format("%b %e, %T").to_string(),
            Style::default()
                .add_modifier(Modifier::ITALIC)
                .fg(Color::Green),
        ),
        Span::raw("->"),
        Span::styled(
            end_time.format("%b %e, %T").to_string(),
            Style::default()
                .add_modifier(Modifier::ITALIC)
                .fg(Color::Green),
        ),
        Span::raw(")"),
    ])];
    let scrubber = Paragraph::new(scrubber_body)
        .block(
            Block::default()
                .title("Timeline scrubber")
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::White).bg(Color::Black))
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true });

    frame.render_widget(scrubber, rect);
}

impl Default for RootScreen {
    fn default() -> Self {
        RootScreen {
            cluster_overview_screen: Default::default(),
            auto_allocator_screen: Default::default(),
            job_overview_screen: Default::default(),
            current_screen: DashboardScreenState::JobOverview,
            timeline_scrubber: Default::default(),
        }
    }
}

impl Default for TimelineScrubber {
    fn default() -> Self {
        TimelineScrubber {
            is_live: true,
            display_time: SystemTime::now(),
            time_range: Default::default(),
            scrub_duration: Duration::from_secs(60),
        }
    }
}
