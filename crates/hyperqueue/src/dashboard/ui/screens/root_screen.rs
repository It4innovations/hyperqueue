use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::autoalloc_screen::AutoAllocScreen;
use crate::dashboard::ui::screens::job_screen::JobScreen;
use crate::dashboard::ui::screens::overview_screen::OverviewScreen;
use crate::dashboard::ui::terminal::{DashboardFrame, DashboardTerminal};
use std::ops::ControlFlow;
use termion::event::Key;
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Block, Borders, Tabs};

pub struct RootScreen {
    cluster_overview_screen: OverviewScreen,
    auto_allocator_screen: AutoAllocScreen,
    job_overview_screen: JobScreen,

    current_screen: DashboardScreenState,
}

pub struct RootChunks {
    pub screen_tabs: Rect,
    pub screen: Rect,
}

#[derive(Clone, Copy)]
pub enum DashboardScreenState {
    WorkerOverview,
    AutoAllocator,
    JobOverview,
}

impl RootScreen {
    pub fn draw(&mut self, terminal: &mut DashboardTerminal, data: &DashboardData) {
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
        .constraints(vec![Constraint::Min(3), Constraint::Min(40)])
        .direction(Direction::Vertical)
        .split(frame.size());

    RootChunks {
        screen_tabs: root_screen_chunks[0],
        screen: root_screen_chunks[1],
    }
}

/// Renders the top `tab bar` of the dashboard.
pub fn render_screen_tabs(
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

impl Default for RootScreen {
    fn default() -> Self {
        RootScreen {
            cluster_overview_screen: Default::default(),
            auto_allocator_screen: Default::default(),
            job_overview_screen: Default::default(),
            current_screen: DashboardScreenState::JobOverview,
        }
    }
}
