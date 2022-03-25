use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::controller::{ChangeScreenCommand, ScreenController};
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::autoalloc_screen::AutoAllocScreen;
use crate::dashboard::ui::screens::job_screen::JobScreen;
use crate::dashboard::ui::screens::overview_screen::OverviewScreen;
use crate::dashboard::ui::terminal::{DashboardFrame, DashboardTerminal};
use std::ops::ControlFlow;
use tako::common::WrappedRcRefCell;
use termion::event::Key;
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Block, Borders, Tabs};

pub struct RootScreen {
    cluster_overview_screen: OverviewScreen,
    auto_allocator_screen: AutoAllocScreen,
    job_overview_screen: JobScreen,

    data_source: WrappedRcRefCell<DashboardData>,

    current_screen: DashboardScreenState,
    controller: ScreenController,
}

pub struct RootChunks {
    pub screen_tabs: Rect,
    pub screen: Rect,
}

#[derive(Clone, Copy)]
pub enum DashboardScreenState {
    ClusterOverview,
    AutoAllocator,
    JobOverview,
}

impl RootScreen {
    pub fn new(data_source: WrappedRcRefCell<DashboardData>, controller: ScreenController) -> Self {
        Self {
            data_source,
            cluster_overview_screen: Default::default(),
            auto_allocator_screen: Default::default(),
            job_overview_screen: Default::default(),
            current_screen: DashboardScreenState::ClusterOverview,
            controller,
        }
    }

    pub fn draw(&mut self, terminal: &mut DashboardTerminal) {
        terminal
            .draw(|frame| {
                let screen_state = self.current_screen;
                let data = self.data_source.clone();
                let (screen, controller) = self.get_current_screen_and_controller();
                screen.update(&data.get(), controller);

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
            // Quits the dashboard
            ControlFlow::Break(Ok(()))
        } else {
            let (screen, controller) = self.get_current_screen_and_controller();
            screen.handle_key(input, controller);
            ControlFlow::Continue(())
        }
    }

    /// Updates data for the next screen if required and changes the dashboard state to show it.
    pub fn change_current_screen(&mut self, next_screen: ChangeScreenCommand) {
        match next_screen {
            ChangeScreenCommand::ClusterOverviewScreen => {
                self.current_screen = DashboardScreenState::ClusterOverview;
            }
            ChangeScreenCommand::AutoAllocatorScreen => {
                self.current_screen = DashboardScreenState::AutoAllocator;
            }
            ChangeScreenCommand::JobOverviewScreen => {
                self.current_screen = DashboardScreenState::JobOverview;
            }
        }
    }

    fn get_current_screen_and_controller(&mut self) -> (&mut dyn Screen, &mut ScreenController) {
        match self.current_screen {
            DashboardScreenState::ClusterOverview => {
                (&mut self.cluster_overview_screen, &mut self.controller)
            }
            DashboardScreenState::AutoAllocator => {
                (&mut self.auto_allocator_screen, &mut self.controller)
            }
            DashboardScreenState::JobOverview => {
                (&mut self.job_overview_screen, &mut self.controller)
            }
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
    let screen_names = vec!["Jobs", "AutoAllocator", "ClusterOverview"];

    let tab_titles = screen_names
        .iter()
        .map(|t| {
            let (first, rest) = t.split_at(1);
            Spans::from(vec![
                Span::styled(first, Style::default().fg(Color::Yellow)),
                Span::styled(rest, Style::default().fg(Color::Green)),
            ])
        })
        .collect();

    let selected_index = match current_screen {
        DashboardScreenState::JobOverview => 0,
        DashboardScreenState::AutoAllocator => 1,
        DashboardScreenState::ClusterOverview => 2,
    };

    let tabs = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::ALL).title("Screens"))
        .select(selected_index)
        .style(Style::default().fg(Color::Cyan))
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(Color::White)
                .bg(Color::Black),
        );
    frame.render_widget(tabs, rect);
}
