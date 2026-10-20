use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};
use crossterm::event::{KeyCode, KeyEvent};
use tako::WorkerId;

use crate::common::format::human_size;
use crate::common::numa::{GetNumaInfo, GpuPciInfo, NumaNodeInfo, NumaTopology};
use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::styles::{
    self, style_footer, style_header_text, style_no_data, table_style_deselected,
    table_style_selected,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::dashboard::ui::widgets::text::draw_text;

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum FocusedWidget {
    #[default]
    Nodes,
    Distances,
    Gpus,
}

impl FocusedWidget {
    pub fn next(&self) -> Self {
        match self {
            FocusedWidget::Nodes => FocusedWidget::Distances,
            FocusedWidget::Distances => FocusedWidget::Gpus,
            FocusedWidget::Gpus => FocusedWidget::Nodes,
        }
    }
}

pub struct WorkerNumaScreen {
    worker_id: Option<WorkerId>,
    hostname: Option<String>,
    topology: Option<NumaTopology>,

    nodes_table: StatefulTable<NumaNodeInfo>,
    gpus_table: StatefulTable<GpuPciInfo>,
    distances_state: TableState,

    focused: FocusedWidget,
}

impl Default for WorkerNumaScreen {
    fn default() -> Self {
        let mut nodes_table = StatefulTable::default();
        nodes_table.set_missing_data_label("No NUMA nodes detected");

        let mut gpus_table = StatefulTable::default();
        gpus_table.set_missing_data_label("No GPUs or accelerators detected");

        Self {
            worker_id: None,
            hostname: None,
            topology: None,
            nodes_table,
            gpus_table,
            distances_state: TableState::default(),
            focused: FocusedWidget::default(),
        }
    }
}

impl WorkerNumaScreen {
    pub fn set_worker_id(&mut self, worker_id: WorkerId) {
        self.worker_id = Some(worker_id);
    }

    pub fn clear_worker_id(&mut self) {
        self.worker_id = None;
        self.hostname = None;
        self.topology = None;
        self.nodes_table.clear_selection();
        self.gpus_table.clear_selection();
        self.distances_state = TableState::default();
        self.focused = FocusedWidget::default();
    }

    pub fn update(&mut self, data: &DashboardData) {
        if let Some(worker_id) = self.worker_id
            && let Some(config) = data.workers().query_worker_config_for(worker_id)
        {
            self.hostname = Some(config.hostname.clone());
            let topo = config.get_numa_info();

            if let Some(ref topo) = topo {
                self.nodes_table.set_items(topo.nodes.clone());
                self.gpus_table.set_items(topo.gpus.clone());
            } else {
                self.nodes_table.set_items(Vec::new());
                self.gpus_table.set_items(Vec::new());
            }

            self.topology = topo;
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Tab => {
                self.focused = self.focused.next();
            }
            KeyCode::Down => match self.focused {
                FocusedWidget::Nodes => self.nodes_table.select_next_wrap(),
                FocusedWidget::Distances => {
                    let total_nodes = self
                        .topology
                        .as_ref()
                        .map(|t| t.nodes.len())
                        .unwrap_or(0);
                    if total_nodes > 0 {
                        let cur = self.distances_state.selected().unwrap_or(0);
                        self.distances_state.select(Some((cur + 1) % total_nodes));
                    }
                }
                FocusedWidget::Gpus => self.gpus_table.select_next_wrap(),
            },
            KeyCode::Up => match self.focused {
                FocusedWidget::Nodes => self.nodes_table.select_previous_wrap(),
                FocusedWidget::Distances => {
                    let total_nodes = self
                        .topology
                        .as_ref()
                        .map(|t| t.nodes.len())
                        .unwrap_or(0);
                    if total_nodes > 0 {
                        let cur = self.distances_state.selected().unwrap_or(0);
                        self.distances_state
                            .select(Some((cur + total_nodes - 1) % total_nodes));
                    }
                }
                FocusedWidget::Gpus => self.gpus_table.select_previous_wrap(),
            },
            _ => {}
        }
    }

    pub fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = NumaScreenLayout::new(&in_area);

        // 1. Header
        let title = if let Some(worker_id) = self.worker_id {
            let host = self.hostname.as_deref().unwrap_or("unknown");
            format!("Worker {} ({}) - NUMA & Device Topology", worker_id.as_num(), host)
        } else {
            "Worker NUMA & Device Topology".to_string()
        };

        let summary = if let Some(ref topo) = self.topology {
            let mem_str = topo
                .total_memory_bytes()
                .map(human_size)
                .unwrap_or_else(|| "N/A".to_string());
            format!(
                "{} NUMA node(s) | {} total core(s) | {} total memory | {} accelerator(s)",
                topo.nodes.len(),
                topo.total_cpus(),
                mem_str,
                topo.gpus.len()
            )
        } else {
            "No NUMA topology data available".to_string()
        };

        let header_content = format!("{}\n{}", title, summary);
        draw_text(
            &header_content,
            layout.header,
            frame,
            style_header_text(),
        );

        // 2. Footer
        let focus_hint = match self.focused {
            FocusedWidget::Nodes => "Focus: NUMA Nodes",
            FocusedWidget::Distances => "Focus: Distance Matrix",
            FocusedWidget::Gpus => "Focus: Accelerators",
        };
        draw_text(
            format!(
                "<\u{21F5}> scroll table, <tab> switch table ({}), <backspace> back",
                focus_hint
            )
            .as_str(),
            layout.footer,
            frame,
            style_footer(),
        );

        // 3. Body
        let Some(ref topo) = self.topology else {
            let msg = Paragraph::new(
                "No NUMA topology information available for this worker.\n\
                 (Worker may be running on a non-Linux platform, inside a container without sysfs, or with an older version.)",
            )
            .style(style_no_data())
            .alignment(Alignment::Center);
            frame.render_widget(msg, layout.main);
            return;
        };

        if topo.nodes.is_empty() {
            let msg = Paragraph::new("No NUMA nodes detected on this worker.")
                .style(style_no_data())
                .alignment(Alignment::Center);
            frame.render_widget(msg, layout.main);
            return;
        }

        let body_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Vertical)
            .split(layout.main);

        let bottom_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(40), Constraint::Percentage(60)])
            .direction(Direction::Horizontal)
            .split(body_chunks[1]);

        let nodes_rect = body_chunks[0];
        let distances_rect = bottom_chunks[0];
        let gpus_rect = bottom_chunks[1];

        // Draw Nodes Table
        self.draw_nodes_table(nodes_rect, frame);

        // Draw Distances Table
        self.draw_distances_table(distances_rect, frame);

        // Draw GPUs Table
        self.draw_gpus_table(gpus_rect, frame);
    }

    fn draw_nodes_table(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let style = if self.focused == FocusedWidget::Nodes {
            table_style_selected()
        } else {
            table_style_deselected()
        };

        let gpus_clone = self
            .topology
            .as_ref()
            .map(|t| t.gpus.clone())
            .unwrap_or_default();

        self.nodes_table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "NUMA Nodes & Memory",
                table_headers: Some(vec![
                    "Node",
                    "CPU Cores",
                    "Cores Count",
                    "Total Memory",
                    "Free Memory",
                    "Bound GPUs",
                ]),
                column_widths: vec![
                    Constraint::Length(8),
                    Constraint::Percentage(25),
                    Constraint::Length(12),
                    Constraint::Length(15),
                    Constraint::Length(15),
                    Constraint::Fill(1),
                ],
            },
            |node| {
                let mem_total_str = node
                    .mem_total_bytes
                    .map(human_size)
                    .unwrap_or_else(|| "N/A".to_string());
                let mem_free_str = node
                    .mem_free_bytes
                    .map(human_size)
                    .unwrap_or_else(|| "N/A".to_string());

                let bound_gpus: Vec<String> = gpus_clone
                    .iter()
                    .filter(|g| g.numa_node == Some(node.id as i32))
                    .map(|g| {
                        let name = g.device_name.as_deref().unwrap_or(g.vendor_name.as_str());
                        format!("{} ({})", name, g.pci_address)
                    })
                    .collect();

                let bound_gpus_str = if bound_gpus.is_empty() {
                    "—".to_string()
                } else {
                    bound_gpus.join(", ")
                };

                Row::new(vec![
                    Cell::from(format!("Node {}", node.id)),
                    Cell::from(node.cpulist.clone()),
                    Cell::from(format!("{} cores", node.cpu_count)),
                    Cell::from(mem_total_str),
                    Cell::from(mem_free_str),
                    Cell::from(bound_gpus_str),
                ])
            },
            style,
        );
    }

    fn draw_distances_table(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let style = if self.focused == FocusedWidget::Distances {
            table_style_selected()
        } else {
            table_style_deselected()
        };

        let title = styles::table_title("NUMA Distance Matrix (Latency)".to_string());
        let body_block = styles::table_block_with_title(title);

        let Some(ref topo) = self.topology else {
            return;
        };

        if topo.nodes.is_empty() {
            return;
        }

        // Header: ["Node", "0", "1", ...]
        let mut header_cells = vec!["Node".to_string()];
        for node in &topo.nodes {
            header_cells.push(node.id.to_string());
        }

        let header_row = Row::new(
            header_cells
                .iter()
                .map(|h| Cell::from(h.as_str()))
                .collect::<Vec<_>>(),
        )
        .style(Style::default().fg(ratatui::style::Color::Yellow));

        let rows: Vec<Row> = topo
            .nodes
            .iter()
            .map(|node| {
                let mut cells = vec![Cell::from(format!("Node {}", node.id))];
                if node.distances.is_empty() {
                    cells.push(Cell::from("10 (Local)"));
                } else {
                    for dist in &node.distances {
                        cells.push(Cell::from(dist.to_string()));
                    }
                }
                Row::new(cells)
            })
            .collect();

        let mut widths = vec![Constraint::Length(9)];
        for _ in &topo.nodes {
            widths.push(Constraint::Length(6));
        }

        let table = Table::new(rows, widths)
            .header(header_row)
            .block(body_block)
            .row_highlight_style(styles::style_table_highlight())
            .style(style);

        frame.render_stateful_widget(table, rect, &mut self.distances_state);
    }

    fn draw_gpus_table(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let style = if self.focused == FocusedWidget::Gpus {
            table_style_selected()
        } else {
            table_style_deselected()
        };

        self.gpus_table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Detected GPUs & Accelerators",
                table_headers: Some(vec![
                    "Model / Vendor",
                    "PCI Address",
                    "Driver",
                    "NUMA Affinity",
                    "Local CPUs",
                ]),
                column_widths: vec![
                    Constraint::Percentage(32),
                    Constraint::Length(16),
                    Constraint::Length(12),
                    Constraint::Length(15),
                    Constraint::Fill(1),
                ],
            },
            |gpu| {
                let model = gpu
                    .device_name
                    .clone()
                    .unwrap_or_else(|| gpu.vendor_name.clone());
                let driver = gpu.driver.as_deref().unwrap_or("none");
                let numa_affinity = match gpu.numa_node {
                    Some(n) if n >= 0 => format!("Node {n}"),
                    Some(-1) => "None (UMA)".to_string(),
                    Some(other) => format!("Node {other}"),
                    None => "N/A".to_string(),
                };
                let local_cpus = gpu
                    .local_cpulist
                    .clone()
                    .unwrap_or_else(|| "N/A".to_string());

                Row::new(vec![
                    Cell::from(model),
                    Cell::from(gpu.pci_address.clone()),
                    Cell::from(driver.to_string()),
                    Cell::from(numa_affinity),
                    Cell::from(local_cpus),
                ])
            },
            style,
        );
    }
}

struct NumaScreenLayout {
    header: Rect,
    main: Rect,
    footer: Rect,
}

impl NumaScreenLayout {
    fn new(rect: &Rect) -> Self {
        let chunks = Layout::default()
            .constraints(vec![
                Constraint::Length(3),
                Constraint::Fill(1),
                Constraint::Length(1),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        Self {
            header: chunks[0],
            main: chunks[1],
            footer: chunks[2],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn test_worker_numa_screen_navigation() {
        let mut screen = WorkerNumaScreen::default();
        assert_eq!(screen.focused, FocusedWidget::Nodes);

        let tab_key = KeyEvent {
            code: KeyCode::Tab,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        };
        screen.handle_key(tab_key);
        assert_eq!(screen.focused, FocusedWidget::Distances);

        screen.handle_key(tab_key);
        assert_eq!(screen.focused, FocusedWidget::Gpus);

        screen.handle_key(tab_key);
        assert_eq!(screen.focused, FocusedWidget::Nodes);
    }

    #[test]
    fn test_worker_numa_screen_draw() {
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).unwrap();

        let mut screen = WorkerNumaScreen::default();
        screen.set_worker_id(WorkerId::new(1));
        screen.hostname = Some("node-01".to_string());
        let topo = NumaTopology {
            nodes: vec![
                NumaNodeInfo {
                    id: 0,
                    cpulist: "0-3".to_string(),
                    cpu_count: 4,
                    mem_total_bytes: Some(16 * 1024 * 1024 * 1024),
                    mem_free_bytes: Some(8 * 1024 * 1024 * 1024),
                    distances: vec![10, 20],
                },
                NumaNodeInfo {
                    id: 1,
                    cpulist: "4-7".to_string(),
                    cpu_count: 4,
                    mem_total_bytes: Some(16 * 1024 * 1024 * 1024),
                    mem_free_bytes: Some(8 * 1024 * 1024 * 1024),
                    distances: vec![20, 10],
                },
            ],
            gpus: vec![GpuPciInfo {
                pci_address: "0000:01:00.0".to_string(),
                vendor_id: 0x10de,
                device_id: 0x1fdd,
                class_id: 0x030000,
                vendor_name: "NVIDIA Corporation".to_string(),
                device_name: Some("GeForce GTX 1650".to_string()),
                driver: Some("nvidia".to_string()),
                numa_node: Some(0),
                local_cpulist: Some("0-3".to_string()),
            }],
        };
        screen.nodes_table.set_items(topo.nodes.clone());
        screen.gpus_table.set_items(topo.gpus.clone());
        screen.topology = Some(topo);

        terminal
            .draw(|f| {
                screen.draw(f.area(), f);
            })
            .unwrap();
    }
}

