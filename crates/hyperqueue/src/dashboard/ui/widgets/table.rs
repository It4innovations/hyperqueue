use tui::layout::{Alignment, Constraint, Rect};
use tui::widgets::{Paragraph, Row, Table, TableState, Wrap};

use crate::dashboard::ui::screens::draw_utils;
use crate::dashboard::ui::terminal::DashboardFrame;
use tui::text::Spans;

static HIGHLIGHT: &str = "=> ";

pub struct ResourceTableProps {
    pub title: String,
    pub inline_help: String,
    pub table_headers: Vec<&'static str>,
    pub column_widths: Vec<Constraint>,
}

pub struct StatefulTable<T> {
    state: TableState,
    items: Vec<T>,
}

impl<T> Default for StatefulTable<T> {
    fn default() -> Self {
        Self {
            state: Default::default(),
            items: Default::default(),
        }
    }
}

impl<T> StatefulTable<T> {
    /// Sets new items for the table.
    ///
    /// Invariant: if there are no items in the table, no item is selected.
    pub fn set_items(&mut self, items: Vec<T>) {
        self.items = items;

        // Make sure that our selection does not dangle
        if !self.has_items() {
            self.state.select(None);
        } else if let Some(index) = self.state.selected() {
            if index >= self.items.len() {
                self.select_last();
            }
        }
    }

    /// Select next item in the table, wrapping to the beginning if the selection was at the last
    /// item.
    pub fn select_next_wrap(&mut self) {
        match self.state.selected() {
            Some(index) => {
                let index = (index + 1) % self.items.len();
                self.state.select(Some(index));
            }
            None => {
                if self.has_items() {
                    self.state.select(Some(0));
                }
            }
        }
    }

    /// Select previous item in the table, wrapping to the end if the selection was at the first
    /// item.
    pub fn select_previous_wrap(&mut self) {
        match self.state.selected() {
            Some(index) => {
                let index = (index + self.items.len() - 1) % self.items.len();
                self.state.select(Some(index));
            }
            None => {
                if self.has_items() {
                    self.select_last();
                }
            }
        }
    }

    pub fn draw<'a, F>(
        &mut self,
        rect: Rect,
        frame: &mut DashboardFrame,
        props: ResourceTableProps,
        row_cell_mapper: F,
    ) where
        F: Fn(&T) -> Row<'a>,
    {
        let title = draw_utils::title_with_dual_style(props.title, props.inline_help);
        let body_block = draw_utils::draw_body_with_title(title);

        if self.has_items() {
            let rows = self.items.iter().map(row_cell_mapper);
            let table = Table::new(rows)
                .header(draw_utils::table_header_style(props.table_headers))
                .block(body_block)
                .highlight_style(draw_utils::style_highlight())
                .highlight_symbol(HIGHLIGHT)
                .widths(&props.column_widths);
            frame.render_stateful_widget(table, rect, &mut self.state);
        } else {
            let header_text = vec![Spans::from("No data")];
            let paragraph = Paragraph::new(header_text)
                .block(body_block)
                .alignment(Alignment::Left)
                .wrap(Wrap { trim: true });
            frame.render_widget(paragraph, rect);
        }
    }
}

impl<T> StatefulTable<T> {
    fn has_items(&self) -> bool {
        !self.items.is_empty()
    }

    fn select_last(&mut self) {
        if self.has_items() {
            self.state.select(Some(self.items.len() - 1));
        } else {
            self.state.select(None);
        }
    }
}
