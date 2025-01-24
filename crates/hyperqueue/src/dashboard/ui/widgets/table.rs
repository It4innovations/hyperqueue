use ratatui::layout::{Alignment, Constraint, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Paragraph, Row, Table, TableState, Wrap};

use crate::dashboard::ui::styles;
use crate::dashboard::ui::terminal::DashboardFrame;
use ratatui::text::{Line, Span};

static HIGHLIGHT: &str = "=> ";

pub struct TableColumnHeaders {
    pub title: &'static str,
    pub table_headers: Option<Vec<&'static str>>,
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

    pub fn current_selection(&self) -> Option<&T> {
        if let Some(selection_index) = self.state.selected() {
            return self.items.get(selection_index);
        }
        None
    }

    pub fn clear_selection(&mut self) {
        self.state.select(None)
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
        &'a mut self,
        rect: Rect,
        frame: &mut DashboardFrame,
        columns: TableColumnHeaders,
        row_cell_mapper: F,
        table_style: Style,
    ) where
        T: 'a,
        F: Fn(&'a T) -> Row<'a>,
    {
        let title = styles::table_title(columns.title.to_string());
        let body_block = styles::table_block_with_title(title);

        if self.has_items() {
            let rows = self.items.iter().map(row_cell_mapper);
            let mut table = Table::new(rows, &columns.column_widths)
                .block(body_block)
                .row_highlight_style(styles::style_table_highlight())
                .style(table_style)
                .highlight_symbol(HIGHLIGHT);

            if let Some(column_headers) = columns.table_headers {
                table = table.header(styles::style_column_headers(column_headers));
            }

            frame.render_stateful_widget(table, rect, &mut self.state);
        } else {
            let header_text = Line::from(vec![Span::from("No data")]);
            let paragraph = Paragraph::new(header_text)
                .block(body_block)
                .style(styles::style_no_data())
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
