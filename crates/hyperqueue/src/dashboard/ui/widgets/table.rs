use std::marker::PhantomData;
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
    marker: PhantomData<T>,
}

impl<T> Default for StatefulTable<T> {
    fn default() -> Self {
        Self {
            state: Default::default(),
            marker: Default::default(),
        }
    }
}

impl<T> StatefulTable<T> {
    pub fn draw<'a, F>(
        &mut self,
        rect: Rect,
        frame: &mut DashboardFrame,
        props: ResourceTableProps,
        items: &[T],
        row_cell_mapper: F,
    ) where
        F: Fn(&T) -> Row<'a>,
    {
        let title = draw_utils::title_with_dual_style(props.title, props.inline_help);
        let body_block = draw_utils::draw_body_with_title(title);

        if !items.is_empty() {
            let rows = items.iter().map(row_cell_mapper);
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
