use tui::widgets::Table;
use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Span, Spans, Text},
    widgets::{Block, Borders, Paragraph, Row},
    Frame,
};

use crate::dashboard::models::StatefulTable;

static HIGHLIGHT: &str = "=> ";

pub struct ResourceTableProps<'a, T> {
    pub title: String,
    pub inline_help: String,
    pub resource: &'a mut StatefulTable<T>,
    pub table_headers: Vec<&'a str>,
    pub column_widths: Vec<Constraint>,
}

pub fn draw_loading_screen<B: Backend>(
    f: &mut Frame<B>,
    block: Block,
    area: Rect,
    is_loading: bool,
) {
    if is_loading {
        let text = "\n\n Loading ...\n\n".to_owned();
        let mut text = Text::from(text);
        text.patch_style(style_secondary());

        // Contains the text
        let paragraph = Paragraph::new(text).style(style_secondary()).block(block);
        f.render_widget(paragraph, area);
    } else {
        f.render_widget(block, area)
    }
}

pub(crate) fn draw_table<'a, B, T, F>(
    f: &mut Frame<B>,
    area: Rect,
    table_props: ResourceTableProps<'a, T>,
    row_cell_mapper: F,
    is_loading: bool,
) where
    B: Backend,
    F: Fn(&T) -> Row<'a>,
{
    let title = title_with_dual_style(table_props.title, table_props.inline_help);
    let body_block = draw_body_with_title(title);

    if !table_props.resource.items.is_empty() {
        let rows = table_props.resource.items.iter().map(row_cell_mapper);

        let table = Table::new(rows)
            .header(table_header_style(table_props.table_headers))
            .block(body_block)
            .highlight_style(style_highlight())
            .highlight_symbol(HIGHLIGHT)
            .widths(&table_props.column_widths);
        f.render_stateful_widget(table, area, &mut table_props.resource.state);
    } else {
        draw_loading_screen(f, body_block, area, is_loading);
    }
}

pub fn style_highlight() -> Style {
    Style::default().add_modifier(Modifier::REVERSED)
}

pub fn style_default() -> Style {
    Style::default().fg(Color::Magenta)
}

pub fn style_secondary() -> Style {
    Style::default().fg(Color::Yellow)
}

pub fn table_header_style(cells: Vec<&str>) -> Row {
    Row::new(cells).style(style_default()).bottom_margin(0)
}

pub fn vertical_chunks(constraints: Vec<Constraint>, size: Rect) -> Vec<Rect> {
    Layout::default()
        .constraints(constraints.as_ref())
        .direction(Direction::Vertical)
        .split(size)
}

pub fn draw_body_with_title(title: Spans) -> Block {
    Block::default().borders(Borders::TOP).title(title)
}

pub fn title_with_dual_style<'a>(part_1: String, part_2: String) -> Spans<'a> {
    Spans::from(vec![
        Span::styled(part_1, style_secondary().add_modifier(Modifier::BOLD)),
        Span::styled(part_2, style_default().add_modifier(Modifier::BOLD)),
    ])
}
