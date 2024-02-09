use ratatui::{
    style::{Color, Modifier, Style},
    text::{Span, Spans},
    widgets::{Block, Borders, Row},
};

/// The Style associated with the hyperqueue logo text
pub fn style_header_text() -> Style {
    Style::default()
        .fg(Color::White)
        .bg(Color::Black)
        .add_modifier(Modifier::BOLD)
}

/// The Style for the footer text on fragments
pub fn style_footer() -> Style {
    Style::default().fg(Color::White).bg(Color::Black)
}

pub fn style_table_title() -> Style {
    Style::default()
        .fg(Color::White)
        .bg(Color::Black)
        .add_modifier(Modifier::BOLD)
}

pub fn table_title<'a>(part_1: String) -> Spans<'a> {
    Spans::from(vec![Span::styled(part_1, style_table_title())])
}

pub fn table_block_with_title(title: Spans) -> Block {
    Block::default()
        .borders(Borders::ALL)
        .style(Style::default().bg(Color::Black))
        .title(title)
}

pub fn style_column_headers(cells: Vec<&str>) -> Row {
    Row::new(cells).style(
        Style::default()
            .fg(Color::Yellow)
            .bg(Color::Black)
            .add_modifier(Modifier::BOLD),
    )
}

pub fn style_table_highlight() -> Style {
    Style::default()
        .add_modifier(Modifier::BOLD)
        .bg(Color::Yellow)
        .fg(Color::Black)
}

pub fn chart_style_deselected() -> Style {
    Style::default().fg(Color::Gray).bg(Color::Black)
}

pub fn table_style_deselected() -> Style {
    Style::default().fg(Color::Gray).bg(Color::Black)
}

pub fn table_style_selected() -> Style {
    Style::default().fg(Color::Yellow).bg(Color::Black)
}

pub fn style_no_data() -> Style {
    Style::default().fg(Color::Magenta).bg(Color::Black)
}
