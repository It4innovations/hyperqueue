use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Span, Spans, Text},
    widgets::{Block, Borders, Paragraph, Row},
    Frame,
};

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
