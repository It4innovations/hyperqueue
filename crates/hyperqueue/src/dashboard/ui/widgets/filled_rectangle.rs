use ratatui::prelude::Color;
use ratatui::widgets::canvas::{Line, Painter, Shape};

#[derive(Debug)]
pub struct FilledRectangle {
    /// The `x` position of the rectangle.
    ///
    /// The rectangle is positioned from its bottom left corner.
    pub x: f64,
    /// The `y` position of the rectangle.
    ///
    /// The rectangle is positioned from its bottom left corner.
    pub y: f64,
    /// The width of the rectangle.
    pub width: f64,
    /// The height of the rectangle.
    pub height: f64,
    /// The color of the rectangle.
    pub color: Color,
}

impl Shape for FilledRectangle {
    fn draw(&self, painter: &mut Painter) {
        let mut value = self.y;
        while value < self.y + self.height {
            Line {
                x1: self.x,
                x2: self.x + self.width,
                y1: value,
                y2: value,
                color: self.color,
            }
            .draw(painter);
            value += 1.0;
        }
    }
}
