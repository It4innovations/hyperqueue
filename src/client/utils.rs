use crate::common::error::error;
use crate::transfer::messages::ToClientMessage;
use cli_table::{Cell, CellStruct, Color, Style, Table};
use rmp_serde::decode::Error::OutOfRange;
use std::fmt::Display;

pub(crate) fn handle_message(
    message: crate::Result<ToClientMessage>,
) -> crate::Result<ToClientMessage> {
    match message {
        Ok(msg) => match msg {
            ToClientMessage::Error(e) => error(format!("Server error: {}", e)),
            _ => Ok(msg),
        },
        Err(e) => error(format!("Communication error: {}", e)),
    }
}

pub struct OutputStyle {
    plain_style: bool,
}

impl OutputStyle {
    pub fn new(plain_style: bool) -> Self {
        OutputStyle { plain_style }
    }

    pub fn bold(&self, cell: CellStruct) -> CellStruct {
        if !self.plain_style {
            cell.bold(true)
        } else {
            cell
        }
    }

    pub fn foreground_color(&self, cell: CellStruct, color: Color) -> CellStruct {
        if !self.plain_style {
            cell.foreground_color(Some(color))
        } else {
            cell
        }
    }

    pub fn apply_on_rows(&self, mut table: Vec<Vec<CellStruct>>) -> Vec<Vec<CellStruct>> {
        if self.plain_style {
            table
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| {
                            cell.bold(false)
                                .background_color(None)
                                .foreground_color(None)
                        })
                        .collect()
                })
                .collect()
        } else {
            table
        }
    }

    pub fn apply_on_row(&self, mut row: Vec<CellStruct>) -> Vec<CellStruct> {
        if self.plain_style {
            row.into_iter()
                .map(|cell| {
                    cell.bold(false)
                        .background_color(None)
                        .foreground_color(None)
                })
                .collect()
        } else {
            row
        }
    }
}
