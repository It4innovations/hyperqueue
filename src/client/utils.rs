use std::fmt::Display;

use cli_table::{Cell, CellStruct, Color, Style, Table};
use rmp_serde::decode::Error::OutOfRange;

use crate::common::error::error;
use crate::common::serverdir::AccessRecord;
use crate::transfer::connection::HqConnection;
use crate::transfer::messages::{FromClientMessage, ToClientMessage, WorkerListResponse};

#[macro_export]
macro_rules! rpc_call {
    ($conn:expr, $message:expr, $matcher:pat $(=> $result:expr)?) => {
        async move {
            match $conn.send_and_receive($message).await? {
                $matcher => $crate::Result::Ok(($($result),*)),
                msg => {
                    $crate::common::error::error(format!("Received an invalid message {:?}", msg))
                }
            }
        }
    };
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
