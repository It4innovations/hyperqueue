use std::path::Path;

mod api;
pub mod server;
#[cfg(test)]
mod task;
mod worker;

pub fn check_file_contents(path: &Path, expected: &[u8]) {
    assert_eq!(std::fs::read(path).unwrap(), expected.to_vec());
}
