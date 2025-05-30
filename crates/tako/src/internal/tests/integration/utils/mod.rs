use std::path::Path;

pub mod api;
pub mod server;
pub mod task;
pub mod worker;

pub fn check_file_contents(path: &Path, expected: &[u8]) {
    assert_eq!(std::fs::read(path).unwrap(), expected.to_vec());
}
