use crate::client::output::outputs::Output;
use std::path::{Path, PathBuf};

pub struct GlobalSettings {
    server_dir: PathBuf,
    printer: Box<dyn Output>,
}

impl GlobalSettings {
    pub fn new(server_dir: PathBuf, printer: Box<dyn Output>) -> Self {
        GlobalSettings {
            server_dir,
            printer,
        }
    }

    pub fn server_directory(&self) -> &Path {
        &self.server_dir
    }

    pub fn printer(&self) -> &dyn Output {
        self.printer.as_ref()
    }
}
