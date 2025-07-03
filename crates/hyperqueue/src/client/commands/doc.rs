use crate::HQ_VERSION;
use clap::Parser;

/// Open HyperQueue documentation.
#[derive(Parser)]
pub struct DocOpts {
    /// Open the documentation in the default browser.
    #[clap(global = true, long, action)]
    open: bool,

    #[clap(subcommand)]
    pub subcmd: Option<DocArea>,
}

// Please update the `check_doc_path_on_disk` test below when adding
// variants to this enum.
#[derive(Parser)]
pub enum DocArea {
    /// Submitting and examining tasks and jobs.
    #[clap(visible_aliases = &["jobs", "tasks"])]
    Job,
    /// Jobs containing large amounts of similar tasks.
    #[clap(name("taskarray"))]
    TaskArray,
    /// CPU and generic resources of tasks
    Resources,
    /// Deployment of workers
    #[clap(visible_alias = "workers")]
    Worker,
    /// Automatic allocator subsystem.
    #[clap(name("autoalloc"), visible_aliases = &["pbs", "slurm"])]
    AutoAlloc,
    /// Python API
    #[clap(visible_alias = "python")]
    PythonAPI,
    /// Cheatsheet with the most common HyperQueue commands
    Cheatsheet,
    /// Changelog
    Changelog,
    /// Frequently asked questions about HyperQueue
    FAQ,
}

const DOC_ROOT: &str = "https://it4innovations.github.io/hyperqueue/";

pub fn command_doc(opts: DocOpts) -> anyhow::Result<()> {
    let link = match opts.subcmd {
        None => "",
        Some(cmd) => get_doc_path(&cmd),
    };
    let link = versioned_link(link);

    if opts.open {
        open::that(link)?;
    } else {
        println!("{link}");
    }
    Ok(())
}

fn get_doc_path(cmd: &DocArea) -> &'static str {
    match cmd {
        DocArea::Job => "jobs/jobs",
        DocArea::TaskArray => "jobs/arrays",
        DocArea::Resources => "jobs/resources",
        DocArea::Worker => "deployment/worker",
        DocArea::AutoAlloc => "deployment/allocation",
        DocArea::PythonAPI => "python",
        DocArea::Cheatsheet => "cheatsheet",
        DocArea::Changelog => "changelog",
        DocArea::FAQ => "faq",
    }
}

/// Generates a link to the current version of the documentation.
/// Note that the CI-built hyperqueue binaries already contain the `v` version prefix,
/// e.g. `v0.21.0`, but locally the `v` will be missing.
fn versioned_link(path: &str) -> String {
    format!("{DOC_ROOT}{HQ_VERSION}/{path}")
}

#[cfg(test)]
mod tests {
    use crate::client::commands::doc::{DocArea, get_doc_path};
    use std::path::PathBuf;

    #[test]
    fn check_doc_path_on_disk() {
        let areas = &[
            DocArea::Job,
            DocArea::TaskArray,
            DocArea::Resources,
            DocArea::Worker,
            DocArea::AutoAlloc,
            DocArea::PythonAPI,
            DocArea::Cheatsheet,
            DocArea::Changelog,
            DocArea::FAQ,
        ];
        let hq_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let root = hq_root.parent().unwrap().parent().unwrap();
        let docs_root = root.join("docs");

        for area in areas {
            let path = get_doc_path(area);
            let md_path = docs_root.join(path).with_extension("md");
            let index_path = docs_root.join(path).join("index.md");
            if !md_path.is_file() && !index_path.is_file() {
                panic!("File {md_path:?} nor {index_path:?} found on disk");
            }
        }
    }
}
