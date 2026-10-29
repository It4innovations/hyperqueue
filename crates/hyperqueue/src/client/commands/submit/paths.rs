use crate::common::placeholders::{
    CWD_PLACEHOLDER, INSTANCE_ID_PLACEHOLDER, SUBMIT_DIR_PLACEHOLDER, StringPart,
    TASK_ID_PLACEHOLDER, parse_resolvable_string,
};
use crate::transfer::messages::{
    JobSubmitDescription, JobTaskDescription, TaskDescription, TaskKind,
};
use std::path::Path;
use tako::program::StdioDef;

const ABSOLUTE_PLACEHOLDERS: [&str; 1] = [SUBMIT_DIR_PLACEHOLDER];
const ABSOLUTE_OUTPUT_PLACEHOLDERS: [&str; 2] = [SUBMIT_DIR_PLACEHOLDER, CWD_PLACEHOLDER];
/// The stream path is shared by all tasks of a job, so it cannot depend on a task.
const TASK_PLACEHOLDERS: [&str; 3] = [
    TASK_ID_PLACEHOLDER,
    INSTANCE_ID_PLACEHOLDER,
    CWD_PLACEHOLDER,
];

fn check_path(path: &Path, name: &str, placeholders: &[&str]) -> anyhow::Result<()> {
    if path.is_absolute() {
        return Ok(());
    }
    let starts_with_absolute_placeholder = path.to_str().is_some_and(|path| {
        matches!(
            parse_resolvable_string(path).first(),
            Some(StringPart::Placeholder(placeholder)) if placeholders.contains(placeholder)
        )
    });
    if starts_with_absolute_placeholder {
        return Ok(());
    }
    anyhow::bail!(
        "Path `{}` in `{name}` is not absolute. Use an absolute path or a path that starts with {}.",
        path.display(),
        placeholders
            .iter()
            .map(|placeholder| format!("`%{{{placeholder}}}`"))
            .collect::<Vec<_>>()
            .join(" or ")
    )
}

fn check_stdio(stdio: &StdioDef, name: &str) -> anyhow::Result<()> {
    match stdio {
        StdioDef::File { path, .. } => check_path(path, name, &ABSOLUTE_OUTPUT_PLACEHOLDERS),
        StdioDef::Null | StdioDef::Pipe => Ok(()),
    }
}

fn check_task(desc: &TaskDescription) -> anyhow::Result<()> {
    match &desc.kind {
        TaskKind::ExternalProgram(program) => {
            let program = &program.program;
            check_path(&program.cwd, "cwd", &ABSOLUTE_PLACEHOLDERS)?;
            check_stdio(&program.stdout, "stdout")?;
            check_stdio(&program.stderr, "stderr")
        }
    }
}

/// Checks that all task paths in the submit description are absolute or start with a placeholder.
/// The stream path is not checked.
pub fn validate_task_paths(desc: &JobSubmitDescription) -> anyhow::Result<()> {
    match &desc.task_desc {
        JobTaskDescription::Array { task_desc, .. } => check_task(task_desc),
        JobTaskDescription::Graph { tasks, .. } => {
            for task in tasks {
                check_task(&task.task_desc)?;
            }
            Ok(())
        }
    }
}

fn check_stream_placeholders(path: &Path) -> anyhow::Result<()> {
    let Some(path_str) = path.to_str() else {
        return Ok(());
    };
    for part in parse_resolvable_string(path_str) {
        if let StringPart::Placeholder(placeholder) = part
            && TASK_PLACEHOLDERS.contains(&placeholder)
        {
            anyhow::bail!(
                "Path `{}` in `stream` contains placeholder `%{{{placeholder}}}` that depends on a task. \
                 The stream path is shared by all tasks of a job.",
                path.display()
            );
        }
    }
    Ok(())
}

/// Checks that the stream path does not contain task-specific placeholders.
/// A relative stream path is allowed; it is resolved against the submit dir.
pub fn validate_stream_path(desc: &JobSubmitDescription) -> anyhow::Result<()> {
    if let Some(path) = &desc.stream_path {
        check_stream_placeholders(path)?;
    }
    Ok(())
}

/// Checks that all paths in the submit description (including the stream path)
/// are absolute or start with a placeholder.
pub fn validate_submit_paths(desc: &JobSubmitDescription) -> anyhow::Result<()> {
    validate_task_paths(desc)?;
    validate_stream_path(desc)?;
    if let Some(path) = &desc.stream_path {
        check_path(path, "stream", &ABSOLUTE_PLACEHOLDERS)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ABSOLUTE_OUTPUT_PLACEHOLDERS, ABSOLUTE_PLACEHOLDERS, check_path, check_stream_placeholders,
    };
    use std::path::Path;

    fn ok(path: &str, placeholders: &[&str]) -> bool {
        check_path(Path::new(path), "cwd", placeholders).is_ok()
    }

    #[test]
    fn accept_absolute_paths_and_absolute_placeholders() {
        for path in [
            "/",
            "/foo/bar",
            "%{SUBMIT_DIR}",
            "%{SUBMIT_DIR}/%{TASK_ID}/x",
        ] {
            assert!(ok(path, &ABSOLUTE_PLACEHOLDERS), "{path}");
        }
        assert!(ok("%{CWD}/%{TASK_ID}.out", &ABSOLUTE_OUTPUT_PLACEHOLDERS));
    }

    #[test]
    fn reject_relative_paths() {
        for path in [
            "",
            "foo",
            "foo/bar",
            "./foo",
            "../foo",
            "a/%{SUBMIT_DIR}",
            "%foo",
        ] {
            assert!(!ok(path, &ABSOLUTE_PLACEHOLDERS), "{path}");
        }
    }

    #[test]
    fn reject_placeholders_that_do_not_produce_an_absolute_path() {
        for path in [
            "%{TASK_ID}/x",
            "%{JOB_ID}",
            "%{INSTANCE_ID}/x",
            "%{SERVER_UID}/x",
            "%{UNKNOWN}/x",
        ] {
            assert!(!ok(path, &ABSOLUTE_OUTPUT_PLACEHOLDERS), "{path}");
        }
    }

    #[test]
    fn cwd_placeholder_only_in_output_paths() {
        assert!(!ok("%{CWD}/x", &ABSOLUTE_PLACEHOLDERS));
        assert!(ok("%{CWD}/x", &ABSOLUTE_OUTPUT_PLACEHOLDERS));
    }

    #[test]
    fn stream_placeholders() {
        for path in [
            "foo",
            "/foo",
            "%{SUBMIT_DIR}/foo",
            "foo-%{JOB_ID}",
            "%{SERVER_UID}/x",
        ] {
            assert!(check_stream_placeholders(Path::new(path)).is_ok(), "{path}");
        }
        for path in [
            "foo-%{TASK_ID}",
            "%{SUBMIT_DIR}/%{INSTANCE_ID}",
            "%{CWD}/foo",
        ] {
            assert!(
                check_stream_placeholders(Path::new(path)).is_err(),
                "{path}"
            );
        }
    }
}
