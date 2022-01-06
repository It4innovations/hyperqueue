use std::borrow::Cow;
use std::fmt::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use bstr::BStr;
use nom::bytes::complete::take_until;
use nom::sequence::delimited;
use nom_supreme::tag::complete::tag;

use tako::messages::common::ProgramDefinition;

use crate::common::env::{HQ_INSTANCE_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
use crate::common::parser::NomResult;
use crate::{JobId, Map};

pub const TASK_ID_PLACEHOLDER: &str = "TASK_ID";
pub const JOB_ID_PLACEHOLDER: &str = "JOB_ID";
pub const INSTANCE_ID_PLACEHOLDER: &str = "INSTANCE_ID";
pub const CWD_PLACEHOLDER: &str = "CWD";
pub const SUBMIT_DIR_PLACEHOLDER: &str = "SUBMIT_DIR";

type PlaceholderMap<'a> = Map<&'static str, Cow<'a, str>>;

fn env_key(key: &str) -> &BStr {
    key.into()
}

/// Fills placeholder values known on the worker.
pub fn fill_placeholders_worker(program: &mut ProgramDefinition) {
    let mut placeholders = PlaceholderMap::new();

    let task_id = program.env[env_key(HQ_TASK_ID)].to_string();
    placeholders.insert(TASK_ID_PLACEHOLDER, task_id.into());

    let instance_id = program.env[env_key(HQ_INSTANCE_ID)].to_string();
    placeholders.insert(INSTANCE_ID_PLACEHOLDER, instance_id.into());

    let submit_dir: PathBuf = program.env[env_key(HQ_SUBMIT_DIR)].to_string().into();
    resolve_program_paths(placeholders, program, &submit_dir, true);
}

/// Fills placeholder values that are known immediately after a job is submitted.
pub fn fill_placeholders_after_submit(
    program: &mut ProgramDefinition,
    job_id: JobId,
    submit_dir: &Path,
) {
    let mut placeholders = PlaceholderMap::new();
    insert_submit_data(&mut placeholders, job_id, submit_dir);
    resolve_program_paths(placeholders, program, submit_dir, false);
}

pub fn fill_placeholders_log(value: &mut PathBuf, job_id: JobId, submit_dir: &Path) {
    let mut placeholders = PlaceholderMap::new();
    insert_submit_data(&mut placeholders, job_id, submit_dir);
    *value = resolve(&placeholders, value.to_str().unwrap()).into();
}

fn insert_submit_data<'a>(map: &mut PlaceholderMap<'a>, job_id: JobId, submit_dir: &'a Path) {
    map.insert(JOB_ID_PLACEHOLDER, job_id.to_string().into());
    map.insert(SUBMIT_DIR_PLACEHOLDER, submit_dir.to_str().unwrap().into());
}

fn resolve_program_paths<'a>(
    mut placeholders: PlaceholderMap<'a>,
    program: &'a mut ProgramDefinition,
    submit_dir: &Path,
    normalize_paths: bool,
) {
    // Replace CWD
    program.cwd = program
        .cwd
        .as_ref()
        .map(|cwd| resolve_path(&placeholders, cwd, submit_dir, normalize_paths))
        .or_else(|| Some(std::env::current_dir().unwrap()));

    if normalize_paths {
        placeholders.insert(
            CWD_PLACEHOLDER,
            program.cwd.as_ref().unwrap().to_str().unwrap().into(),
        );
    }

    program.stdout = std::mem::take(&mut program.stdout)
        .map_filename(|path| resolve_path(&placeholders, &path, submit_dir, normalize_paths));
    program.stderr = std::mem::take(&mut program.stderr)
        .map_filename(|path| resolve_path(&placeholders, &path, submit_dir, normalize_paths));
}

fn resolve(map: &PlaceholderMap, input: &str) -> String {
    let mut buffer = String::with_capacity(input.len());
    for placeholder in parse_resolvable_string(input) {
        match placeholder {
            StringPart::Verbatim(data) => buffer.write_str(data),
            StringPart::Placeholder(placeholder) => match map.get(placeholder) {
                Some(value) => buffer.write_str(value.deref()),
                None => {
                    log::warn!(
                        "Encountered an unknown placeholder `{}` in `{}`",
                        placeholder,
                        input
                    );
                    buffer.write_fmt(format_args!("%{{{}}}", placeholder))
                }
            },
        }
        .unwrap();
    }
    buffer
}

fn resolve_path(map: &PlaceholderMap, path: &Path, base_dir: &Path, normalize: bool) -> PathBuf {
    let path: PathBuf = resolve(map, path.to_str().unwrap()).into();
    if normalize {
        normalize_path(&path, base_dir)
    } else {
        path
    }
}

/// Adds the given base `directory` to the `path`, if it's not already absolute.
pub fn normalize_path(path: &Path, directory: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        [directory, path].into_iter().collect()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum StringPart<'a> {
    Verbatim(&'a str),
    Placeholder(&'a str),
}

fn parse_placeholder(data: &str) -> NomResult<&str> {
    delimited(tag("%{"), take_until("}"), tag("}"))(data)
}

/// Parses strings containing placeholders.
///
/// # Example
/// ```rust
/// use hyperqueue::common::placeholders::{parse_resolvable_string, StringPart};
///
/// assert_eq!(parse_resolvable_string("a%{b}c"), vec![
///     StringPart::Verbatim("a"),
///     StringPart::Placeholder("b"),
///     StringPart::Verbatim("c"),
/// ]);
/// ```
pub fn parse_resolvable_string(data: &str) -> Vec<StringPart> {
    let mut parts = vec![];
    let mut start = 0;
    let mut input = data;

    while start < input.len() {
        if let Ok((rest, placeholder)) = parse_placeholder(&input[start..]) {
            if start > 0 {
                parts.push(StringPart::Verbatim(&input[..start]));
            }
            input = rest;
            parts.push(StringPart::Placeholder(placeholder));
            start = 0;
        } else {
            start += 1;
        }
    }

    if start > 0 {
        parts.push(StringPart::Verbatim(&input[..start]));
    }

    parts
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use tako::messages::common::{ProgramDefinition, StdioDef};

    use crate::common::env::{HQ_INSTANCE_ID, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
    use crate::common::placeholders::{
        fill_placeholders_after_submit, fill_placeholders_worker, parse_resolvable_string,
        StringPart,
    };
    use crate::Map;

    #[test]
    fn test_parse_empty_string() {
        assert!(parse_resolvable_string("").is_empty());
    }

    #[test]
    fn test_parse_verbatim_only() {
        assert_eq!(
            parse_resolvable_string("foo"),
            vec![StringPart::Verbatim("foo")]
        );
    }

    #[test]
    fn test_parse_placeholder_only() {
        assert_eq!(
            parse_resolvable_string("%{FOO}"),
            vec![StringPart::Placeholder("FOO")]
        );
    }

    #[test]
    fn test_parse_placeholder_start() {
        assert_eq!(
            parse_resolvable_string("%{FOO}BAR"),
            vec![StringPart::Placeholder("FOO"), StringPart::Verbatim("BAR")]
        );
    }

    #[test]
    fn test_parse_placeholder_middle() {
        assert_eq!(
            parse_resolvable_string("BAZ%{FOO}BAR"),
            vec![
                StringPart::Verbatim("BAZ"),
                StringPart::Placeholder("FOO"),
                StringPart::Verbatim("BAR")
            ]
        );
    }

    #[test]
    fn test_parse_placeholder_end() {
        assert_eq!(
            parse_resolvable_string("BAR%{FOO}"),
            vec![StringPart::Verbatim("BAR"), StringPart::Placeholder("FOO")]
        );
    }

    #[test]
    fn test_parse_placeholder_multiple() {
        assert_eq!(
            parse_resolvable_string("A%{B}C%{D}E"),
            vec![
                StringPart::Verbatim("A"),
                StringPart::Placeholder("B"),
                StringPart::Verbatim("C"),
                StringPart::Placeholder("D"),
                StringPart::Verbatim("E")
            ]
        );
    }

    #[test]
    fn test_parse_percent() {
        assert_eq!(
            parse_resolvable_string("%"),
            vec![StringPart::Verbatim("%")]
        );
    }

    #[test]
    fn test_replace_after_submit() {
        let mut program = program_def(
            "%{SUBMIT_DIR}/%{JOB_ID}-%{TASK_ID}",
            Some("%{CWD}.out"),
            Some("%{CWD}.err"),
            "/foo",
            99,
            99,
        );
        fill_placeholders_after_submit(&mut program, 5.into(), &PathBuf::from("/foo"));
        assert_eq!(program.cwd, Some("/foo/5-%{TASK_ID}".into()));
        assert_eq!(program.stdout, StdioDef::File("%{CWD}.out".into()));
        assert_eq!(program.stderr, StdioDef::File("%{CWD}.err".into()));
    }

    #[test]
    fn test_replace_worker() {
        let mut program = program_def(
            "%{TASK_ID}-%{INSTANCE_ID}",
            Some("%{CWD}/stdout"),
            Some("%{CWD}/stderr"),
            "/foo",
            5,
            10,
        );
        fill_placeholders_worker(&mut program);
        assert_eq!(program.cwd, Some("/foo/10-0".into()));
        assert_eq!(program.stdout, StdioDef::File("/foo/10-0/stdout".into()));
        assert_eq!(program.stderr, StdioDef::File("/foo/10-0/stderr".into()));
    }

    #[test]
    fn test_fill_cwd_into_output_paths() {
        let mut program = program_def(
            "bar/%{TASK_ID}",
            Some("%{CWD}/%{TASK_ID}.out"),
            Some("%{CWD}/%{TASK_ID}.err"),
            "/foo",
            5,
            1,
        );
        fill_placeholders_worker(&mut program);
        assert_eq!(program.cwd, Some("/foo/bar/1".into()));
        assert_eq!(program.stdout, StdioDef::File("/foo/bar/1/1.out".into()));
        assert_eq!(program.stderr, StdioDef::File("/foo/bar/1/1.err".into()));
    }

    fn program_def(
        cwd: &str,
        stdout: Option<&str>,
        stderr: Option<&str>,
        submit_dir: &str,
        job_id: u32,
        task_id: u32,
    ) -> ProgramDefinition {
        let mut env = Map::new();
        env.insert(HQ_SUBMIT_DIR.into(), submit_dir.into());
        env.insert(HQ_JOB_ID.into(), job_id.to_string().into());
        env.insert(HQ_TASK_ID.into(), task_id.to_string().into());
        env.insert(HQ_INSTANCE_ID.into(), "0".into());

        ProgramDefinition {
            args: vec![],
            env,
            stdout: stdout.map(|v| StdioDef::File(v.into())).unwrap_or_default(),
            stderr: stderr.map(|v| StdioDef::File(v.into())).unwrap_or_default(),
            cwd: Some(cwd.into()),
        }
    }
}
