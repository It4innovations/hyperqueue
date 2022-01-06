use std::borrow::Cow;
use std::fmt::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use bstr::BString;
use nom::bytes::complete::take_until;
use nom::sequence::delimited;
use nom_supreme::tag::complete::tag;

use tako::messages::common::ProgramDefinition;

use crate::common::env::{HQ_INSTANCE_ID, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
use crate::common::parser::NomResult;
use crate::{JobId, Map};

pub const TASK_ID_PLACEHOLDER: &str = "TASK_ID";
pub const JOB_ID_PLACEHOLDER: &str = "JOB_ID";
pub const INSTANCE_ID_PLACEHOLDER: &str = "INSTANCE_ID";
pub const CWD_PLACEHOLDER: &str = "CWD";
pub const SUBMIT_DIR_PLACEHOLDER: &str = "SUBMIT_DIR";

type PlaceholderMap<'a> = Map<&'static str, Cow<'a, str>>;

pub fn fill_placeholders_worker(program: &mut ProgramDefinition) {
    let mut placeholders = Map::new();
    let job_id = program.env[&BString::from(HQ_JOB_ID)].to_string();
    let task_id = program.env[&BString::from(HQ_TASK_ID)].to_string();
    let instance_id = program.env[&BString::from(HQ_INSTANCE_ID)].to_string();
    let submit_dir = program.env[&BString::from(HQ_SUBMIT_DIR)].to_string();

    placeholders.insert(JOB_ID_PLACEHOLDER, job_id.into());
    placeholders.insert(TASK_ID_PLACEHOLDER, task_id.into());
    placeholders.insert(INSTANCE_ID_PLACEHOLDER, instance_id.into());
    placeholders.insert(SUBMIT_DIR_PLACEHOLDER, submit_dir.clone().into());

    let replace_path = |map: &PlaceholderMap, path: &PathBuf, base_dir: &Path| -> PathBuf {
        let path: PathBuf = replace_placeholders(map, path.to_str().unwrap()).into();
        normalize_path(&path, base_dir)
    };

    // Replace CWD
    let submit_dir = PathBuf::from(submit_dir);
    program.cwd = program
        .cwd
        .as_ref()
        .map(|cwd| replace_path(&placeholders, cwd, &submit_dir))
        .or_else(|| Some(std::env::current_dir().unwrap()));

    // Replace STDOUT and STDERR
    placeholders.insert(
        CWD_PLACEHOLDER,
        program.cwd.as_ref().unwrap().to_str().unwrap().into(),
    );

    program.stdout = std::mem::take(&mut program.stdout)
        .map_filename(|path| replace_path(&placeholders, &path, &submit_dir));
    program.stderr = std::mem::take(&mut program.stderr)
        .map_filename(|path| replace_path(&placeholders, &path, &submit_dir));
}

pub fn fill_placeholders_log(value: &mut PathBuf, job_id: JobId, submit_dir: &Path) {
    let mut map = Map::new();
    map.insert(JOB_ID_PLACEHOLDER, job_id.to_string().into());
    map.insert(SUBMIT_DIR_PLACEHOLDER, submit_dir.to_str().unwrap().into());
    *value = replace_placeholders(&map, value.to_str().unwrap()).into();
}

fn replace_placeholders(map: &PlaceholderMap, input: &str) -> String {
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
    use tako::messages::common::{ProgramDefinition, StdioDef};

    use crate::common::env::{HQ_INSTANCE_ID, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
    use crate::common::placeholders::{
        fill_placeholders_worker, parse_resolvable_string, StringPart,
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
    fn test_replace_task_id() {
        let mut program = program_def(
            "dir-%{TASK_ID}",
            Some("%{TASK_ID}.out"),
            Some("%{TASK_ID}.err"),
            "",
            0,
            1,
        );
        fill_placeholders_worker(&mut program);
        assert_eq!(program.cwd, Some("dir-1".into()));
        assert_eq!(program.stdout, StdioDef::File("1.out".into()));
        assert_eq!(program.stderr, StdioDef::File("1.err".into()));
    }

    #[test]
    fn test_replace_job_id() {
        let mut program = program_def(
            "dir-%{JOB_ID}-%{TASK_ID}",
            Some("%{JOB_ID}-%{TASK_ID}.out"),
            Some("%{JOB_ID}-%{TASK_ID}.err"),
            "",
            5,
            1,
        );
        fill_placeholders_worker(&mut program);
        assert_eq!(program.cwd, Some("dir-5-1".into()));
        assert_eq!(program.stdout, StdioDef::File("5-1.out".into()));
        assert_eq!(program.stderr, StdioDef::File("5-1.err".into()));
    }

    #[test]
    fn test_replace_submit_dir() {
        let mut program = program_def(
            "%{SUBMIT_DIR}",
            Some("%{SUBMIT_DIR}/out"),
            Some("%{SUBMIT_DIR}/err"),
            "/submit-dir",
            5,
            1,
        );
        fill_placeholders_worker(&mut program);

        assert_eq!(program.cwd, Some("/submit-dir".into()));
        assert_eq!(program.stdout, StdioDef::File("/submit-dir/out".into()));
        assert_eq!(program.stderr, StdioDef::File("/submit-dir/err".into()));
    }

    #[test]
    fn test_replace_cwd() {
        let mut program = program_def(
            "dir-%{JOB_ID}-%{TASK_ID}",
            Some("%{CWD}.out"),
            Some("%{CWD}.err"),
            "",
            5,
            1,
        );
        fill_placeholders_worker(&mut program);
        assert_eq!(program.cwd, Some("dir-5-1".into()));
        assert_eq!(program.stdout, StdioDef::File("dir-5-1.out".into()));
        assert_eq!(program.stderr, StdioDef::File("dir-5-1.err".into()));
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
