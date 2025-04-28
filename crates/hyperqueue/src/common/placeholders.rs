use std::borrow::Cow;
use std::fmt::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use nom::bytes::complete::take_until;
use nom::sequence::delimited;
use nom_supreme::tag::complete::tag;
use tako::InstanceId;

use tako::program::{ProgramDefinition, StdioDef};

use crate::common::parser::NomResult;
use tako::{JobId, JobTaskId, Map};

pub const SERVER_UID_PLACEHOLDER: &str = "SERVER_UID";
pub const TASK_ID_PLACEHOLDER: &str = "TASK_ID";
pub const JOB_ID_PLACEHOLDER: &str = "JOB_ID";
pub const INSTANCE_ID_PLACEHOLDER: &str = "INSTANCE_ID";
pub const CWD_PLACEHOLDER: &str = "CWD";
pub const SUBMIT_DIR_PLACEHOLDER: &str = "SUBMIT_DIR";

const KNOWN_PLACEHOLDERS: [&str; 6] = [
    SERVER_UID_PLACEHOLDER,
    TASK_ID_PLACEHOLDER,
    JOB_ID_PLACEHOLDER,
    INSTANCE_ID_PLACEHOLDER,
    CWD_PLACEHOLDER,
    SUBMIT_DIR_PLACEHOLDER,
];

type PlaceholderMap<'a> = Map<&'static str, Cow<'a, str>>;

pub struct CompletePlaceholderCtx<'a> {
    pub job_id: JobId,
    pub task_id: JobTaskId,
    pub instance_id: InstanceId,
    pub submit_dir: &'a Path,
    pub server_uid: &'a str,
}

pub struct ResolvablePaths<'a> {
    pub cwd: &'a mut PathBuf,
    pub stdout: &'a mut StdioDef,
    pub stderr: &'a mut StdioDef,
}

impl<'a> ResolvablePaths<'a> {
    pub fn from_program_def(program: &'a mut ProgramDefinition) -> Self {
        Self {
            cwd: &mut program.cwd,
            stdout: &mut program.stdout,
            stderr: &mut program.stderr,
        }
    }
}

/// Fills all known placeholders in the given paths.
pub fn fill_placeholders_in_paths(paths: ResolvablePaths, ctx: CompletePlaceholderCtx) {
    let mut placeholders = PlaceholderMap::new();
    placeholders.insert(JOB_ID_PLACEHOLDER, ctx.job_id.to_string().into());
    placeholders.insert(TASK_ID_PLACEHOLDER, ctx.task_id.to_string().into());
    placeholders.insert(INSTANCE_ID_PLACEHOLDER, ctx.instance_id.to_string().into());
    placeholders.insert(
        SUBMIT_DIR_PLACEHOLDER,
        ctx.submit_dir.to_str().unwrap().into(),
    );
    placeholders.insert(SERVER_UID_PLACEHOLDER, ctx.server_uid.to_string().into());

    resolve_program_paths(placeholders, paths, ctx.submit_dir);
}

/// Fills placeholder values that are known immediately after a job is submitted.
pub fn fill_placeholders_after_submit(
    program: &mut ProgramDefinition,
    job_id: JobId,
    submit_dir: &Path,
    server_uid: &str,
) {
    let mut placeholders = PlaceholderMap::new();
    insert_submit_data(&mut placeholders, job_id, submit_dir, server_uid);
    resolve_program_paths(
        placeholders,
        ResolvablePaths::from_program_def(program),
        submit_dir,
    );
}

pub fn fill_placeholders_log(
    value: &mut PathBuf,
    job_id: JobId,
    submit_dir: &Path,
    server_uid: &str,
) {
    let mut placeholders = PlaceholderMap::new();
    insert_submit_data(&mut placeholders, job_id, submit_dir, server_uid);
    *value = resolve(&placeholders, value.to_str().unwrap())
        .into_owned()
        .into();
}

/// Find placeholders in the input that are not supported by HyperQueue.
pub fn get_unknown_placeholders(input: &str) -> Vec<&str> {
    let mut unknown = Vec::new();
    for placeholder in parse_resolvable_string(input) {
        if let StringPart::Placeholder(placeholder) = placeholder {
            if !KNOWN_PLACEHOLDERS.contains(&placeholder) {
                unknown.push(placeholder);
            }
        }
    }
    unknown
}

fn insert_submit_data<'a>(
    map: &mut PlaceholderMap<'a>,
    job_id: JobId,
    submit_dir: &'a Path,
    server_id: &'a str,
) {
    map.insert(JOB_ID_PLACEHOLDER, job_id.to_string().into());
    map.insert(SUBMIT_DIR_PLACEHOLDER, submit_dir.to_str().unwrap().into());
    map.insert(SERVER_UID_PLACEHOLDER, server_id.into());
}

fn resolve_program_paths<'a>(
    mut placeholders: PlaceholderMap<'a>,
    paths: ResolvablePaths<'a>,
    submit_dir: &Path,
) {
    *paths.cwd = resolve_path(&placeholders, paths.cwd, submit_dir);
    placeholders.insert(CWD_PLACEHOLDER, paths.cwd.to_str().unwrap().into());

    *paths.stdout = std::mem::take(paths.stdout)
        .map_filename(|path| resolve_path(&placeholders, &path, paths.cwd));
    *paths.stderr = std::mem::take(paths.stderr)
        .map_filename(|path| resolve_path(&placeholders, &path, paths.cwd));
}

fn resolve<'b>(map: &PlaceholderMap, input: &'b str) -> Cow<'b, str> {
    let parts = parse_resolvable_string(input);

    let is_known_placeholder = |part: &StringPart| match part {
        StringPart::Verbatim(_) => false,
        StringPart::Placeholder(placeholder) => map.contains_key(placeholder),
    };

    if !parts.iter().any(is_known_placeholder) {
        return input.into();
    }

    let mut buffer = String::with_capacity(input.len());
    for part in parts {
        match part {
            StringPart::Verbatim(data) => buffer.push_str(data),
            StringPart::Placeholder(placeholder) => match map.get(placeholder) {
                Some(value) => buffer.push_str(value.deref()),
                None => buffer
                    .write_fmt(format_args!("%{{{placeholder}}}"))
                    .unwrap(),
            },
        }
    }
    buffer.into()
}

fn resolve_path(map: &PlaceholderMap, path: &Path, base_dir: &Path) -> PathBuf {
    let path: PathBuf = resolve(map, path.to_str().unwrap()).into_owned().into();
    if !has_placeholders(path.to_str().unwrap()) {
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

pub fn has_placeholders(data: &str) -> bool {
    if !data.contains('%') {
        false
    } else {
        parse_resolvable_string(data)
            .iter()
            .any(|part| matches!(part, StringPart::Placeholder(_)))
    }
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
    let mut input = data;
    let mut start = 0;

    while start < input.len() {
        match input[start..].find('%') {
            Some(pos) => {
                if let Ok((rest, placeholder)) = parse_placeholder(&input[pos..]) {
                    if pos > 0 {
                        parts.push(StringPart::Verbatim(&input[..pos]));
                    }
                    input = rest;
                    start = 0;
                    parts.push(StringPart::Placeholder(placeholder));
                } else {
                    start += 1;
                }
            }
            None => break,
        };
    }

    if !input.is_empty() {
        parts.push(StringPart::Verbatim(input));
    }

    parts
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use tako::program::{FileOnCloseBehavior, ProgramDefinition, StdioDef};

    use crate::common::env::{HQ_INSTANCE_ID, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
    use crate::common::placeholders::{
        fill_placeholders_after_submit, fill_placeholders_in_paths, parse_resolvable_string,
        CompletePlaceholderCtx, ResolvablePaths, StringPart,
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
            "%{SUBMIT_DIR}/%{SERVER_UID}/%{JOB_ID}-%{TASK_ID}",
            Some("%{CWD}/out"),
            Some("%{CWD}/err"),
            "/foo",
            99,
            99,
        );
        fill_placeholders_after_submit(&mut program, 5.into(), &PathBuf::from("/foo"), "testHQ");
        assert_eq!(program.cwd, PathBuf::from("/foo/testHQ/5-%{TASK_ID}"));
        assert_eq!(
            program.stdout,
            StdioDef::File {
                path: "/foo/testHQ/5-%{TASK_ID}/out".into(),
                on_close: FileOnCloseBehavior::None
            }
        );
        assert_eq!(
            program.stderr,
            StdioDef::File {
                path: "/foo/testHQ/5-%{TASK_ID}/err".into(),
                on_close: FileOnCloseBehavior::None
            }
        );
    }

    #[test]
    fn test_replace_paths() {
        let submit_dir = PathBuf::from("/foo");
        let ctx = ctx(5, 10, 6, &submit_dir, "testHQ");
        let mut paths = paths(
            "%{SERVER_UID}-%{TASK_ID}-%{INSTANCE_ID}",
            "%{JOB_ID}/stdout",
            "%{JOB_ID}/stderr",
        );

        fill_placeholders_in_paths(ResolvablePaths::from_paths(&mut paths), ctx);
        assert_eq!(paths.cwd, PathBuf::from("/foo/testHQ-10-6"));
        assert_eq!(
            paths.stdout,
            StdioDef::File {
                path: "/foo/testHQ-10-6/5/stdout".into(),
                on_close: FileOnCloseBehavior::None
            }
        );
        assert_eq!(
            paths.stderr,
            StdioDef::File {
                path: "/foo/testHQ-10-6/5/stderr".into(),
                on_close: FileOnCloseBehavior::None
            }
        );
    }

    #[test]
    fn test_fill_cwd_into_output_paths() {
        let submit_dir = PathBuf::from("/foo");
        let ctx = ctx(1, 2, 3, &submit_dir, "testHQ");
        let mut paths = paths(
            "bar/%{JOB_ID}",
            "%{CWD}/%{TASK_ID}.out",
            "%{CWD}/%{TASK_ID}.err",
        );

        fill_placeholders_in_paths(ResolvablePaths::from_paths(&mut paths), ctx);
        assert_eq!(paths.cwd, PathBuf::from("/foo/bar/1"));
        assert_eq!(
            paths.stdout,
            StdioDef::File {
                path: "/foo/bar/1/2.out".into(),
                on_close: FileOnCloseBehavior::None
            }
        );
        assert_eq!(
            paths.stderr,
            StdioDef::File {
                path: "/foo/bar/1/2.err".into(),
                on_close: FileOnCloseBehavior::None
            }
        );
    }

    struct ResolvedPaths {
        cwd: PathBuf,
        stdout: StdioDef,
        stderr: StdioDef,
    }

    impl<'a> ResolvablePaths<'a> {
        fn from_paths(paths: &'a mut ResolvedPaths) -> Self {
            Self {
                cwd: &mut paths.cwd,
                stdout: &mut paths.stdout,
                stderr: &mut paths.stderr,
            }
        }
    }

    fn paths(cwd: &str, stdout: &str, stderr: &str) -> ResolvedPaths {
        ResolvedPaths {
            cwd: cwd.to_string().into(),
            stdout: StdioDef::File {
                path: stdout.to_string().into(),
                on_close: FileOnCloseBehavior::None,
            },
            stderr: StdioDef::File {
                path: stderr.to_string().into(),
                on_close: FileOnCloseBehavior::None,
            },
        }
    }

    fn ctx<'a>(
        job_id: u32,
        task_id: u32,
        instance_id: u32,
        submit_dir: &'a Path,
        server_uid: &'a str,
    ) -> CompletePlaceholderCtx<'a> {
        CompletePlaceholderCtx {
            job_id: job_id.into(),
            task_id: task_id.into(),
            instance_id: instance_id.into(),
            submit_dir,
            server_uid,
        }
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
            stdout: stdout
                .map(|v| StdioDef::File {
                    path: v.into(),
                    on_close: FileOnCloseBehavior::None,
                })
                .unwrap_or_default(),
            stderr: stderr
                .map(|v| StdioDef::File {
                    path: v.into(),
                    on_close: FileOnCloseBehavior::None,
                })
                .unwrap_or_default(),
            stdin: vec![],
            cwd: cwd.into(),
        }
    }
}
