use std::borrow::Cow;
use std::fmt::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use nom::bytes::complete::take_until;
use nom::sequence::delimited;
use nom_supreme::tag::complete::tag;
use tako::InstanceId;

use tako::messages::common::{ProgramDefinition, StdioDef};

use crate::common::parser::NomResult;
use crate::{JobId, JobTaskId, Map};

// If a new placeholder is added, also change `get_unknown_placeholders`
pub const TASK_ID_PLACEHOLDER: &str = "TASK_ID";
pub const JOB_ID_PLACEHOLDER: &str = "JOB_ID";
pub const INSTANCE_ID_PLACEHOLDER: &str = "INSTANCE_ID";
pub const CWD_PLACEHOLDER: &str = "CWD";
pub const SUBMIT_DIR_PLACEHOLDER: &str = "SUBMIT_DIR";

type PlaceholderMap<'a> = Map<&'static str, Cow<'a, str>>;

pub struct CompletePlaceholderCtx<'a> {
    pub job_id: JobId,
    pub task_id: JobTaskId,
    pub instance_id: InstanceId,
    pub submit_dir: &'a Path,
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

    resolve_program_paths(placeholders, paths, ctx.submit_dir, true);
}

/// Fills placeholder values that are known immediately after a job is submitted.
pub fn fill_placeholders_after_submit(
    program: &mut ProgramDefinition,
    job_id: JobId,
    submit_dir: &Path,
) {
    let mut placeholders = PlaceholderMap::new();
    insert_submit_data(&mut placeholders, job_id, submit_dir);
    resolve_program_paths(
        placeholders,
        ResolvablePaths::from_program_def(program),
        submit_dir,
        false,
    );
}

pub fn fill_placeholders_log(value: &mut PathBuf, job_id: JobId, submit_dir: &Path) {
    let mut placeholders = PlaceholderMap::new();
    insert_submit_data(&mut placeholders, job_id, submit_dir);
    *value = resolve(&placeholders, value.to_str().unwrap())
        .into_owned()
        .into();
}

/// Find placeholders in the input that are not supported by HyperQueue.
pub fn get_unknown_placeholders(input: &str) -> Vec<&str> {
    let known_placeholders = [
        TASK_ID_PLACEHOLDER,
        JOB_ID_PLACEHOLDER,
        INSTANCE_ID_PLACEHOLDER,
        CWD_PLACEHOLDER,
        SUBMIT_DIR_PLACEHOLDER,
    ];

    let mut unknown = Vec::new();
    for placeholder in parse_resolvable_string(input) {
        if let StringPart::Placeholder(placeholder) = placeholder {
            if !known_placeholders.contains(&placeholder) {
                unknown.push(placeholder);
            }
        }
    }
    unknown
}

fn insert_submit_data<'a>(map: &mut PlaceholderMap<'a>, job_id: JobId, submit_dir: &'a Path) {
    map.insert(JOB_ID_PLACEHOLDER, job_id.to_string().into());
    map.insert(SUBMIT_DIR_PLACEHOLDER, submit_dir.to_str().unwrap().into());
}

fn resolve_program_paths<'a>(
    mut placeholders: PlaceholderMap<'a>,
    paths: ResolvablePaths<'a>,
    submit_dir: &Path,
    normalize_paths: bool,
) {
    *paths.cwd = resolve_path(&placeholders, paths.cwd, submit_dir, normalize_paths);

    if normalize_paths {
        placeholders.insert(CWD_PLACEHOLDER, paths.cwd.to_str().unwrap().into());
    }

    *paths.stdout = std::mem::take(paths.stdout)
        .map_filename(|path| resolve_path(&placeholders, &path, submit_dir, normalize_paths));
    *paths.stderr = std::mem::take(paths.stderr)
        .map_filename(|path| resolve_path(&placeholders, &path, submit_dir, normalize_paths));
}

fn resolve<'a, 'b>(map: &'a PlaceholderMap, input: &'b str) -> Cow<'b, str> {
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
                    .write_fmt(format_args!("%{{{}}}", placeholder))
                    .unwrap(),
            },
        }
    }
    buffer.into()
}

fn resolve_path(map: &PlaceholderMap, path: &Path, base_dir: &Path, normalize: bool) -> PathBuf {
    let path: PathBuf = resolve(map, path.to_str().unwrap()).into_owned().into();
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
    use std::path::{Path, PathBuf};
    use tako::messages::common::{ProgramDefinition, StdioDef};

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
            "%{SUBMIT_DIR}/%{JOB_ID}-%{TASK_ID}",
            Some("%{CWD}.out"),
            Some("%{CWD}.err"),
            "/foo",
            99,
            99,
        );
        fill_placeholders_after_submit(&mut program, 5.into(), &PathBuf::from("/foo"));
        assert_eq!(program.cwd, PathBuf::from("/foo/5-%{TASK_ID}"));
        assert_eq!(program.stdout, StdioDef::File("%{CWD}.out".into()));
        assert_eq!(program.stderr, StdioDef::File("%{CWD}.err".into()));
    }

    #[test]
    fn test_replace_paths() {
        let submit_dir = PathBuf::from("/foo");
        let ctx = ctx(5, 10, 6, &submit_dir);
        let mut paths = paths(
            "%{TASK_ID}-%{INSTANCE_ID}",
            "%{JOB_ID}/stdout",
            "%{JOB_ID}/stderr",
        );

        fill_placeholders_in_paths(ResolvablePaths::from_paths(&mut paths), ctx);
        assert_eq!(paths.cwd, PathBuf::from("/foo/10-6"));
        assert_eq!(paths.stdout, StdioDef::File("/foo/5/stdout".into()));
        assert_eq!(paths.stderr, StdioDef::File("/foo/5/stderr".into()));
    }

    #[test]
    fn test_fill_cwd_into_output_paths() {
        let submit_dir = PathBuf::from("/foo");
        let ctx = ctx(1, 2, 3, &submit_dir);
        let mut paths = paths(
            "bar/%{JOB_ID}",
            "%{CWD}/%{TASK_ID}.out",
            "%{CWD}/%{TASK_ID}.err",
        );

        fill_placeholders_in_paths(ResolvablePaths::from_paths(&mut paths), ctx);
        assert_eq!(paths.cwd, PathBuf::from("/foo/bar/1"));
        assert_eq!(paths.stdout, StdioDef::File("/foo/bar/1/2.out".into()));
        assert_eq!(paths.stderr, StdioDef::File("/foo/bar/1/2.err".into()));
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
            stdout: StdioDef::File(stdout.to_string().into()),
            stderr: StdioDef::File(stderr.to_string().into()),
        }
    }

    fn ctx(
        job_id: u32,
        task_id: u32,
        instance_id: u32,
        submit_dir: &Path,
    ) -> CompletePlaceholderCtx {
        CompletePlaceholderCtx {
            job_id: job_id.into(),
            task_id: task_id.into(),
            instance_id: instance_id.into(),
            submit_dir,
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
            stdout: stdout.map(|v| StdioDef::File(v.into())).unwrap_or_default(),
            stderr: stderr.map(|v| StdioDef::File(v.into())).unwrap_or_default(),
            stdin: vec![],
            cwd: cwd.into(),
        }
    }
}
