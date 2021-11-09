use std::path::PathBuf;

use bstr::BString;
use humantime::format_rfc3339;
use tako::messages::common::ProgramDefinition;

use crate::common::env::{HQ_INSTANCE_ID, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
use crate::{JobId, Map};

pub const TASK_ID_PLACEHOLDER: &str = "%{TASK_ID}";
pub const JOB_ID_PLACEHOLDER: &str = "%{JOB_ID}";

type PlaceholderMap = Map<&'static str, String>;

pub fn replace_placeholders_worker(program: &mut ProgramDefinition) {
    let mut placeholder_map = Map::new();
    let job_id = program.env[&BString::from(HQ_JOB_ID)].to_string();
    let submit_dir = program.env[&BString::from(HQ_SUBMIT_DIR)].to_string();
    fill_server_context(&mut placeholder_map, job_id, submit_dir.clone());

    placeholder_map.insert(
        TASK_ID_PLACEHOLDER,
        program.env[&BString::from(HQ_TASK_ID)].to_string(),
    );
    placeholder_map.insert(
        "%{INSTANCE_ID}",
        program.env[&BString::from(HQ_INSTANCE_ID)].to_string(),
    );

    let replace_path = |map: &PlaceholderMap, path: &PathBuf| -> PathBuf {
        replace_placeholders(map, path.to_str().unwrap().into()).into()
    };

    // Replace CWD
    let submit_dir = PathBuf::from(submit_dir);
    program.cwd = program
        .cwd
        .as_ref()
        .map(|cwd| submit_dir.join(replace_path(&placeholder_map, cwd)))
        .or_else(|| Some(std::env::current_dir().unwrap()));

    // Replace STDOUT and STDERR
    placeholder_map.insert(
        "%{CWD}",
        program.cwd.as_ref().unwrap().to_str().unwrap().to_string(),
    );

    program.stdout = std::mem::take(&mut program.stdout)
        .map_filename(|path| submit_dir.join(replace_path(&placeholder_map, &path)));

    program.stderr = std::mem::take(&mut program.stderr)
        .map_filename(|path| submit_dir.join(replace_path(&placeholder_map, &path)));
}

pub fn replace_placeholders_server(value: &mut PathBuf, job_id: JobId, submit_dir: PathBuf) {
    let mut placeholder_map = Map::new();
    fill_server_context(
        &mut placeholder_map,
        job_id.to_string(),
        submit_dir.to_str().unwrap().to_string(),
    );

    *value = replace_placeholders(&placeholder_map, value.to_str().unwrap().to_string()).into();
}

pub fn replace_placeholders(map: &PlaceholderMap, mut value: String) -> String {
    for (placeholder, replacement) in map {
        value = value.replace(placeholder, replacement);
    }
    value
}

fn fill_server_context(map: &mut PlaceholderMap, job_id: String, submit_dir: String) {
    let date = format_rfc3339(std::time::SystemTime::now()).to_string();
    map.insert(JOB_ID_PLACEHOLDER, job_id);
    map.insert("%{SUBMIT_DIR}", submit_dir);
    map.insert("%{DATE}", date);
}

#[cfg(test)]
mod tests {
    use tako::messages::common::{ProgramDefinition, StdioDef};

    use crate::common::env::{HQ_INSTANCE_ID, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
    use crate::common::placeholders::replace_placeholders_worker;
    use crate::Map;

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
        replace_placeholders_worker(&mut program);
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
        replace_placeholders_worker(&mut program);
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
        replace_placeholders_worker(&mut program);

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
        replace_placeholders_worker(&mut program);
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
