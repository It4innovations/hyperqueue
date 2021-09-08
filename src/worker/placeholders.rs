use std::path::PathBuf;

use bstr::{BString, ByteSlice};
use humantime::format_rfc3339;
use tako::messages::common::ProgramDefinition;

use crate::common::env::{HQ_INSTANCE_ID, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
use crate::Map;

pub const TASK_ID_PLACEHOLDER: &str = "%{TASK_ID}";
pub const JOB_ID_PLACEHOLDER: &str = "%{JOB_ID}";

/// Replace placeholders in user-defined program attributes
pub fn replace_placeholders(program: &mut ProgramDefinition) {
    let date = format_rfc3339(std::time::SystemTime::now()).to_string();
    let submit_dir = PathBuf::from(
        program.env[&BString::from(HQ_SUBMIT_DIR)]
            .to_os_str()
            .unwrap_or_default(),
    );

    let mut placeholder_map = Map::new();
    placeholder_map.insert(
        JOB_ID_PLACEHOLDER,
        program.env[&BString::from(HQ_JOB_ID)].to_string(),
    );
    placeholder_map.insert(
        TASK_ID_PLACEHOLDER,
        program.env[&BString::from(HQ_TASK_ID)].to_string(),
    );
    placeholder_map.insert(
        "%{INSTANCE_ID}",
        program.env[&BString::from(HQ_INSTANCE_ID)].to_string(),
    );
    placeholder_map.insert(
        "%{SUBMIT_DIR}",
        program.env[&BString::from(HQ_SUBMIT_DIR)].to_string(),
    );
    placeholder_map.insert("%{DATE}", date);

    let replace = |replacement_map: &Map<&str, String>, path: &PathBuf| -> PathBuf {
        let mut result: String = path.to_str().unwrap().into();
        for (placeholder, replacement) in replacement_map.iter() {
            result = result.replace(placeholder, replacement);
        }
        result.into()
    };

    // Replace CWD
    program.cwd = program
        .cwd
        .as_ref()
        .map(|cwd| submit_dir.join(replace(&placeholder_map, cwd)))
        .or_else(|| Some(std::env::current_dir().unwrap()));

    // Replace STDOUT and STDERR
    placeholder_map.insert(
        "%{CWD}",
        program.cwd.as_ref().unwrap().to_str().unwrap().to_string(),
    );

    program.stdout = std::mem::take(&mut program.stdout)
        .map_filename(|path| submit_dir.join(replace(&placeholder_map, &path)));

    program.stderr = std::mem::take(&mut program.stderr)
        .map_filename(|path| submit_dir.join(replace(&placeholder_map, &path)));
}
