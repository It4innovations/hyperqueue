pub mod env;
pub mod resources;
pub mod schedule;
pub mod task;
pub mod workflows;

pub fn sorted_vec<T: Ord>(mut vec: Vec<T>) -> Vec<T> {
    vec.sort();
    vec
}

#[allow(unused)]
#[cfg(test)]
pub fn enable_test_logging() {
    env_logger::builder().is_test(false).init()
}

#[cfg(test)]
pub fn expect_error_message<T>(result: anyhow::Result<T>, msg: &str) {
    match result {
        Ok(_) => panic!("Expected error, got Ok"),
        Err(error) => {
            let formatted = format!("{:?}", error);
            if !formatted.contains(msg) {
                panic!("Did not find `{}` in `{}`", msg, formatted);
            }
        }
    }
}
