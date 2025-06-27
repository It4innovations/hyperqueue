#[cfg(test)]
pub mod env;
#[cfg(test)]
pub mod resources;
#[cfg(test)]
pub mod schedule;
#[cfg(test)]
pub mod task;
#[cfg(test)]
pub mod worker;
#[cfg(test)]
pub mod workflows;

pub mod shared;

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
            let formatted = format!("{error:?}");
            if !formatted.contains(msg) {
                panic!("Did not find `{msg}` in `{formatted}`");
            }
        }
    }
}
