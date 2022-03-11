#[macro_export]
macro_rules! get_or_return {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            _ => return,
        }
    };
}

#[macro_export]
macro_rules! get_or_return_ok {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            _ => return Ok(()),
        }
    };
}
