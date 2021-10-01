use std::borrow::Cow;

/// Return the input string with an added "s" at the end if `count` is larger than one.
pub fn pluralize(value: &str, count: usize) -> Cow<str> {
    if count > 1 {
        Cow::Owned(format!("{}s", value))
    } else {
        Cow::Borrowed(value)
    }
}
