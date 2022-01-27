use std::borrow::Cow;

/// Return the input string with an added "s" at the end if `count` is larger than one and non-zero.
pub fn pluralize(value: &str, count: usize) -> Cow<str> {
    if count == 1 {
        Cow::Borrowed(value)
    } else {
        Cow::Owned(format!("{}s", value))
    }
}

/// Select `single` variant if `count` is one or `other` variant otherwise.
pub fn select_plural<'a>(single: &'a str, other: &'a str, count: usize) -> Cow<'a, str> {
    if count == 1 {
        Cow::Borrowed(single)
    } else {
        Cow::Borrowed(other)
    }
}
