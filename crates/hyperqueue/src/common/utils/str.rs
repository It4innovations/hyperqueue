use std::borrow::Cow;

/// Return the input string with an added "s" at the end if `count` is larger than one and non-zero.
pub fn pluralize(value: &str, count: usize) -> Cow<'_, str> {
    if count == 1 {
        Cow::Borrowed(value)
    } else {
        Cow::Owned(format!("{value}s"))
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

/// Truncates the middle of a string so that it's total length doesn't exceed `length`.
/// The returned string will never exceed `length`.
/// `length` has to be at least five, otherwise there wouldn't be space for `...`.
pub fn truncate_middle(value: &str, length: usize) -> Cow<'_, str> {
    assert!(length >= 5);

    if value.len() <= length {
        value.into()
    } else {
        let length = length - 3; // space for ...
        let half = length as f64 / 2.0;
        let start = half.ceil() as usize;
        let end = half.floor() as usize;
        format!("{}...{}", &value[..start], &value[value.len() - end..]).into()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::utils::str::truncate_middle;
    use std::borrow::Cow;

    #[test]
    fn test_truncate_middle_empty() {
        assert_eq!(truncate_middle("", 5), Cow::from(""));
    }

    #[test]
    fn test_truncate_middle_length_1() {
        assert_eq!(truncate_middle("a", 5), Cow::from("a"));
    }

    #[test]
    fn test_truncate_middle_length_2() {
        assert_eq!(truncate_middle("a", 5), Cow::from("a"));
    }

    #[test]
    fn test_truncate_middle_length_5() {
        assert_eq!(truncate_middle("abcde", 5), Cow::from("abcde"));
    }

    #[test]
    fn test_truncate_middle_length_6() {
        assert_eq!(truncate_middle("abcdef", 5), Cow::from("a...f"));
    }
}
