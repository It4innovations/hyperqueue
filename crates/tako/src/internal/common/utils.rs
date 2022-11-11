use std::fmt::Display;

/// Checks at compile-time that the given type $ty has the corresponding $size.
///
/// It can be used to prevent performance-critical data structures to grow in size unexpectedly.
#[macro_export]
macro_rules! static_assert_size {
    ($ty:ty, $size:expr) => {
        #[cfg(target_arch = "x86_x64")]
        const _: [(); $size] = [(); ::std::mem::size_of::<$ty>()];
    };
}

pub use static_assert_size;

pub fn format_comma_delimited<I: IntoIterator<Item = T>, T: Display>(iter: I) -> String {
    iter.into_iter()
        .map(|item| item.to_string())
        .collect::<Vec<_>>()
        .join(",")
}
