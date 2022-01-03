/// Checks at compile-time that the given type $ty has the corresponding $size.
///
/// It can be used to prevent performance-critical data structures to grow in size unexpectedly.
#[macro_export]
macro_rules! static_assert_size {
    ($ty:ty, $size:expr) => {
        const _: [(); $size] = [(); ::std::mem::size_of::<$ty>()];
    };
}

pub use static_assert_size;
