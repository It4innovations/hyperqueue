/// Create a newtype that will contain an index represented by an integer.
#[macro_export]
macro_rules! define_id_type {
    ($name: ident, $type: ident) => {
        #[derive(
            ::std::marker::Copy,
            ::std::clone::Clone,
            ::std::default::Default,
            ::std::fmt::Debug,
            ::std::hash::Hash,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::std::cmp::Ord,
            ::std::cmp::PartialOrd,
            ::std::cmp::Eq,
            ::std::cmp::PartialEq,
        )]
        #[repr(transparent)]
        pub struct $name($type);

        impl $name {
            #[inline]
            pub fn new(value: $type) -> Self {
                Self(value)
            }

            #[inline]
            pub fn as_u32(&self) -> u32 {
                self.0 as u32
            }

            #[inline]
            pub fn as_u64(&self) -> u64 {
                self.0 as u64
            }
        }

        impl ::std::convert::From<$type> for $name {
            #[inline]
            fn from(value: $type) -> Self {
                Self::new(value)
            }
        }

        impl ::std::convert::From<$name> for $type {
            #[inline]
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl ::std::fmt::Display for $name {
            #[inline]
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = ::std::num::ParseIntError;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                Ok($name(s.parse::<$type>()?))
            }
        }
    };
}

/// Create a newtype that will contain a type wrapped inside [`WrappedRcRefCell`].
#[macro_export]
macro_rules! define_wrapped_type {
    ($name: ident, $type: ty $(, $visibility: vis)?) => {
        #[derive(::std::clone::Clone)]
        #[repr(transparent)]
        $($visibility)* struct $name($crate::common::WrappedRcRefCell<$type>);

        impl ::std::ops::Deref for $name {
            type Target = $crate::common::WrappedRcRefCell<$type>;

            #[inline]
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

pub use {define_id_type, define_wrapped_type};
