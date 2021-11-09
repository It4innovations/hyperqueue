/// Create a newtype that will contain an index represented by an integer.
#[macro_export]
macro_rules! define_id_type {
    ($name: ident) => {
        #[derive(
            ::std::marker::Copy,
            ::std::clone::Clone,
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
        pub struct $name(u32);

        impl $name {
            #[inline]
            pub fn new(value: u32) -> Self {
                Self(value)
            }

            #[inline]
            pub fn as_u32(&self) -> u32 {
                self.0
            }
        }

        impl ::std::convert::From<u32> for $name {
            #[inline]
            fn from(value: u32) -> Self {
                Self::new(value)
            }
        }

        impl ::std::convert::From<$name> for u32 {
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
    };
}

/// Create a newtype that will contain a type wrapped inside [`WrappedRcRefCell`].
#[macro_export]
macro_rules! define_wrapped_type {
    ($name: ident, $type: ty $(, $visibility: vis)?) => {
        $($visibility)* struct $name($crate::common::WrappedRcRefCell<$type>);

        impl ::std::ops::Deref for $name {
            type Target = $crate::common::WrappedRcRefCell<$type>;

            #[inline]
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl ::std::clone::Clone for $name {
            #[inline]
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }
    };
}

pub use {define_id_type, define_wrapped_type};
