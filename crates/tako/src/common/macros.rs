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
            pub fn new(value: u32) -> Self {
                Self(value)
            }

            pub fn as_u32(&self) -> u32 {
                self.0
            }
        }

        impl ::std::convert::From<u32> for $name {
            fn from(value: u32) -> Self {
                Self::new(value)
            }
        }

        impl ::std::convert::From<$name> for u32 {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}
pub use define_id_type;
