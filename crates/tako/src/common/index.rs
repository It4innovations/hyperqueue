use std::ops::{Deref, DerefMut, Index, IndexMut};

/// Vec that can only be indexed by the specified `Idx` type.
/// Useful in combination with index types created by `define_id_type`.
#[derive(Debug, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IndexVec<Idx, Value>(Vec<Value>, std::marker::PhantomData<Idx>);

impl<Idx: Into<usize>, Value: Copy> IndexVec<Idx, Value> {
    #[inline]
    pub fn filled(value: Value, count: usize) -> Self {
        Self(vec![value; count], Default::default())
    }
}

impl<Idx: Into<usize>, Value> Index<Idx> for IndexVec<Idx, Value> {
    type Output = Value;

    #[inline]
    fn index(&self, index: Idx) -> &Self::Output {
        self.0.index(index.into())
    }
}

impl<Idx: Into<usize>, Value> IndexMut<Idx> for IndexVec<Idx, Value> {
    #[inline]
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        self.0.index_mut(index.into())
    }
}

impl<Idx, Value> From<Vec<Value>> for IndexVec<Idx, Value> {
    #[inline]
    fn from(vec: Vec<Value>) -> Self {
        Self(vec, Default::default())
    }
}

impl<Idx, Value> Deref for IndexVec<Idx, Value> {
    type Target = Vec<Value>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Idx, Value> DerefMut for IndexVec<Idx, Value> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

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
        pub struct $name($type);

        impl $name {
            #[inline]
            pub fn new(value: $type) -> Self {
                Self(value)
            }

            #[inline]
            pub fn as_num(&self) -> $type {
                self.0 as $type
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

        impl ::std::convert::From<$name> for usize {
            #[inline]
            fn from(id: $name) -> Self {
                id.0 as usize
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

        impl $crate::common::index::AsIdVec<$name> for ::std::vec::Vec<$type> {
            #[inline]
            fn to_ids(self) -> ::std::vec::Vec<$name> {
                self.into_iter().map(|id| id.into()).collect()
            }
        }
    };
}

/// Converts a vector of integers into a vector of a corresponding newtype index
pub trait AsIdVec<IdType> {
    fn to_ids(self) -> Vec<IdType>;
}
