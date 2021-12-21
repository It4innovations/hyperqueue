use fxhash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

// Map
type InnerMap<K, V> = hashbrown::HashMap<K, V, FxBuildHasher>;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Map<K: Eq + Hash, V>(InnerMap<K, V>);

impl<K: Eq + Hash, V> Map<K, V> {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Map(InnerMap::with_capacity_and_hasher(
            capacity,
            FxBuildHasher::default(),
        ))
    }
}

impl<K: Eq + Hash, V> Default for Map<K, V> {
    #[inline]
    fn default() -> Self {
        Self(InnerMap::default())
    }
}

impl<K: Eq + Hash, V> Deref for Map<K, V> {
    type Target = InnerMap<K, V>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Eq + Hash, V> DerefMut for Map<K, V> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K: Eq + Hash, V> FromIterator<(K, V)> for Map<K, V>
where
    K: Eq + Hash,
{
    #[inline]
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(InnerMap::from_iter(iter))
    }
}

impl<K: Eq + Hash, V> IntoIterator for Map<K, V> {
    type Item = (K, V);
    type IntoIter = <InnerMap<K, V> as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K: Eq + Hash, V> IntoIterator for &'a Map<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = <&'a InnerMap<K, V> as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

// Set
type InnerSet<T> = hashbrown::HashSet<T, FxBuildHasher>;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Set<T: Eq + Hash>(InnerSet<T>);

impl<T: Eq + Hash> Set<T> {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Set(InnerSet::with_capacity_and_hasher(
            capacity,
            FxBuildHasher::default(),
        ))
    }
}

impl<T: Eq + Hash> Default for Set<T> {
    #[inline]
    fn default() -> Self {
        Self(InnerSet::default())
    }
}

impl<T: Eq + Hash> Deref for Set<T> {
    type Target = InnerSet<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Eq + Hash> DerefMut for Set<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Eq + Hash> FromIterator<T> for Set<T>
where
    T: Eq + Hash,
{
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(InnerSet::from_iter(iter))
    }
}

impl<T: Eq + Hash> IntoIterator for Set<T> {
    type Item = T;
    type IntoIter = <InnerSet<T> as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T: Eq + Hash> IntoIterator for &'a Set<T> {
    type Item = &'a T;
    type IntoIter = <&'a InnerSet<T> as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
