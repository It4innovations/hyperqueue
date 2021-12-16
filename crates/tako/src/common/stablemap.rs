use crate::common::Map;
use crate::define_id_type;
use std::borrow::Borrow;
use std::hash::Hash;

#[derive(Debug)]
struct StableVec<V> {
    items: Vec<V>,
}

impl<V> Default for StableVec<V> {
    #[inline]
    fn default() -> Self {
        Self {
            items: Default::default(),
        }
    }
}

define_id_type!(StableVecIndex, u64);

impl<V> StableVec<V> {
    #[inline]
    fn get(&self, index: StableVecIndex) -> Option<&V> {
        let index: usize = index.into();
        self.items.get(index)
    }

    #[inline]
    fn get_mut(&mut self, index: StableVecIndex) -> Option<&mut V> {
        let index: usize = index.into();
        self.items.get_mut(index)
    }

    #[inline]
    fn insert(&mut self, value: V) -> StableVecIndex {
        let index = StableVecIndex::new(self.items.len() as u64);
        self.items.push(value);
        index
    }
}

#[derive(Debug)]
pub struct StableMap<K, V> {
    map: Map<K, StableVecIndex>,
    storage: StableVec<V>,
}

impl<K, V> Default for StableMap<K, V> {
    #[inline]
    fn default() -> Self {
        Self {
            map: Default::default(),
            storage: Default::default(),
        }
    }
}

impl<K, V> StableMap<K, V>
where
    K: Hash + Eq,
    V: ExtractKey<K>,
{
    #[inline]
    pub fn find<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get(key).and_then(|&index| self.storage.get(index))
    }

    #[inline]
    pub fn find_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map
            .get(key)
            .and_then(|&index| self.storage.get_mut(index))
    }

    #[inline]
    pub fn get<Q: ?Sized>(&self, key: &Q) -> &V
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.find(key).expect("Key not found")
    }

    #[inline]
    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> &mut V
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.find_mut(key).expect("Key not found")
    }

    #[inline]
    pub fn insert(&mut self, value: V) {
        let key = value.extract_key();
        let index = self.storage.insert(value);
        assert!(self.map.insert(key, index).is_none());
    }

    #[inline]
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.remove(key).and_then(|index| {
            let idx: usize = index.into();
            let length = self.storage.items.len();

            // If we got here, the index must be valid
            if idx == length - 1 {
                self.storage.items.pop()
            } else {
                // Remember the key of the last element
                let last_key = self.storage.items.last().unwrap().extract_key();
                // Swap the removed element with the last element
                let removed = self.storage.items.swap_remove(idx);
                // Redirect the key of the swapped (previously last) element
                self.map.insert(last_key, index);
                Some(removed)
            }
        })
    }

    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.map.keys()
    }

    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.storage.items.iter()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// This trait is needed to provide fast key inversion (value -> key) without auxiliary storage.
pub trait ExtractKey<K> {
    fn extract_key(&self) -> K;
}

#[cfg(test)]
mod tests {
    use crate::common::stablemap::{ExtractKey, StableMap, StableVec};

    #[test]
    fn vec_insert_empty() {
        let mut v = StableVec::default();
        let index = v.insert(1);
        assert_eq!(index.as_num(), 0);
    }

    #[test]
    fn vec_get_missing_index() {
        let v: StableVec<u32> = Default::default();
        assert!(v.get(0.into()).is_none());
    }

    #[test]
    fn vec_insert_and_get() {
        let mut v: StableVec<u32> = Default::default();
        let i1 = v.insert(1);
        let i2 = v.insert(2);
        let i3 = v.insert(3);
        assert_eq!(v.get(i1), Some(&1));
        assert_eq!(v.get(i2), Some(&2));
        assert_eq!(v.get(i3), Some(&3));
    }

    #[test]
    fn map_insert_get() {
        let mut v: StableMap<u32, u64> = Default::default();
        v.insert(1);

        assert_eq!(v.get(&1), &1);
    }

    #[test]
    fn map_remove_only() {
        let mut v: StableMap<u32, u64> = Default::default();
        v.insert(1);

        assert_eq!(v.remove(&1), Some(1));
        assert_eq!(v.find(&1), None);
    }

    #[test]
    fn map_remove_in_middle() {
        let mut v: StableMap<u32, u64> = Default::default();
        v.insert(1);
        v.insert(2);
        v.insert(3);

        assert_eq!(v.remove(&2), Some(2));
        assert_eq!(v.find(&1), Some(&1));
        assert_eq!(v.find(&2), None);
        assert_eq!(v.find(&3), Some(&3));
    }

    #[test]
    fn map_remove_last() {
        let mut v: StableMap<u32, u64> = Default::default();
        v.insert(1);
        v.insert(2);

        assert_eq!(v.remove(&2), Some(2));
        assert_eq!(v.find(&2), None);
        assert_eq!(v.find(&1), Some(&1));
    }

    #[test]
    fn map_remove_missing() {
        let mut v: StableMap<u32, u64> = Default::default();
        v.insert(1);

        assert_eq!(v.remove(&2), None);
        assert_eq!(v.find(&1), Some(&1));
    }

    #[test]
    fn map_insert_after_remove() {
        let mut v: StableMap<u32, u64> = Default::default();
        v.insert(1);
        v.insert(2);
        v.insert(3);

        assert_eq!(v.remove(&2), Some(2));
        v.insert(2);
        assert_eq!(v.find(&1), Some(&1));
        assert_eq!(v.find(&2), Some(&2));
        assert_eq!(v.find(&3), Some(&3));
    }

    impl ExtractKey<u32> for u64 {
        #[inline]
        fn extract_key(&self) -> u32 {
            *self as u32
        }
    }
}
