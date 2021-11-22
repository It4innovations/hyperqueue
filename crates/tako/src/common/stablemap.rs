use crate::common::Map;
use crate::define_id_type;
use std::borrow::Borrow;
use std::hash::Hash;

#[derive(Debug)]
struct StableVec<V> {
    items: Vec<Option<V>>,
    free_indices: Vec<StableVecIndex>,
}

impl<V> Default for StableVec<V> {
    #[inline]
    fn default() -> Self {
        Self {
            items: Default::default(),
            free_indices: Default::default(),
        }
    }
}

define_id_type!(StableVecIndex, u64);

impl<V> StableVec<V> {
    #[inline]
    fn get(&self, index: StableVecIndex) -> Option<&V> {
        self.items
            .get(index.as_num() as usize)
            .and_then(|v| v.as_ref())
    }

    #[inline]
    fn get_mut(&mut self, index: StableVecIndex) -> Option<&mut V> {
        self.items
            .get_mut(index.as_num() as usize)
            .and_then(|v| v.as_mut())
    }

    #[inline]
    fn insert(&mut self, value: V) -> StableVecIndex {
        match self.free_indices.pop() {
            Some(index) => {
                debug_assert!(self.items[index.as_num() as usize].is_none());
                self.items[index.as_num() as usize] = Some(value);
                index
            }
            None => {
                self.items.push(Some(value));
                StableVecIndex::new((self.items.len() - 1) as u64)
            }
        }
    }

    #[inline]
    fn remove(&mut self, index: StableVecIndex) -> Option<V> {
        match self.items.get_mut(index.as_num() as usize) {
            Some(value) => {
                debug_assert!(value.is_some());

                let value = std::mem::replace(value, None);
                self.free_indices.push(index);
                value
            }
            None => panic!("Attempted to remove invalid index {}", index),
        }
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
    pub fn insert(&mut self, key: K, value: V) {
        let index = self.storage.insert(value);
        self.map.insert(key, index);
    }

    #[inline]
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map
            .remove(key)
            .and_then(|index| self.storage.remove(index))
    }
}

#[cfg(test)]
mod tests {
    use crate::common::stablemap::{StableMap, StableVec};

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
    #[should_panic]
    fn vec_remove_missing_should_panic() {
        let mut v: StableVec<u32> = Default::default();
        v.remove(0.into());
    }

    #[test]
    fn vec_remove_returns_existing() {
        let mut v: StableVec<u32> = Default::default();
        let i1 = v.insert(1);

        assert_eq!(v.remove(i1), Some(1));
    }

    #[test]
    fn vec_remove_get() {
        let mut v: StableVec<u32> = Default::default();
        let i1 = v.insert(1);
        let i2 = v.insert(2);
        let i3 = v.insert(3);

        v.remove(i2);
        assert_eq!(v.get(i1), Some(&1));
        assert_eq!(v.get(i3), Some(&3));
    }

    #[test]
    fn vec_insert_after_remove() {
        let mut v: StableVec<u32> = Default::default();
        v.insert(1);
        let i2 = v.insert(2);
        v.insert(3);

        v.remove(i2);

        let i4 = v.insert(4);
        assert_eq!(i4, i2);

        assert_eq!(v.items.len(), 3);
    }

    #[test]
    fn vec_insert_after_multiple_remove() {
        let mut v: StableVec<u32> = Default::default();
        v.insert(1);
        let i2 = v.insert(2);
        let i3 = v.insert(3);

        v.remove(i2);
        v.remove(i3);

        let i4 = v.insert(4);
        assert_eq!(i4, i3);

        assert_eq!(v.items.len(), 3);
    }

    #[test]
    fn map_insert_get() {
        let mut v: StableMap<u32, u64> = Default::default();
        v.insert(1, 1);

        assert_eq!(v.get(&1), &1);
    }

    #[test]
    fn map_remove_get() {
        let mut v: StableMap<u32, u64> = Default::default();
        v.insert(1, 1);

        assert_eq!(v.remove(&1), Some(1));
        assert_eq!(v.find(&1), None);
    }
}
