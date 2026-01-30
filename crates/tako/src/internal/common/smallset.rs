use crate::Set;
use std::hash::Hash;

/// A set optimized for size and case that there is for exactly one u64/u32 element.
pub enum SmallSet<T: PartialEq + Eq + Hash> {
    One(T),
    Empty,
    Many(Box<Set<T>>),
}

impl<T: PartialEq + Eq + Hash + Copy + Clone> SmallSet<T> {
    pub fn new_single_value(value: T) -> Self {
        SmallSet::One(value)
    }

    pub fn insert(&mut self, value: T) {
        match self {
            SmallSet::One(v) => {
                let mut set = Set::new();
                set.insert(*v);
                set.insert(value);
                *self = SmallSet::Many(Box::new(set));
            }
            SmallSet::Many(set) => {
                set.insert(value);
            }
            SmallSet::Empty => {
                *self = SmallSet::One(value);
            }
        };
    }

    /// This functions assumes that the set for SURE contains the value.
    /// So in release mode, and there is exactly one element, it does not check the equivalence
    /// Returns True if the set is empty after the removal
    pub fn remove(&mut self, value: T) {
        match self {
            SmallSet::One(v) => {
                if *v == value {
                    *self = SmallSet::Empty;
                }
            }
            SmallSet::Many(set) => {
                set.remove(&value);
            }
            SmallSet::Empty => {}
        }
    }

    pub fn size(&self) -> usize {
        match self {
            SmallSet::One(_) => 1,
            SmallSet::Empty => 0,
            SmallSet::Many(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            SmallSet::One(_) => false,
            SmallSet::Many(set) => set.is_empty(),
            SmallSet::Empty => true,
        }
    }
}
