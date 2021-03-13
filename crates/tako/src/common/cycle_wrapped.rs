use std::ops::Deref;

use crate::common::WrappedRcRefCell;

pub trait HasCycle {
    fn clear_cycle(&mut self);
}

#[derive(Default, Debug)]
pub struct CycleOwner<T: HasCycle>(WrappedRcRefCell<T>);

impl<T: HasCycle> CycleOwner<T> {
    #[inline]
    pub fn wrap(value: T) -> Self {
        CycleOwner(WrappedRcRefCell::wrap(value))
    }

    #[inline]
    pub fn clone(&self) -> WrappedRcRefCell<T> {
        self.0.clone()
    }
}

impl<T: HasCycle> Deref for CycleOwner<T> {
    type Target = WrappedRcRefCell<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: HasCycle> Drop for CycleOwner<T> {
    #[inline]
    fn drop(&mut self) {
        self.0.get_mut().clear_cycle();
    }
}
