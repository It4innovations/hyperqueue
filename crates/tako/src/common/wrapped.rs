use std::cell::{Ref, RefCell, RefMut};
use std::clone::Clone;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::rc::Rc;

/// Wrapper struct containing a `Rc<RefCell<T>>`, implementing  several
/// helper functions and useful traits.
///
/// The traits implemented are `Clone` and `Debug` (if `T` is `Debug`).
/// The traits`PartialEq`, `Eq` and `Hash` are implemented on the *pointer value*.
/// This allows very fast collections of such wrapped structs when the contained
/// structs are all considered semantically distinct objects.
///
/// Note that you can add methods to the wrapper with
/// `impl WrappedRcRefCell<MyType> { fn foo(&self) {} }`
/// or even `type WrapType = WrappedRcRefCell<MyType>; impl WrapType { ... }`.
#[derive(Default, Debug)]
pub struct WrappedRcRefCell<T: ?Sized> {
    inner: Rc<RefCell<T>>,
}

impl<T> WrappedRcRefCell<T> {
    /// Create a new wrapped instance. This is not called `new` so that you may implement
    /// your own function `new`.
    #[inline]
    pub fn wrap(t: T) -> Self {
        WrappedRcRefCell {
            inner: Rc::new(RefCell::new(t)),
        }
    }
}

impl<T: ?Sized> WrappedRcRefCell<T> {
    #[inline]
    pub fn new_wrapped(inner: Rc<RefCell<T>>) -> Self {
        WrappedRcRefCell { inner }
    }

    /// Return a immutable reference to contents. Panics whenever `RefCell::borrow()` would.
    #[inline]
    #[track_caller]
    pub fn get(&self) -> Ref<T> {
        self.inner.deref().borrow()
    }

    /// Return a mutable reference to contents. Panics whenever `RefCell::borrow_mut()` would.
    #[inline]
    #[track_caller]
    pub fn get_mut(&self) -> RefMut<T> {
        self.inner.deref().borrow_mut()
    }

    // Return the number of strong references to the contained Rc
    #[inline]
    pub fn get_num_refs(&self) -> usize {
        Rc::strong_count(&self.inner)
    }
}

impl<T: ?Sized> Clone for WrappedRcRefCell<T> {
    #[inline]
    fn clone(&self) -> Self {
        WrappedRcRefCell {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ?Sized> Hash for WrappedRcRefCell<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = &*self.inner as *const RefCell<T>;
        ptr.hash(state);
    }
}

impl<T: ?Sized> PartialEq for WrappedRcRefCell<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<T: ?Sized> Eq for WrappedRcRefCell<T> {}

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
