const CACHE_LINE_SIZE: usize = 64;

cfg_if::cfg_if! {
    if #[cfg(feature = "loom_test")] {
        pub(crate) use loom::sync;
        pub(crate) struct CUnsafeCell<T>(loom::cell::UnsafeCell<T>);
        impl<T> CUnsafeCell<T> {
            #[inline(always)]
            pub(crate) fn new(data: T) -> Self { Self(loom::cell::UnsafeCell::new(data)) }
            #[inline(always)]
            pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
                self.0.with_mut(|ptr| f(ptr))
            }
        }
        pub(crate) use CUnsafeCell as UnsafeCell;
    } else {
        pub(crate) use std::sync;
        pub(crate) struct CUnsafeCell<T>(std::cell::UnsafeCell<T>);
        impl<T> CUnsafeCell<T> {
            #[inline(always)]
            pub(crate) fn new(data: T) -> Self { Self(std::cell::UnsafeCell::new(data)) }
            #[inline(always)]
            pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
                f(self.0.get())
            }
        }
        pub(crate) use CUnsafeCell as UnsafeCell;
    }
}

mod core; // NOT public → crate-internal only
mod utility;

pub mod mpmc;
pub mod mpsc;
pub mod spmc;
pub mod spsc;
