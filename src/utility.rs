use {
    crate::CACHE_LINE_SIZE,
    std::sync::atomic::{AtomicUsize, Ordering},
};

#[repr(align(64))]
pub(crate) struct PaddedAtomicUsize {
    value: AtomicUsize,
    _padding: [u8; CACHE_LINE_SIZE - size_of::<AtomicUsize>()],
}
impl PaddedAtomicUsize {
    pub(crate) fn new() -> Self {
        Self {
            value: AtomicUsize::new(0),
            _padding: [0; CACHE_LINE_SIZE - size_of::<AtomicUsize>()],
        }
    }

    #[inline(always)]
    pub(crate) fn load(&self, memory_ordering: Ordering) -> usize {
        self.value.load(memory_ordering)
    }

    #[inline(always)]
    pub(crate) fn store(&self, value: usize, memory_ordering: Ordering) {
        self.value.store(value, memory_ordering)
    }

    #[inline(always)]
    pub(crate) fn compare_exchange_weak(
        &self,
        current: usize,
        new: usize,
        success_memory_ordering: Ordering,
        failure_memory_ordering: Ordering,
    ) -> Result<usize, usize> {
        self.value.compare_exchange_weak(
            current,
            new,
            success_memory_ordering,
            failure_memory_ordering,
        )
    }
}
