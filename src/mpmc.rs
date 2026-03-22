use {
    crate::{core::{MultipleConsumer, MultipleProducer, RingBuffer}, sync::Arc},
};

pub struct MpmcQueue<T: Copy + Default, const N: usize> {
    producer: MultipleProducer<T, N>,
    consumer: MultipleConsumer<T, N>,
}
impl<T: Copy + Default, const N: usize> Default for MpmcQueue<T, N> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: Copy + Default, const N: usize> MpmcQueue<T, N> {
    pub fn new() -> Self {
        let queue = Arc::new(RingBuffer::<T, N>::new());
        Self {
            producer: MultipleProducer::new(queue.clone()),
            consumer: MultipleConsumer::new(queue),
        }
    }

    pub fn split(self) -> (MultipleProducer<T, N>, MultipleConsumer<T, N>) {
        (self.producer, self.consumer)
    }
}
