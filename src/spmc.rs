use {
    crate::core::{RingBuffer, SingleProducer, MultipleConsumer},
    std::sync::Arc,
};

pub struct SpmcQueue<T: Copy + Default, const N: usize> {
    producer: SingleProducer<T, N>,
    consumer: MultipleConsumer<T, N>,
}
impl<T: Copy + Default, const N: usize> SpmcQueue<T, N> {
    pub fn new() -> Self {
        let queue = Arc::new(RingBuffer::new());
        Self {
            producer: SingleProducer::new(queue.clone()),
            consumer: MultipleConsumer::new(queue),
        }
    }

    pub fn split(self) -> (SingleProducer<T, N>, MultipleConsumer<T, N>) {
        (self.producer, self.consumer)
    }
}
