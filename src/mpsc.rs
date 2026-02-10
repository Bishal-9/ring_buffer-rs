use {
    crate::core::{RingBuffer, MultipleProducer, SingleConsumer},
    std::sync::Arc,
};

pub struct MpscQueue<T: Copy + Default, const N: usize> {
    producer: MultipleProducer<T, N>,
    consumer: SingleConsumer<T, N>,
}
impl<T: Copy + Default, const N: usize> MpscQueue<T, N> {
    pub fn new() -> Self {
        let queue = Arc::new(RingBuffer::new());
        Self {
            producer: MultipleProducer::new(queue.clone()),
            consumer: SingleConsumer::new(queue),
        }
    }

    pub fn split(self) -> (MultipleProducer<T, N>, SingleConsumer<T, N>) {
        (self.producer, self.consumer)
    }
}
