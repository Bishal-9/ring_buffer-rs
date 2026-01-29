#![allow(unused)]

use {
    crate::core::{RingBuffer, SingleConsumer, SingleProducer},
    std::sync::Arc,
};

pub struct SpscQueue<T: Copy + Default, const N: usize> {
    buffer: Arc<RingBuffer<T, N>>,
    producer: SingleProducer<T, N>,
    consumer: SingleConsumer<T, N>,
}
impl<T: Copy + Default, const N: usize> SpscQueue<T, N> {
    pub fn new() -> Self {
        let queue = Arc::new(RingBuffer::new());
        Self {
            buffer: queue.clone(),
            producer: SingleProducer::new(queue.clone()),
            consumer: SingleConsumer::new(queue.clone()),
        }
    }

    pub fn split(&self) -> (&SingleProducer<T, N>, &SingleConsumer<T, N>) {
        let producer = &self.producer;
        let consumer = &self.consumer;
        (producer, consumer)
    }
}
