#![allow(unused)]

use std::fmt::Debug;
use {
    crate::core::{RingBuffer, SingleConsumer, SingleProducer},
    std::sync::Arc,
};

pub struct SpscQueue<T: Copy + Default + Debug, const N: usize> {
    producer: SingleProducer<T, N>,
    consumer: SingleConsumer<T, N>,
}
impl<T: Copy + Default + Debug, const N: usize> SpscQueue<T, N> {
    pub fn new() -> Self {
        let queue = Arc::new(RingBuffer::new());
        Self {
            producer: SingleProducer::new(queue.clone()),
            consumer: SingleConsumer::new(queue),
        }
    }

    pub fn split(self) -> (SingleProducer<T, N>, SingleConsumer<T, N>) {
        (self.producer, self.consumer)
    }
}
