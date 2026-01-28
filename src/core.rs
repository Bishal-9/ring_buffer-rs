use {anyhow::Result, std::sync::atomic::AtomicUsize, thiserror::Error};

#[derive(Copy, Debug)]
pub struct RingBuffer<T: Copy + Default, const N: usize> {
    data: [T; N],
}

#[derive(Debug, Error)]
pub enum ProducerConsumerError {
    #[error("Buffer is empty. No data to read.")]
    Empty,
}

impl<T: Copy + Default, const N: usize> RingBuffer<T, N> {
    pub fn new() -> Self {
        Self {
            data: [T::default(); N],
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct SingleProducer<T: Copy + Default, const N: usize> {
    buffer: RingBuffer<T, N>,
    pointer: usize,
}
impl<T: Copy + Default, const N: usize> SingleProducer<T, N> {
    pub fn new(buffer: RingBuffer<T, N>) -> Self {
        Self { buffer, pointer: 0 }
    }
}

#[derive(Debug)]
pub struct MultiProducer<T: Copy + Default, const N: usize> {
    buffer: RingBuffer<T, N>,
    pointer: AtomicUsize,
}
impl<T: Copy + Default, const N: usize> MultiProducer<T, N> {
    pub fn new(buffer: RingBuffer<T, N>) -> Self {
        Self {
            buffer,
            pointer: AtomicUsize::new(0),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct SingleConsumer<T: Copy + Default, const N: usize> {
    buffer: RingBuffer<T, N>,
    pointer: usize,
}
impl<T: Copy + Default, const N: usize> SingleConsumer<T, N> {
    pub fn new(buffer: RingBuffer<T, N>) -> Self {
        Self { buffer, pointer: 0 }
    }
}

#[derive(Debug)]
pub struct MultiConsumer<T: Copy + Default, const N: usize> {
    buffer: RingBuffer<T, N>,
    pointer: AtomicUsize,
}
impl<T: Copy + Default, const N: usize> MultiConsumer<T, N> {
    pub fn new(buffer: RingBuffer<T, N>) -> Self {
        Self {
            buffer,
            pointer: AtomicUsize::new(0),
        }
    }
}
