#![allow(unused)]

use {
    crate::utility::PaddedAtomicUsize,
    anyhow::Result,
    std::{
        cell::UnsafeCell,
        ptr::copy_nonoverlapping,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    },
    thiserror::Error,
};

pub(crate) struct RingBuffer<T: Copy + Default, const N: usize> {
    data: UnsafeCell<[T; N]>,
    capacity: usize,

    write_pointer: PaddedAtomicUsize,
    read_pointer: PaddedAtomicUsize,
}
unsafe impl<T: Copy + Default + Send, const N: usize> Send for RingBuffer<T, N> {}
unsafe impl<T: Copy + Default + Send, const N: usize> Sync for RingBuffer<T, N> {}

#[derive(Debug, Error)]
pub(crate) enum ProducerConsumerError {
    #[error("Buffer is empty. No data to write.")]
    EmptyData,
}
impl<T: Copy + Default, const N: usize> RingBuffer<T, N> {
    pub fn new() -> Self {
        Self {
            data: UnsafeCell::new([T::default(); N]),
            capacity: N,

            write_pointer: PaddedAtomicUsize::new(),
            read_pointer: PaddedAtomicUsize::new(),
        }
    }

    #[inline(always)]
    fn read_counter(&self) -> usize {
        self.read_pointer.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn write_counter(&self) -> usize {
        self.write_pointer.load(Ordering::Acquire)
    }

    #[inline]
    fn available_space(&self) -> usize {
        let read_position = self.read_counter();
        let write_position = self.write_counter();

        let pointer_difference = write_position.wrapping_sub(read_position);
        let mask = ((write_position >= read_position) as usize).wrapping_sub(1); // 0 if true, usize::MAX if false

        /*
         * If write_position >= read_position: return seld.data.len() - pointer_difference
         * If write_position < read_position: return -pointer_difference (i.e., read_position - write_position)
         */
        (self.capacity.wrapping_sub(pointer_difference) & !mask) | (pointer_difference & mask)
    }

    fn write_to_ring(&self, value: &[T], start_position: usize) {
        let value_length = value.len();
        let start_index = start_position & (self.capacity - 1);
        let data = unsafe { &mut *self.data.get() };

        if (start_index + value_length) <= self.capacity {
            // Value fits without wrapping
            unsafe {
                copy_nonoverlapping(
                    value.as_ptr(),
                    data.as_mut_ptr().add(start_index),
                    value_length,
                )
            }
        } else {
            // Value wraps around the ring buffer
            let first_part_length = self.capacity - start_index;
            let second_part_length = value_length - first_part_length;

            unsafe {
                copy_nonoverlapping(
                    value.as_ptr(),
                    data.as_mut_ptr().add(start_index),
                    first_part_length,
                );
                copy_nonoverlapping(
                    value.as_ptr(),
                    data.as_mut_ptr().add(start_index),
                    second_part_length,
                );
            }
        }
    }

    fn write(&self, value: &[T]) -> Option<usize> {
        let value_length = value.len();
        if value_length == 0 {
            return Some(self.write_counter());
        }

        if value_length > self.capacity {
            // Data is too large
            return None;
        }

        loop {
            let currnet_write_pointer = self.write_counter();
            let available_space = self.available_space();

            if available_space < value_length {
                // Not enough space
                return None;
            }

            let new_write_pointer = currnet_write_pointer + value_length;

            match self.write_pointer.compare_exchange_weak(
                currnet_write_pointer,
                new_write_pointer,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Successfully reserved space, now write the data
                    self.write_to_ring(value, currnet_write_pointer);

                    // // Update the read counter to make data visible to consumers
                    // self.read_pointer
                    //     .store(new_write_pointer, Ordering::Release);

                    return Some(new_write_pointer);
                }
                Err(_) => {
                    // Another thread updated the counter, retry
                    continue;
                }
            }
        }
    }

    #[inline]
    fn available_data(&self, local_read_counter: usize) -> usize {
        let read_position = self.read_counter();
        let pointer_difference = read_position.wrapping_sub(local_read_counter);
        let mask = ((read_position > local_read_counter) as usize).wrapping_sub(1);
        pointer_difference & !mask
    }

    fn read_from_ring(&self, value: &mut [T], start_position: usize, bytes_to_read: usize) {
        let start_index = start_position & (self.capacity - 1);
        let data = unsafe { &mut *self.data.get() };

        if (start_index + bytes_to_read) <= self.capacity {
            unsafe {
                copy_nonoverlapping(
                    data.as_mut_ptr().add(start_index),
                    value.as_mut_ptr(),
                    bytes_to_read,
                );
            }
        } else {
            let first_part_length = self.capacity - start_index;
            let second_part_length = bytes_to_read - first_part_length;

            unsafe {
                copy_nonoverlapping(
                    data.as_mut_ptr().add(start_index),
                    value.as_mut_ptr(),
                    first_part_length,
                );
                copy_nonoverlapping(data.as_mut_ptr(), value.as_mut_ptr(), second_part_length);
            }
        }
    }

    fn read(&self, local_read_counter: &mut usize, value: &mut [T]) -> usize {
        // Calculator available data from our local position
        let available_from_local = self.available_data(*local_read_counter);

        if available_from_local == 0 {
            return 0; // No new data available
        }

        let bytes_to_read = available_from_local.min(value.len());

        // Read the data from the ring buffer
        self.read_from_ring(value, *local_read_counter, bytes_to_read);

        // Update local read counter
        *local_read_counter += bytes_to_read;

        bytes_to_read
    }
}

pub(crate) struct SingleProducer<T: Copy + Default, const N: usize> {
    buffer: Arc<RingBuffer<T, N>>,
    pointer: usize,
}
impl<T: Copy + Default, const N: usize> SingleProducer<T, N> {
    pub(crate) fn new(buffer: Arc<RingBuffer<T, N>>) -> Self {
        Self { buffer, pointer: 0 }
    }
    pub(crate) fn write(&mut self, value: &[T]) -> Option<usize> {
        self.buffer.write(value)
    }
    pub(crate) fn position(&self) -> usize {
        self.buffer.write_counter()
    }
    pub(crate) fn available_space(&self) -> usize {
        self.buffer.available_space()
    }
}

pub struct MultiProducer<T: Copy + Default, const N: usize> {
    buffer: Arc<RingBuffer<T, N>>,
    pointer: AtomicUsize,
}
impl<T: Copy + Default, const N: usize> MultiProducer<T, N> {
    pub fn new(buffer: Arc<RingBuffer<T, N>>) -> Self {
        Self {
            buffer,
            pointer: AtomicUsize::new(0),
        }
    }
}

pub(crate) struct SingleConsumer<T: Copy + Default, const N: usize> {
    buffer: Arc<RingBuffer<T, N>>,
    pointer: usize,
}
impl<T: Copy + Default, const N: usize> SingleConsumer<T, N> {
    pub(crate) fn new(buffer: Arc<RingBuffer<T, N>>) -> Self {
        Self { buffer, pointer: 0 }
    }
    pub(crate) fn read(&mut self, value: &mut [T]) -> usize {
        self.buffer.read(&mut self.pointer, value)
    }
    pub(crate) fn position(&self) -> usize {
        self.buffer.read_counter()
    }
    pub(crate) fn available(&self) -> usize {
        self.buffer.available_data(self.buffer.read_counter())
    }
}

pub struct MultiConsumer<T: Copy + Default, const N: usize> {
    buffer: Arc<RingBuffer<T, N>>,
    pointer: AtomicUsize,
}
impl<T: Copy + Default, const N: usize> MultiConsumer<T, N> {
    pub fn new(buffer: Arc<RingBuffer<T, N>>) -> Self {
        Self {
            buffer,
            pointer: AtomicUsize::new(0),
        }
    }
}
