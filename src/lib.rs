#[derive(Debug)]
struct RingBuffer {
    data: Vec<u8>,
    size: usize,
    is_full: bool,
    read_pointer: usize,
    write_pointer: usize,
}

impl RingBuffer {
    pub fn new(size: usize) -> RingBuffer {
        RingBuffer {
            data: vec![0; size],
            size,
            is_full: false,
            read_pointer: 0,
            write_pointer: 0,
        }
    }
    pub fn read(&mut self) -> Option<Vec<u8>> {
        if self.write_pointer == self.read_pointer && !self.is_full {
            // No data to read
            return None;
        }

        let mut _read_data = Vec::new();

        if self.write_pointer < self.read_pointer {
            // Too many data got accumulated
            _read_data.extend_from_slice(&self.data[self.read_pointer..self.size]);
            _read_data.extend_from_slice(&self.data[0..self.write_pointer]);

            // Update the Read Pointer
            self.read_pointer = self.write_pointer;

            return Some(_read_data);
        }

        if self.read_pointer == self.write_pointer {
            let min = self.read_pointer.min(self.size);
            _read_data.extend_from_slice(&self.data[min..]);
            _read_data.extend_from_slice(&self.data[..min]);
        } else {
            _read_data.extend_from_slice(&self.data[self.read_pointer..self.write_pointer]);
            self.read_pointer = self.write_pointer;
        }

        self.is_full = false;

        Some(_read_data)
    }
    pub fn write(&mut self, mut data_to_write: Vec<u8>) -> Result<(), &'static str> {
        // Check whether the data_to_write is empty or not
        if data_to_write.is_empty() {
            return Err("Please, provide some data.");
        }

        let mut length = data_to_write.len();
        // Update the Write Pointer
        if self.write_pointer + length > self.size {
            let mut min = length.min(self.size - self.write_pointer);
            while length > 0 && min > 0 {
                self.data[self.write_pointer..(min + self.write_pointer)]
                    .copy_from_slice(&data_to_write[..min]);
                data_to_write = data_to_write[min..].to_vec();
                length = data_to_write.len();

                if min + self.write_pointer == self.size {
                    self.write_pointer = 0;
                } else {
                    self.write_pointer = min;
                }

                min = length.min(self.size - self.write_pointer);
            }
        } else {
            self.data[..length].copy_from_slice(&data_to_write[self.write_pointer..]);

            if length == self.size {
                self.write_pointer = 0;
            } else {
                self.write_pointer = self.write_pointer + length;
            }
        }
        self.is_full = true;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_buffer_read() {
        let mut buffer = RingBuffer::new(5);
        assert_eq!(buffer.read(), None);
    }

    #[test]
    fn writing_on_buffer() {
        let mut buffer = RingBuffer::new(10);
        assert_eq!(buffer.write(vec![2, 4, 6]), Ok(()));
    }

    #[test]
    fn reading_data_from_buffer() {
        let mut buffer = RingBuffer::new(10);

        buffer.write(vec![2, 4, 6, 8, 10]).unwrap();

        assert_eq!(buffer.read(), Some(vec![2, 4, 6, 8, 10]));
    }

    #[test]
    fn write_then_read() {
        let mut buffer = RingBuffer::new(5);
        buffer.write(vec![1, 2, 3]).unwrap();
        assert_eq!(buffer.read(), Some(vec![1, 2, 3]));
        assert_eq!(buffer.read(), None); // buffer empty after read
    }

    #[test]
    fn writing_overload() {
        let mut buffer = RingBuffer::new(3);

        buffer.write(vec![2, 4, 6, 7, 8, 9]).unwrap();

        assert_eq!(buffer.read(), Some(vec![7, 8, 9]));
    }

    #[test]
    fn overwrite_when_full() {
        let mut buffer = RingBuffer::new(3);
        buffer.write(vec![1, 2, 3]).unwrap(); // fills buffer
        buffer.write(vec![4]).unwrap(); // should overwrite oldest (1)
        assert_eq!(buffer.read(), Some(vec![4]));
    }

    #[test]
    fn write_more_than_capacity() {
        let mut buffer = RingBuffer::new(3);
        buffer.write(vec![10, 11, 12, 13, 14]).unwrap();
        // buffer can only hold last 3 bytes
        assert_eq!(buffer.read(), Some(vec![13, 14]));
    }

    #[test]
    fn wrap_around_write() {
        let mut buffer = RingBuffer::new(5);
        buffer.write(vec![1, 2, 3, 4]).unwrap();
        buffer.read().unwrap(); // read all
        buffer.write(vec![5, 6, 7, 8]).unwrap(); // should wrap around
        assert_eq!(buffer.read(), Some(vec![5, 6, 7, 8]));
    }

    #[test]
    fn read_after_wrap() {
        let mut buffer = RingBuffer::new(5);

        // Fill the buffer completely
        buffer.write(vec![1, 2, 3, 4, 5]).unwrap();

        // Read everything to empty it
        assert_eq!(buffer.read(), Some(vec![1, 2, 3, 4, 5]));

        // Write more data, which will wrap around
        buffer.write(vec![6, 7, 8, 9]).unwrap();

        // Now reading should return all the newly written data
        // Partial reads are not supported; we always read everything
        assert_eq!(buffer.read(), Some(vec![6, 7, 8, 9]));
    }

    #[test]
    fn consecutive_overwrites() {
        let mut buffer = RingBuffer::new(3);
        buffer.write(vec![1, 2, 3]).unwrap(); // buffer full
        buffer.write(vec![4, 5]).unwrap(); // should overwrite oldest 2 bytes
        assert_eq!(buffer.read(), Some(vec![4, 5]));
    }
}
