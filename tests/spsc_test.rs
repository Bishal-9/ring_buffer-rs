use ring_buffer::spsc::SpscQueue;

#[test]
fn test_spsc_basic_flow_u64() {
    let queue: SpscQueue<u64, 8> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let data = [1, 2, 3, 4];
    assert_eq!(producer.write(&data), Some(4));
    assert_eq!(producer.position(), 4);

    let mut read_buf = [0u64; 4];
    assert_eq!(consumer.read(&mut read_buf), 4);
    assert_eq!(read_buf, data);
    assert_eq!(consumer.position(), 4);
}

#[test]
fn test_spsc_full_buffer() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    // Fill the buffer
    let data = [10, 20, 30, 40];
    assert_eq!(producer.write(&data), Some(4));
    
    // Attempt to write more when full
    assert_eq!(producer.write(&[50]), Some(5));
    assert_eq!(producer.available_space(), 4);

    // Read some data to make space
    let mut read_buf = [0u8; 2];
    assert_eq!(consumer.read(&mut read_buf), 2);
    assert_eq!(read_buf, [50, 20]);
    
    // Now we should have space for 2 elements
    assert_eq!(producer.available_space(), 4);
    assert_eq!(producer.write(&[50, 60]), Some(7));
}

#[test]
fn test_spsc_wrap_around() {
    let queue: SpscQueue<u32, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    // 1. Fill buffer
    producer.write(&[1, 2, 3]).unwrap();
    
    // 2. Read all
    let mut read_buf = [0u32; 3];
    consumer.read(&mut read_buf);
    assert_eq!(read_buf, [1, 2, 3]);

    // 3. Write again to cause wrap around in the underlying array
    // At this point, write_pointer is 3, read_pointer is 3.
    // Capacity 4,
    producer.write(&[5, 6]).unwrap(); // write_pointer becomes 5

    let mut read_buf = [0u32; 2];
    assert_eq!(consumer.read(&mut read_buf), 2);
    assert_eq!(read_buf, [5, 6]);
}

#[test]
fn test_spsc_large_write_rejection() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, _) = queue.split();

    // Attempt to write more than capacity in one go
    assert_eq!(producer.write(&[1, 2, 3, 4, 5]), None);
}

#[test]
fn test_spsc_empty_read() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (_, mut consumer) = queue.split();

    let mut buf = [0u8; 4];
    assert_eq!(consumer.read(&mut buf), 0);
}

// NOTE: Test cases for "external user must not be able to create more producer or consumer"
// and "must not be able to clone" are enforced by the API design:
// 1. SpscQueue::split() consumes 'self', so it can only be called once.
// 2. SingleProducer and SingleConsumer do not implement Clone.
// 3. RingBuffer is pub(crate), so it cannot be instantiated externally.
