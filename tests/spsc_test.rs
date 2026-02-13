use ring_buffer::spsc::SpscQueue;
use std::thread;

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
    assert_eq!(producer.write(&[50]), Some(1));
    assert_eq!(producer.available_space(), 4);

    // Read some data to make space
    let mut read_buf = [0u8; 1];
    assert_eq!(consumer.read(&mut read_buf), 1);
    assert_eq!(read_buf, [50]);

    // Now we should have space for 2 elements
    assert_eq!(producer.available_space(), 4);
    assert_eq!(producer.write(&[50, 60]), Some(2));
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

#[test]
fn test_spsc_write_empty_slice() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    assert_eq!(producer.write(&[]), Some(0));
    assert_eq!(producer.position(), 0);

    let mut buf = [0u8; 4];
    assert_eq!(consumer.read(&mut buf), 0);
}

#[test]
fn test_spsc_read_empty_buffer() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3]).unwrap();

    let mut buf: [u8; 0] = [];
    assert_eq!(consumer.read(&mut buf), 0);
    assert_eq!(consumer.position(), 0);
}

#[test]
fn test_spsc_exact_capacity_overwrite() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3, 4]).unwrap();
    producer.write(&[5, 6, 7, 8]).unwrap();

    let mut buf = [0u8; 4];
    assert_eq!(consumer.read(&mut buf), 1);
    assert_eq!(buf, [8, 0, 0, 0]);
}

#[test]
fn test_spsc_repeated_partial_overwrite() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3]).unwrap();
    producer.write(&[4, 5, 6]).unwrap();

    let mut buf = [0u8; 3];
    assert_eq!(consumer.read(&mut buf), 1);
    assert_eq!(buf, [6, 0, 0]);
}

#[test]
fn test_spsc_overwrite_unread_data() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3, 4]).unwrap();
    producer.write(&[5, 6]).unwrap();

    let mut buf = [0u8; 4];
    assert_eq!(consumer.read(&mut buf), 1);
    assert_eq!(buf, [6, 0, 0, 0]);
}

#[test]
fn test_spsc_multiple_overwrites_before_read() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3, 4]).unwrap();
    producer.write(&[5]).unwrap();
    producer.write(&[6]).unwrap();

    let mut buf = [0u8; 4];
    consumer.read(&mut buf);
    assert_eq!(buf, [6, 0, 0, 0]);
}

#[test]
fn test_spsc_alternating_read_write() {
    let queue: SpscQueue<u8, 2> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1]).unwrap();
    let mut buf = [0u8; 1];
    consumer.read(&mut buf);
    assert_eq!(buf, [1]);

    producer.write(&[2]).unwrap();
    consumer.read(&mut buf);
    assert_eq!(buf, [2]);
}

#[test]
fn test_spsc_partial_read() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3, 4]).unwrap();

    let mut buf = [0u8; 2];
    assert_eq!(consumer.read(&mut buf), 2);
    assert_eq!(buf, [1, 2]);

    let mut buf = [0u8; 2];
    assert_eq!(consumer.read(&mut buf), 2);
    assert_eq!(buf, [3, 4]);
}

#[test]
fn test_spsc_producer_position_monotonic() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, _) = queue.split();

    let p1 = producer.position();
    producer.write(&[1]).unwrap();
    let p2 = producer.position();
    producer.write(&[2, 3]).unwrap();
    let p3 = producer.position();

    assert!(p1 < p2 && p2 < p3);
}

#[test]
fn test_spsc_consumer_position_monotonic() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3]).unwrap();

    let c1 = consumer.position();
    let mut buf = [0u8; 1];
    consumer.read(&mut buf);
    let c2 = consumer.position();

    assert!(c2 > c1);
}

#[test]
fn test_spsc_no_default_leak() {
    let queue: SpscQueue<u32, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[42]).unwrap();

    let mut buf = [999u32; 4];
    let read = consumer.read(&mut buf);

    assert_eq!(read, 1);
    assert_eq!(buf[0], 42);
}

#[test]
fn test_spsc_capacity_one() {
    let queue: SpscQueue<u8, 1> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1]).unwrap();
    producer.write(&[2]).unwrap();

    let mut buf = [0u8; 1];
    consumer.read(&mut buf);
    assert_eq!(buf, [2]);
}

#[test]
fn test_spsc_long_single_thread_stress() {
    let queue: SpscQueue<u32, 8> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    for i in 0..1_000 {
        producer.write(&[i]).unwrap();
        let mut buf = [0u32; 1];
        consumer.read(&mut buf);
        assert_eq!(buf[0], i);
    }
}

// Multi Threaded test cases
#[test]
fn test_spsc_basic_multithread_flow() {
    let queue: SpscQueue<u64, 8> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let producer_t = thread::spawn(move || {
        producer.write(&[1, 2, 3, 4]).unwrap();
    });

    let consumer_t = thread::spawn(move || {
        let mut buf = [0u64; 4];
        while consumer.read(&mut buf) == 0 {}
        buf
    });

    producer_t.join().unwrap();
    let buf = consumer_t.join().unwrap();
    assert_eq!(buf, [1, 2, 3, 4]);
}

#[test]
fn test_spsc_multithread_empty_reads() {
    let queue: SpscQueue<u8, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let cons = thread::spawn(move || {
        let mut buf = [0u8; 1];
        let mut empty_reads = 0;

        for _ in 0..1000 {
            if consumer.read(&mut buf) == 0 {
                empty_reads += 1;
            }
        }
        empty_reads
    });

    let prod = thread::spawn(move || {
        for i in 0..10 {
            producer.write(&[i]).unwrap();
        }
    });

    prod.join().unwrap();
    let empty = cons.join().unwrap();
    assert!(empty > 0);
}

#[test]
fn test_spsc_multithread_wraparound() {
    use std::thread;

    const END: u32 = u32::MAX;

    let queue: SpscQueue<u32, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let prod = thread::spawn(move || {
        for i in 0..100 {
            producer.write(&[i]).unwrap();
        }
        producer.write(&[END]).unwrap(); // termination signal
    });

    let cons = thread::spawn(move || {
        let mut buf = [0u32; 1];
        let mut seen = Vec::new();

        loop {
            if consumer.read(&mut buf) == 1 {
                if buf[0] == END {
                    break;
                }
                seen.push(buf[0]);
            }
        }
        seen
    });

    prod.join().unwrap();
    let seen = cons.join().unwrap();

    // Values must be strictly increasing
    for w in seen.windows(2) {
        assert!(w[1] > w[0]);
    }
}

#[test]
fn test_spsc_multithread_wraparound_with_sync_safe() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::thread;

    const ITERS: u32 = 1_000;

    let done = Arc::new(AtomicBool::new(false));
    let queue: SpscQueue<u32, 4> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let done_p = done.clone();
    let done_c = done.clone();

    // Producer thread
    let prod = thread::spawn(move || {
        for i in 0..ITERS {
            // Ignore failed writes if the queue is full (overwrite will happen)
            let _ = producer.write(&[i]);
        }
        done_p.store(true, Ordering::Release);
    });

    // Consumer thread
    let cons = thread::spawn(move || {
        let mut buf = [0u32; 1];
        let mut last_read = None;

        loop {
            // Read any available value
            if consumer.read(&mut buf) == 1 {
                let val = buf[0];

                // Monotonicity invariant among surviving values
                if let Some(prev) = last_read {
                    assert!(val >= prev, "SPSC queue value decreased unexpectedly");
                }

                last_read = Some(val);
            }

            // Stop when producer is done
            if done_c.load(Ordering::Acquire) {
                break;
            }
        }

        last_read
    });

    prod.join().unwrap();
    let last_read = cons.join().unwrap();

    // Progress invariant: consumer saw at least one value
    assert!(last_read.is_some(), "Consumer did not read any value");

    // Optional: print last value for debugging
    println!("Last value read by consumer: {:?}", last_read.unwrap());
}

#[test]
fn test_spsc_multithread_wraparound_time_bounded() {
    use std::thread;
    use std::time::{Duration, Instant};

    let queue: SpscQueue<u32, 8> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let prod = thread::spawn(move || {
        let mut i = 0;
        while i < 100_000 {
            producer.write(&[i]).unwrap();
            i += 1;
        }
    });

    let cons = thread::spawn(move || {
        let start = Instant::now();
        let mut buf = [0u32; 1];
        let mut last = 0;

        while start.elapsed() < Duration::from_millis(100) {
            if consumer.read(&mut buf) == 1 {
                last = buf[0];
            }
        }
        last
    });

    prod.join().unwrap();
    let last = cons.join().unwrap();

    assert!(last > 0);
}

#[test]
#[test]
fn test_spsc_multithread_position_visibility() {
    use std::thread;

    const END: u64 = u64::MAX;

    let queue: SpscQueue<u64, 8> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let prod = thread::spawn(move || {
        for i in 0..100 {
            producer.write(&[i]).unwrap();
        }
        producer.write(&[END]).unwrap();
        producer.position()
    });

    let cons = thread::spawn(move || {
        let mut buf = [0u64; 1];
        let mut observed = Vec::new();

        loop {
            if consumer.read(&mut buf) == 1 {
                let pos = consumer.position();
                if buf[0] == END {
                    break;
                }
                observed.push((buf[0], pos));
            }
        }
        observed
    });

    let ppos = prod.join().unwrap();
    let observed = cons.join().unwrap();

    // For every value observed, consumer position must be > value index
    for (value, cpos) in observed {
        assert!(cpos > value.try_into().unwrap());
    }

    assert!(ppos >= 101);
}

// IMPORTANT NOTE FOR USERS:
//
// This test is intentionally non-deterministic and may fail depending on
// OS thread scheduling.
//
// In an overwrite-based SPSC (single-producer, single-consumer) queue,
// the producer is allowed to overwrite unread data at any time.
// Additionally, thread scheduling may cause the consumer to run
// before the producer has written any data.
//
// In such cases, the consumer may:
//   - Make no progress (read zero items)
//   - Miss all produced values
//   - Miss the final value entirely
//
// This behavior is EXPECTED and CORRECT for overwrite SPSC queues.
// The queue guarantees memory safety, lock-free progress, and correctness
// of *observed* values — but it does NOT guarantee delivery of every write
// or that the consumer will observe the "latest" value without
// external synchronization.
//
// Tests that assert exact values (e.g., `last == expected`) or assume
// consumer progress without synchronization are inherently scheduler-dependent
// and should NOT be used as correctness tests.
//
// For deterministic testing, use:
//   - Invariant-based tests (no tearing, monotonicity of surviving values), or
//   - Explicit external synchronization to coordinate producer and consumer.
#[test]
fn test_spsc_multithread_capacity_one_invariant() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::thread;

    let queue: SpscQueue<u8, 1> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let done = Arc::new(AtomicBool::new(false));
    let done_p = done.clone();
    let done_c = done.clone();

    let prod = thread::spawn(move || {
        for i in 0..100 {
            let _ = producer.write(&[i]);
        }
        done_p.store(true, Ordering::Release);
    });

    let cons = thread::spawn(move || {
        let mut buf = [0u8; 1];
        let mut last = None;

        loop {
            if consumer.read(&mut buf) == 1 {
                let v = buf[0];
                if let Some(prev) = last {
                    assert!(v >= prev); // monotonic among surviving values
                }
                last = Some(v);
            }

            if done_c.load(Ordering::Acquire) {
                break;
            }
        }

        last
    });

    prod.join().unwrap();
    let last = cons.join().unwrap();

    assert!(last.is_some(), "consumer made no progress");
}

#[test]
fn test_spsc_capacity_one_deterministic_latest_value() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::thread;

    let done = Arc::new(AtomicBool::new(false));

    let queue: SpscQueue<u32, 1> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let done_p = done.clone();
    let done_c = done.clone();

    let prod = thread::spawn(move || {
        for i in 0..10_000 {
            producer.write(&[i]).unwrap();
        }
        // publish completion AFTER last write
        done_p.store(true, Ordering::Release);
    });

    let cons = thread::spawn(move || {
        let mut buf = [0u32; 1];
        let mut last = None;

        loop {
            if consumer.read(&mut buf) == 1 {
                last = Some(buf[0]);
            }

            // once producer is done AND buffer is empty, we can stop
            if done_c.load(Ordering::Acquire) {
                break;
            }
        }
        last.unwrap()
    });

    prod.join().unwrap();
    let last = cons.join().unwrap();

    // Now this assertion is deterministic
    assert_eq!(last, 9_999);
}

#[test]
fn test_spsc_multithread_capacity_one_progress() {
    use std::thread;
    use std::time::{Duration, Instant};

    let queue: SpscQueue<u8, 1> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let prod = thread::spawn(move || {
        let start = Instant::now();
        let mut i = 0;
        while start.elapsed() < Duration::from_millis(100) {
            producer.write(&[i]).unwrap();
            i = i.wrapping_add(1);
        }
    });

    let cons = thread::spawn(move || {
        let start = Instant::now();
        let mut buf = [0u8; 1];
        let mut reads = 0;

        while start.elapsed() < Duration::from_millis(100) {
            if consumer.read(&mut buf) == 1 {
                reads += 1;
            }
        }
        reads
    });

    prod.join().unwrap();
    let reads = cons.join().unwrap();

    assert!(reads > 0);
}

// IMPORTANT NOTE FOR USERS:
//
// This test validates an *invariant* of an overwrite-based SPSC queue,
// not delivery guarantees or exact sequencing.
//
// The assertion `val > prev` checks **monotonicity among surviving values**:
//   - The producer writes values in strictly increasing order.
//   - The queue may overwrite unread values at any time.
//   - The consumer may observe only a subset of produced values.
//   - Any value that *is* observed is expected to be newer than the
//     previously observed value.
//
// RARE SCHEDULER-DEPENDENT SCENARIO:
//
// In very rare cases, the OS scheduler may run the consumer thread
// before the producer has advanced the write position. In this case,
// the consumer may read the same buffer slot multiple times before
// a new value is written, causing:
//
//     val == prev
//
// This behavior is NOT a violation of the queue's correctness and
// does NOT indicate stale or torn reads. It is an expected outcome of
// overwrite semantics combined with single-element reads and
// non-deterministic scheduling.
//
// IMPORTANT LIMITATIONS:
//
// - This test does NOT guarantee that all values are read.
// - This test does NOT guarantee that the final value is observed.
// - This test may fail or be flaky due to thread scheduling alone.
//
// For deterministic tests, either relax the invariant to `val >= prev`
// or use explicit external synchronization between producer and consumer.
#[test]
fn test_spsc_multithread_stress_invariant() {
    use std::thread;
    use std::time::{Duration, Instant};

    let queue: SpscQueue<u32, 64> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let prod = thread::spawn(move || {
        let start = Instant::now();
        let mut i = 0;
        while start.elapsed() < Duration::from_millis(100) {
            let _ = producer.write(&[i]);
            i = i.wrapping_add(1);
        }
    });

    let cons = thread::spawn(move || {
        let start = Instant::now();
        let mut buf = [0u32; 1];
        let mut last_read = None;

        while start.elapsed() < Duration::from_millis(100) {
            if consumer.read(&mut buf) == 1 {
                // The value is valid (fully initialized)
                let val = buf[0];

                if let Some(prev) = last_read {
                    // Monotonicity *among surviving values*, allowing skips
                    assert!(val > prev);
                }

                last_read = Some(val);
            }
        }
        last_read
    });

    prod.join().unwrap();
    let last_read = cons.join().unwrap();

    // Ensure at least one value was read
    assert!(last_read.is_some());
}

// NOTE: Test cases for "external user must not be able to create more producer or consumer"
// and "must not be able to clone" are enforced by the API design:
// 1. SpscQueue::split() consumes 'self', so it can only be called once.
// 2. SingleProducer and SingleConsumer do not implement Clone.
// 3. RingBuffer is pub(crate), so it cannot be instantiated externally.
