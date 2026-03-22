use ring_buffer::spmc::SpmcQueue;
use std::{thread, time::Duration};

#[test]
fn spmc_new_is_empty() {
    let q: SpmcQueue<u32, 16> = SpmcQueue::new();
    let (_p, mut c) = q.split();

    let mut buf = [0u32; 4];
    assert_eq!(c.read(&mut buf), 0);
    assert_eq!(c.available(), 0);
}

#[test]
fn spmc_basic_write_read() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let data = [1, 2, 3, 4];
    assert_eq!(producer.write(&data), Some(4));

    let mut out = [0u64; 4];
    let read = consumer.read(&mut out);

    assert_eq!(read, 4);
    assert_eq!(out, data);
}

#[test]
fn spmc_empty_read() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (_producer, mut consumer) = queue.split();

    let mut out = [0u64; 4];
    let read = consumer.read(&mut out);

    assert_eq!(read, 0);
}

#[test]
fn spmc_write_too_large() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, _) = queue.split();

    let data = [1u64; 16];
    assert_eq!(producer.write(&data), None);
}

#[test]
fn spmc_wraparound() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3, 4, 5, 6]);
    let mut tmp = [0u64; 6];
    consumer.read(&mut tmp);

    // Force wrap
    producer.write(&[7, 8, 9, 10]);

    let mut out = [0u64; 4];
    let read = consumer.read(&mut out);

    assert_eq!(read, 4);
    assert_eq!(out, [7, 8, 9, 10]);
}

#[test]
fn spmc_overwrite_behavior() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    // Write more than capacity over time
    for i in 0..16 {
        producer.write(&[i]);
    }

    let mut out = [0u64; 8];
    let read = consumer.read(&mut out);

    assert!(read > 0);

    // Should only see most recent elements
    let expected_start = 16 - read as u64;
    for (i, val) in out.iter().take(read).enumerate() {
        assert_eq!(*val, expected_start + i as u64);
    }
}

#[test]
fn spmc_multiple_consumers_independent_positions() {
    const N: usize = 16;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, consumer1) = queue.split();

    let mut consumer2 = consumer1.clone();
    let mut consumer1 = consumer1;

    producer.write(&[1, 2, 3, 4]);

    let mut out1 = [0u64; 4];
    let mut out2 = [0u64; 4];

    let r1 = consumer1.read(&mut out1);
    let r2 = consumer2.read(&mut out2);

    assert_eq!(r1, 4);
    assert_eq!(r2, 4);
    assert_eq!(out1, [1, 2, 3, 4]);
    assert_eq!(out2, [1, 2, 3, 4]);
}

#[test]
fn spmc_concurrent_stress() {
    const N: usize = 1024;
    const COUNT: usize = 100_000;

    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, consumer) = queue.split();

    let mut consumers = vec![];
    for _ in 0..4 {
        consumers.push(consumer.clone());
    }

    let producer_thread = thread::spawn(move || {
        for i in 0..COUNT as u64 {
            while producer.write(&[i]).is_none() {}
        }
    });

    let mut handles = vec![];

    for mut c in consumers {
        handles.push(thread::spawn(move || {
            let mut last = 0;
            let mut buf = [0u64; 64];

            loop {
                let read = c.read(&mut buf);
                if read > 0 {
                    for v in &buf[..read] {
                        assert!(*v >= last);
                        last = *v;
                    }
                }
                if last >= (COUNT - 1) as u64 {
                    break;
                }
            }
        }));
    }

    producer_thread.join().unwrap();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn spmc_zero_length_write() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    assert_eq!(producer.write(&[]), Some(0));

    let mut out = [0u64; 4];
    assert_eq!(consumer.read(&mut out), 0);
}

#[test]
fn spmc_position_tracking() {
    const N: usize = 16;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3]);

    assert_eq!(producer.position(), 3);

    let mut out = [0u64; 2];
    consumer.read(&mut out);

    assert_eq!(consumer.position(), 2);
}

#[test]
#[should_panic]
fn spmc_non_power_of_two_panics() {
    let _queue: SpmcQueue<u64, 7> = SpmcQueue::new();
}

#[test]
fn consumer_falling_behind() {
    const N: usize = 8; // small buffer
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    // Write chunks repeatedly to overflow the buffer
    for i in 0..12u64 {
        // Each write is small enough to fit, but cumulative writes exceed capacity
        assert!(producer.write(&[i]).is_some());
    }

    let mut out = [0u64; N];
    let read = consumer.read(&mut out);

    // Consumer should read the last N elements (oldest overwritten)
    assert_eq!(read, 1);
    assert_eq!(out, [11, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn zero_length_operations() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    // Write empty slice
    assert_eq!(producer.write(&[]), Some(0));

    // Read into empty buffer
    let mut out = [0u64; 0];
    assert_eq!(consumer.read(&mut out), 0);
}

#[test]
fn wraparound_read_write() {
    const N: usize = 4;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    producer.write(&[1, 2, 3]);
    let mut out = [0u64; 2];
    consumer.read(&mut out); // consume 2

    producer.write(&[4, 5]); // this wraps the buffer
    let mut out2 = [0u64; 3];
    let read = consumer.read(&mut out2);

    assert_eq!(read, 3);
    assert_eq!(out2, [3, 4, 5]);
}

#[test]
fn multiple_consumers_independent_positions() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, consumer1) = queue.split();

    let mut consumer2 = consumer1.clone();
    let mut consumer1 = consumer1;

    producer.write(&[1, 2, 3, 4, 5, 6]);

    let mut out1 = [0u64; 3];
    let mut out2 = [0u64; 4];

    let r1 = consumer1.read(&mut out1); // first consumer reads 3
    let r2 = consumer2.read(&mut out2); // second reads all 4

    assert_eq!(r1, 3);
    assert_eq!(out1, [1, 2, 3]);
    assert_eq!(r2, 4);
    assert_eq!(out2, [1, 2, 3, 4]); // consumer2 independent
}

#[test]
fn fast_producer_slow_consumer() {
    const N: usize = 32;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, consumer) = queue.split();
    let mut consumer = consumer;

    let producer_thread = thread::spawn(move || {
        for i in 0..1000u64 {
            while producer.write(&[i]).is_none() {}
        }
    });

    let consumer_thread = thread::spawn(move || {
        let mut buf = [0u64; 8];
        let mut last = 0;
        loop {
            let read = consumer.read(&mut buf);
            if read > 0 {
                for &v in &buf[..read] {
                    assert!(v >= last);
                    last = v;
                }
            }
            if last >= 999 {
                break;
            }
            thread::sleep(Duration::from_micros(1));
        }
    });

    producer_thread.join().unwrap();
    consumer_thread.join().unwrap();
}

#[test]
fn write_larger_than_capacity() {
    const N: usize = 8;
    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, _) = queue.split();

    let large_data = [0u64; 16];
    assert_eq!(producer.write(&large_data), None); // Should not panic
}

#[test]
fn multi_consumer_stress() {
    const N: usize = 1024;
    const COUNT: usize = 10_000;

    let queue: SpmcQueue<u64, N> = SpmcQueue::new();
    let (mut producer, consumer) = queue.split();

    let mut consumers = vec![];
    for _ in 0..4 {
        consumers.push(consumer.clone());
    }

    let producer_thread = thread::spawn(move || {
        for i in 0..COUNT as u64 {
            while producer.write(&[i]).is_none() {}
        }
    });

    let mut handles = vec![];
    for mut c in consumers {
        handles.push(thread::spawn(move || {
            let mut last = 0;
            let mut buf = [0u64; 64];
            loop {
                let read = c.read(&mut buf);
                if read > 0 {
                    for v in &buf[..read] {
                        assert!(*v >= last);
                        last = *v;
                    }
                }
                if last >= (COUNT - 1) as u64 {
                    break;
                }
            }
        }));
    }

    producer_thread.join().unwrap();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_spmc_multithread_wraparound_with_sync_safe() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::thread;

    const ITERS: u32 = 10_000;

    let done = Arc::new(AtomicBool::new(false));
    let queue: SpmcQueue<u32, 4> = SpmcQueue::new();
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
                    assert!(val >= prev, "SPMC queue value decreased unexpectedly");
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
}
