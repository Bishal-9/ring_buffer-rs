//! # Single-Producer Multi-Consumer (SPMC) Example
//!
//! This example demonstrates a single producer broadcasting data to multiple
//! consumer threads.
//!
//! ## Key Characteristics
//! - **Fan-Out Pattern**: One source distributing load across multiple worker threads.
//! - **Work Stealing/Distribution**: Different consumers will compete for available segments.
//! - **Sequential Producer**: No contention on the write side, providing maximum ingest speed.

use ring_buffer::spmc::SpmcQueue;
use std::thread;
use std::time::Duration;

fn main() {
    const CAPACITY: usize = 64;
    let queue: SpmcQueue<i32, CAPACITY> = SpmcQueue::new();

    let (mut producer, consumer) = queue.split();

    // Spawn 2 consumer threads.
    let mut consumer_handles = Vec::new();
    for consumer_id in 0..2 {
        // MultipleConsumer handles are Cloneable.
        let mut c = consumer.clone();

        let handle = thread::spawn(move || {
            let mut buf = [0i32; 1];
            let mut count = 0;
            // We'll have each consumer try to read until they hit a specific sum
            // or a timeout, just for demonstration purposes.
            while count < 10 {
                if c.read(&mut buf) > 0 {
                    println!("[Consumer {}] Grabbed: {}", consumer_id, buf[0]);
                    count += 1;
                    thread::sleep(Duration::from_millis(20));
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        consumer_handles.push(handle);
    }

    // Producer sends a burst of data.
    thread::sleep(Duration::from_millis(100));
    println!("[Producer] Sending batch of 20 items...");
    for i in 0..20 {
        producer.write(&[i]);
    }

    for h in consumer_handles {
        h.join().unwrap();
    }

    println!("SPMC Example completed successfully.");
}
