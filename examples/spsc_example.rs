//! # Single-Producer Single-Consumer (SPSC) Example
//!
//! This example demonstrates the most efficient use case for the `ring_buffer` library:
//! a point-to-point communication channel between two dedicated threads.
//!
//! ## Key Characteristics
//! - **Zero Contention**: Producer and Consumer never compete for the same atomic pointers.
//! - **High Throughput**: Ideal for high-frequency data streams (e.g., sensor data, market feeds).
//! - **Strict Ownership**: Only one producer handle and one consumer handle can exist.

use ring_buffer::spsc::SpscQueue;
use std::thread;
use std::time::Duration;

fn main() {
    // 1. Initialize the SpscQueue.
    // The capacity MUST be a power of two (e.g., 8, 16, 1024, 4096).
    const CAPACITY: usize = 16;
    let queue: SpscQueue<u64, CAPACITY> = SpscQueue::new();

    // 2. Split the queue into Producer and Consumer handles.
    // These handles are NOT Cloneable in SPSC mode to enforce single-threaded access per end.
    let (mut producer, mut consumer) = queue.split();

    // 3. Spawn a Producer Thread
    let producer_handle = thread::spawn(move || {
        println!("[Producer] Sequential write started...");
        for i in 1..=20 {
            let data = [i as u64];

            // write() returns Some(count) on success, or None if the input exceeds capacity.
            // Note: In this implementation, if the buffer is full, it overwrites the oldest data.
            producer.write(&data);

            println!("[Producer] Sent: {}", i);
            thread::sleep(Duration::from_millis(50));
        }
        println!("[Producer] Finished.");
    });

    // 4. Spawn a Consumer Thread
    let consumer_handle = thread::spawn(move || {
        println!("[Consumer] Real-time read started...");
        let mut read_count = 0;
        let mut output = [0u64; 1];

        // Continue reading until we've processed our expected 20 items.
        while read_count < 20 {
            // read() returns the number of elements actually copied into the buffer.
            let received = consumer.read(&mut output);

            if received > 0 {
                println!(
                    "[Consumer] Received: {} (at position {})",
                    output[0],
                    consumer.position()
                );
                read_count += 1;
            } else {
                // No data available yet, yield the CPU briefly.
                std::hint::spin_loop();
            }
        }
        println!("[Consumer] Finished.");
    });

    // 5. Wait for both threads to complete.
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();

    println!("SPSC Example completed successfully.");
}
