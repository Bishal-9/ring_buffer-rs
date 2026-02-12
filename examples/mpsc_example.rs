//! # Multi-Producer Single-Consumer (MPSC) Example
//!
//! This example demonstrates multiple producer threads sending data to a single
//! dedicated consumer thread.
//!
//! ## Key Characteristics
//! - **Fan-In Pattern**: Multiple sources of data consolidated into one processing stream.
//! - **Scalable Productivity**: Producer handles can be cloned and shared across threads.
//! - **Lock-Free Coordination**: Multiple producers use `compare_exchange` to safely reserve slots.

use ring_buffer::mpsc::MpscQueue;
use std::thread;
use std::sync::Arc;
use std::time::Duration;

fn main() {
    // Capacity must be a power of two.
    const CAPACITY: usize = 32;
    let queue: MpscQueue<u32, CAPACITY> = MpscQueue::new();
    
    // Split into a Multi-Producer handle and a Single-Consumer handle.
    let (producer, mut consumer) = queue.split();

    // Spawn 3 producer threads, each sending a different range of numbers.
    let mut handles = Vec::new();
    for thread_id in 0..3 {
        // MultiProducer handles are Cloneable.
        let mut p = producer.clone();
        
        let handle = thread::spawn(move || {
            for i in 1..=5 {
                let val = (thread_id * 100) + i;
                p.write(&[val]);
                println!("[Producer {}] Sent: {}", thread_id, val);
                thread::sleep(Duration::from_millis(thread_id as u64 * 30 + 10));
            }
        });
        handles.push(handle);
    }

    // Consumer thread processes data from all producers.
    let consumer_handle = thread::spawn(move || {
        let mut processed = 0;
        let mut buf = [0u32; 1];

        // In this example, we expect 15 total items (3 threads * 5 items).
        while processed < 15 {
            if consumer.read(&mut buf) > 0 {
                println!("[Consumer] Processed: {}", buf[0]);
                processed += 1;
            } else {
                std::hint::spin_loop();
            }
        }
    });

    for h in handles {
        h.join().unwrap();
    }
    consumer_handle.join().unwrap();

    println!("MPSC Example completed successfully.");
}
