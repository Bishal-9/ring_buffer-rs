//! # Multi-Producer Multi-Consumer (MPMC) Example
//!
//! This example demonstrates the most complex topology where multiple threads
//! are writing and reading simultaneously from the same buffer.
//!
//! ## Key Characteristics
//! - **Full Parallelism**: Maximum concurrency on both ingress and egress sides.
//! - **Safe Overwrites**: The library ensures that even under heavy congestion, 
//!   producers can overwrite old data without causing memory corruption.
//! - **High Throughput**: Optimized for multi-core systems where data needs to 
//!   move between pools of threads.

use ring_buffer::mpmc::MpmcQueue;
use std::thread;
use std::time::Duration;

fn main() {
    const CAPACITY: usize = 128;
    let queue: MpmcQueue<usize, CAPACITY> = MpmcQueue::new();
    
    let (producer, consumer) = queue.split();

    // 1. Setup multi-producers
    let mut p_handles = Vec::new();
    for thread_id in 0..2 {
        let mut p = producer.clone();
        p_handles.push(thread::spawn(move || {
            for i in 0..50 {
                let val = (thread_id * 1000) + i;
                p.write(&[val]);
                if i % 10 == 0 {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        }));
    }

    // 2. Setup multi-consumers
    let mut c_handles = Vec::new();
    for thread_id in 0..2 {
        let mut c = consumer.clone();
        c_handles.push(thread::spawn(move || {
            let mut buf = [0usize; 1];
            let mut read_count = 0;
            // Each consumer attempts to read 50 samples
            while read_count < 50 {
                if c.read(&mut buf) > 0 {
                    read_count += 1;
                    if read_count % 10 == 0 {
                        println!("[Consumer {}] Read {} items so far...", thread_id, read_count);
                    }
                } else {
                    std::hint::spin_loop();
                }
            }
        }));
    }

    // Wait for everyone to finish
    for h in p_handles { h.join().unwrap(); }
    for h in c_handles { h.join().unwrap(); }

    println!("MPMC Example completed successfully.");
}
