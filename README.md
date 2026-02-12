# ring_buffer-rs

[![Crates.io](https://img.shields.io/crates/v/ring_buffer.svg)](https://crates.io/crates/ring_buffer)
[![Documentation](https://docs.rs/ring_buffer/badge.svg)](https://docs.rs/ring_buffer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance, lock-free, overwrite-capable ring buffer implementation in Rust. Designed for low-latency systems such as High-Frequency Trading (HFT), audio processing, and telemetry where thread synchronization overhead must be kept to an absolute minimum.

## 🚀 Key Features

- **Lock-Free Concurrency**: Uses atomic operations with strict `Acquire/Release` memory ordering for thread safety without the cost of mutexes or spinlocks.
- **Cache-Line Isolation**: Internal counters are padded to 64 bytes to prevent "false sharing," ensuring that producers and consumers don't contend for the same CPU cache line.
- **Overwrite Semantics**: High-priority producers never block. If the buffer is full, the latest data overwrites the oldest unread data—ideal for "latest-value" telemetry.
- **Power-of-Two Optimizations**: Enforces power-of-two capacities to replace expensive modulo `%` operations with lightning-fast bitwise masking.
- **Zero-Copy Data Transfer**: Utilizes `copy_nonoverlapping` for efficient, raw memory moves of data slices.
- **Flexible Topologies**: Native support for **SPSC**, **MPSC**, **SPMC**, and **MPMC** configurations.

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
ring_buffer = "0.1.0"
```

## 🛠 Usage

### Single-Producer Single-Consumer (SPSC)
The most efficient mode for point-to-point communication.

```rust
use ring_buffer::spsc::SpscQueue;

fn main() {
    // Capacity must be a power of two
    let queue: SpscQueue<u64, 1024> = SpscQueue::new();
    let (mut producer, mut consumer) = queue.split();

    // Producer thread
    std::thread::spawn(move || {
        let data = [1, 2, 3, 4, 5];
        producer.write(&data);
    });

    // Consumer thread
    let handle = std::thread::spawn(move || {
        let mut buf = [0u64; 5];
        while consumer.read(&mut buf) == 0 {
            std::hint::spin_loop();
        }
        println!("Received: {:?}", buf);
    });

    handle.join().unwrap();
}
```

### Multi-Producer Multi-Consumer (MPMC)
Safe for concurrent access from multiple threads on both sides.

```rust
use ring_buffer::mpmc::MpmcQueue;
use std::sync::Arc;

let queue: MpmcQueue<i32, 512> = MpmcQueue::new();
let (producer, consumer) = queue.split();

// Producers and Consumers can be cloned in MPMC mode
let mut p1 = producer.clone();
let mut c1 = consumer.clone();
```

## 🏗 Architecture

The core of the library is built around a `RingBuffer` struct that manages a contiguous array of type `T`.

- **Padded Counters**: The `write_pointer` and `read_pointer` are stored in `PaddedAtomicUsize`, ensuring they reside on different cache lines.
- **Jolt Mechanism**: If a producer overwrites data that a slow consumer was about to read, the consumer's local pointer is automatically "jolted" forward to the next valid data segment to prevent reading torn or stale data.

For more details, see the [ARCHITECTURE.md](ARCHITECTURE.md).

## ⚠️ Constraints

1. **Power-of-Two Capacity**: The constant `N` must be a power of two (e.g., 64, 1024, 4096).
2. **Data Type Bounds**: Elements `T` must implement `Copy + Default`.
   - `Copy`: Required for zero-copy memory transfers.
   - `Default`: Required for safe array initialization.

## ⚖️ License

This project is licensed under the MIT License - see the LICENSE file for details (or assume MIT if not specified).

---
*Built for speed. Optimized for Rust.*
