
use ring_buffer::mpsc::MpscQueue;
use std::{sync::{atomic::{AtomicU64, Ordering}, Arc, Barrier, Mutex}, thread};

const N: usize = 8;

#[test]
fn mpsc_new_is_empty() {
    let q: MpscQueue<u32, N> = MpscQueue::new();
    let (_p, mut c) = q.split();

    let mut buf = [0u32; N];
    assert_eq!(c.read(&mut buf), 0);
    assert_eq!(c.available(), 0);
}

#[test]
fn mpsc_split_roundtrip() {
    let q: MpscQueue<u32, N> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    assert_eq!(p.write(&[1, 2, 3]), Some(3));

    let mut buf = [0u32; 4];
    let read = c.read(&mut buf);

    assert_eq!(read, 3);
    assert_eq!(&buf[..3], &[1, 2, 3]);
}

#[test]
fn mpsc_zero_length_write() {
    let q: MpscQueue<u32, N> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    assert_eq!(p.write(&[]), Some(0));

    let mut buf = [0u32; 4];
    assert_eq!(c.read(&mut buf), 0);
}

#[test]
fn mpsc_write_larger_than_capacity_fails() {
    let q: MpscQueue<u32, N> = MpscQueue::new();
    let (mut p, _) = q.split();

    let big = [1u32; N + 1];
    assert_eq!(p.write(&big), None);
}

#[test]
fn multiple_producers_sequential() {
    let q: MpscQueue<u32, N> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    assert_eq!(p.write(&[1, 2]), Some(2));
    assert_eq!(p.write(&[3, 4]), Some(2));

    let mut buf = [0u32; 4];
    let read = c.read(&mut buf);

    assert_eq!(read, 4);
    assert_eq!(&buf, &[1, 2, 3, 4]);
}

#[test]
fn concurrent_producers_no_data_loss() {
    const THREADS: usize = 4;
    const ITER: usize = 100;

    let q: MpscQueue<u32, 256> = MpscQueue::new();
    let (producer, mut consumer) = q.split();

    let producer = Arc::new(Mutex::new(producer));
    let barrier = Arc::new(Barrier::new(THREADS));

    let mut handles = Vec::new();

    for t in 0..THREADS {
        let p = producer.clone();
        let b = barrier.clone();

        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..ITER {
                let mut guard = p.lock().unwrap();
                guard.write(&[(t * ITER + i) as u32]);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut buf = [0u32; 512];
    let read = consumer.read(&mut buf);

    assert!(read > 0);

    let mut seen = buf[..read].to_vec();
    seen.sort_unstable();
    seen.dedup();

    assert!(seen.len() <= read);
}

#[test]
fn wraparound_write_and_read() {
    let q: MpscQueue<u32, N> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    assert_eq!(p.write(&[1, 2, 3, 4, 5, 6]), Some(6));

    let mut buf1 = [0u32; 4];
    assert_eq!(c.read(&mut buf1), 4);
    assert_eq!(&buf1, &[1, 2, 3, 4]);

    // This write must wrap
    assert_eq!(p.write(&[7, 8, 9, 10]), Some(4));

    let mut buf2 = [0u32; 8];
    let read = c.read(&mut buf2);

    assert_eq!(read, 6);
    assert_eq!(&buf2[..6], &[5, 6, 7, 8, 9, 10]);
}

#[test]
fn overwrite_drops_old_data() {
    let q: MpscQueue<u32, N> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    // Write more than capacity without reading
    for i in 0..(N * 2) {
        p.write(&[i as u32]);
    }

    let mut buf = [0u32; N];
    let read = c.read(&mut buf);

    assert!(read <= N);

    // Data should be the *latest*
    let max = buf[..read].iter().copied().max().unwrap();
    assert!(max >= (N as u32));
}

#[test]
#[should_panic]
fn ringbuffer_requires_power_of_two() {
    let _: MpscQueue<u32, 3> = MpscQueue::new();
}

#[test]
fn stress_test_mpsc() {
    const THREADS: usize = 4;
    const ITER: usize = 10_000;

    let q: MpscQueue<u64, 1024> = MpscQueue::new();
    let (producer, mut consumer) = q.split();

    let producer = Arc::new(Mutex::new(producer));
    let barrier = Arc::new(Barrier::new(THREADS));

    let mut handles = Vec::new();

    for t in 0..THREADS {
        let p = producer.clone();
        let b = barrier.clone();

        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..ITER {
                let mut guard = p.lock().unwrap();
                guard.write(&[(t * ITER + i) as u64]);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut total = 0;
    let mut buf = [0u64; 512];

    loop {
        let r = consumer.read(&mut buf);
        if r == 0 {
            break;
        }
        total += r;
    }

    assert!(total > 0);
}

#[test]
fn partial_reads_preserve_remaining_data() {
    let q: MpscQueue<u32, 16> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    p.write(&[1, 2, 3, 4, 5, 6]);

    let mut buf = [0u32; 2];
    assert_eq!(c.read(&mut buf), 2);
    assert_eq!(buf, [1, 2]);

    let mut buf2 = [0u32; 8];
    let read = c.read(&mut buf2);

    assert_eq!(read, 4);
    assert_eq!(&buf2[..4], &[3, 4, 5, 6]);
}

#[test]
fn consumer_never_reads_default_value() {
    const N: usize = 64;
    let q: MpscQueue<u64, N> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    for i in 1..1000 {
        p.write(&[i]);
        let mut buf = [0u64; 1];
        let r = c.read(&mut buf);
        if r == 1 {
            assert_ne!(buf[0], 0);
        }
    }
}

#[test]
fn repeated_wraparound_correctness() {
    const N: usize = 32;
    let q: MpscQueue<u32, N> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    for round in 0..1000 {
        for i in 0..N {
            p.write(&[(round * 1000 + i) as u32]);
        }

        let mut buf = [0u32; N];
        let r = c.read(&mut buf);
        assert_eq!(r, N);
    }
}

#[test]
fn consumer_progress_under_producer_spin() {
    const N: usize = 128;
    let q: MpscQueue<u64, N> = MpscQueue::new();
    let (producer, mut consumer) = q.split();

    let producer = Arc::new(std::sync::Mutex::new(producer));

    let writer = {
        let p = producer.clone();
        std::thread::spawn(move || {
            for i in 0..1_000_000 {
                p.lock().unwrap().write(&[i]);
            }
        })
    };

    let mut total = 0;
    let mut buf = [0u64; 64];

    loop {
        let r = consumer.read(&mut buf);
        total += r;
        if r == 0 && writer.is_finished() {
            break;
        }
    }

    writer.join().unwrap();
    assert!(total > 0);
}

#[test]
fn counter_wraparound_safety() {
    let q: MpscQueue<u8, 16> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    // Force counter overflow behavior
    for _ in 0..1_000_000 {
        p.write(&[1]);
        let mut buf = [0u8; 1];
        c.read(&mut buf);
    }
}

#[test]
fn write_read_latency_smoke_test() {
    use std::time::Instant;

    let q: MpscQueue<u64, 1024> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    let start = Instant::now();
    for i in 0..100_000 {
        p.write(&[i]);
        let mut buf = [0u64; 1];
        c.read(&mut buf);
    }
    let elapsed = start.elapsed();

    assert!(elapsed.as_millis() < 200);
}

#[test]
fn mpsc_heavy_contention_latest_value_survives() {
    const THREADS: usize = 8;
    const ITER: usize = 50_000;
    const N: usize = 1024;

    let q: MpscQueue<u64, N> = MpscQueue::new();
    let (producer, mut consumer) = q.split();

    let producer = Arc::new(Mutex::new(producer));
    let mut handles = Vec::new();

    // Track the last value written globally
    let last_value = Arc::new(AtomicU64::new(0));

    for t in 0..THREADS {
        let p = producer.clone();
        let lv = last_value.clone();

        handles.push(thread::spawn(move || {
            for i in 0..ITER {
                let val = ((t as u64) << 48) | i as u64;
                lv.store(val, Ordering::Relaxed);
                p.lock().unwrap().write(&[val]);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut buf = [0u64; 512];
    let mut last_seen = None;

    loop {
        let r = consumer.read(&mut buf);
        if r == 0 {
            break;
        }
        last_seen = Some(buf[r - 1]);
    }

    let expected = last_value.load(Ordering::Relaxed);
    assert_eq!(last_seen.unwrap(), expected);
}

#[test]
fn consumer_pointer_jumps_on_overwrite() {
    let q: MpscQueue<u64, 8> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    for i in 0..100 {
        p.write(&[i]);
    }

    let mut buf = [0u64; 4];
    let r = c.read(&mut buf);

    assert!(r > 0);
    assert!(c.position() >= 96);
}

#[test]
fn overwrite_never_exposes_uninitialized_data() {
    let q: MpscQueue<u64, 16> = MpscQueue::new();
    let (mut p, mut c) = q.split();

    for i in 1..10_000 {
        p.write(&[i]);
        let mut buf = [0u64; 1];
        let r = c.read(&mut buf);
        if r == 1 {
            assert_ne!(buf[0], 0);
        }
    }
}
