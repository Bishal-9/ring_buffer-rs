use {
    ring_buffer::mpmc::MpmcQueue,
    std::thread
};

#[test]
fn mpmc_basic_write_read() {
    const N: usize = 8;
    let queue: MpmcQueue<u64, N> = MpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    let data = [1, 2, 3, 4];
    assert_eq!(producer.write(&data), Some(4));

    let mut out = [0u64; 4];
    let read = consumer.read(&mut out);

    assert_eq!(read, 4);
    assert_eq!(out, data);
}

#[test]
fn mpmc_wraparound() {
    const N: usize = 8;
    let queue: MpmcQueue<u64, N> = MpmcQueue::new();
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
fn mpmc_consumer_falling_behind() {
    const N: usize = 8;
    let queue: MpmcQueue<u64, N> = MpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    // Write more than capacity in small chunks
    for i in 0..12u64 {
        assert!(producer.write(&[i]).is_some());
    }

    let mut out = [0u64; N];
    let read = consumer.read(&mut out);

    assert_eq!(read, 1);
    assert_eq!(out, [11, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn mpmc_multiple_consumers_independent_positions() {
    const N: usize = 16;
    let queue: MpmcQueue<u64, N> = MpmcQueue::new();
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
fn mpmc_concurrent_stress() {

    const N: usize = 1024;
    const COUNT: usize = 50_000;

    let queue: MpmcQueue<u64, N> = MpmcQueue::new();
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
                        assert!(*v >= last, "v: {} last: {}", *v, last);
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
fn mpmc_available_space_and_data() {
    const N: usize = 8;
    let queue: MpmcQueue<u64, N> = MpmcQueue::new();
    let (mut producer, mut consumer) = queue.split();

    assert_eq!(producer.available_space(), N);
    assert_eq!(consumer.available(), 0);

    producer.write(&[1, 2, 3, 4]).unwrap();

    assert_eq!(producer.available_space(), N);
    assert_eq!(consumer.available(), 4);

    let mut out = [0u64; 2];
    consumer.read(&mut out);

    assert_eq!(consumer.available(), 2);
}

#[test]
fn mpmc_high_contention_small_buffer() {
    use std::thread;

    const N: usize = 32;
    const THREADS: usize = 8;
    const COUNT: usize = 10_000;

    let queue: MpmcQueue<u64, N> = MpmcQueue::new();
    let (producer, consumer) = queue.split();

    let mut handles = vec![];

    for t in 0..THREADS {
        let mut prod = producer.clone();
        handles.push(thread::spawn(move || {
            for i in 0..COUNT {
                let v = (t as u64) << 32 | i as u64;
                while prod.write(&[v]).is_none() {}
            }
        }));
    }

    let mut consumers = vec![];
    for _ in 0..THREADS {
        let mut cons = consumer.clone();
        consumers.push(thread::spawn(move || {
            let mut buf = [0u64; 16];
            for _ in 0..COUNT {
                cons.read(&mut buf);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
}

#[test]
fn mpmc_no_deadlock() {
    use std::thread;

    const N: usize = 128;

    let queue: MpmcQueue<u64, N> = MpmcQueue::new();
    let (producer, consumer) = queue.split();

    let mut prod = producer.clone();
    let p = thread::spawn(move || {
        for i in 0..10_000 {
            while prod.write(&[i]).is_none() {}
        }
    });

    let mut cons = consumer.clone();
    let c = thread::spawn(move || {
        let mut buf = [0u64; 32];
        for _ in 0..10_000 {
            cons.read(&mut buf);
        }
    });

    p.join().unwrap();
    c.join().unwrap();
}
