#![cfg(feature = "loom")]

use loom::thread;
use ring_buffer::mpsc::MpscQueue;
use ring_buffer::spsc::SpscQueue;
use ring_buffer::mpmc::MpmcQueue;

#[test]
fn test_spsc_visibility() {
    loom::model(|| {
        let (mut p, mut c) = SpscQueue::<usize, 2>::new().split();

        let producer = thread::spawn(move || {
            p.write(&[42]);
        });

        let mut out = [0; 1];
        loop {
            let read = c.read(&mut out);
            if read > 0 {
                assert_eq!(out[0], 42, "Consumer read stale/uninitialized data");
                break;
            }
            loom::thread::yield_now();
        }
        producer.join().unwrap();
    });
}

#[test]
fn test_mpsc_races() {
    loom::model(|| {
        let (mut p1, mut c) = MpscQueue::<usize, 2>::new().split();
        let mut p2 = p1.clone();

        let t1 = thread::spawn(move || {
            p1.write(&[1]);
        });
        
        let t2 = thread::spawn(move || {
            p2.write(&[2]);
        });

        let mut out = [0; 2];
        let mut seen_1 = false;
        let mut seen_2 = false;
        
        // the Mpsc queue should collect both messages.
        // It could overwrite if slow, but here we only write 1 item per producer 
        // and buffer capacity is 2, so no overwriting will occur natively.
        let mut total_reads = 0;
        loop {
            let read = c.read(&mut out);
            for i in 0..read {
                if out[i] == 1 { seen_1 = true; }
                if out[i] == 2 { seen_2 = true; }
                assert!(out[i] == 1 || out[i] == 2, "Read unexpected data");
            }
            total_reads += read;
            if total_reads >= 2 {
                break;
            }
            loom::thread::yield_now();
        }

        assert!(seen_1 && seen_2, "Lost writes due to MPMC CAS overlap");

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn test_contention_wraparound() {
    loom::model(|| {
        let (mut p, mut c) = MpmcQueue::<usize, 2>::new().split();
        let mut c2 = c.clone();

        let producer = thread::spawn(move || {
            p.write(&[1, 2]); // Fills the buffer
            p.write(&[3]); // Wrap around overwrite!
        });

        let t1 = thread::spawn(move || {
            let mut out = [0; 2];
            let mut total_reads = 0;
            loop {
                let n = c.read(&mut out);
                for i in 0..n {
                    assert!(out[i] != 0, "Read uninitialized data");
                }
                total_reads += n;
                // If it was overwritten, it might read [3, x] or skip items, 
                // but should not read 0. We'll just read 2 times or check values.
                if total_reads >= 2 {
                    break;
                }
                loom::thread::yield_now();
            }
        });

        let t2 = thread::spawn(move || {
            let mut out = [0; 2];
            let mut total_reads = 0;
            loop {
                let n = c2.read(&mut out);
                for i in 0..n {
                    assert!(out[i] != 0, "Read uninitialized data");
                }
                total_reads += n;
                if total_reads >= 2 {
                    break;
                }
                loom::thread::yield_now();
            }
        });

        producer.join().unwrap();
        t1.join().unwrap();
        t2.join().unwrap();
    });
}
