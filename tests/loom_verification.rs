#![cfg(feature = "loom_test")]

use loom::thread;
use ring_buffer::spsc::SpscQueue;
use ring_buffer::spmc::SpmcQueue;
use ring_buffer::mpsc::MpscQueue;
use ring_buffer::mpmc::MpmcQueue;

// The "Pressure" Model: N=2
const CAPACITY: usize = 2;

#[test]
fn formal_spsc_handover() {
    loom::model(|| {
        let (mut p, mut c) = SpscQueue::<usize, CAPACITY>::new().split();

        let t1 = thread::spawn(move || {
            p.write(&[100]);
        });

        let mut out = [0; 1];
        loop {
            if c.read(&mut out) > 0 {
                assert_eq!(out[0], 100, "Inconsistent handover: data corrupted or stale");
                break;
            }
            loom::thread::yield_now();
        }
        t1.join().unwrap();
    });
}

#[test]
fn formal_spmc_race_to_drain() {
    loom::model(|| {
        let (mut p, mut c1) = SpmcQueue::<usize, CAPACITY>::new().split();
        let mut c2 = c1.clone();

        p.write(&[42]); // One item produced

        let t1 = thread::spawn(move || {
            let mut out = [0; 1];
            c1.read(&mut out)
        });

        let t2 = thread::spawn(move || {
            let mut out = [0; 1];
            c2.read(&mut out)
        });

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // INVARIANT CHECK: In a work-stealing queue, only one consumer should get the item.
        // NOTE: In the current BROADCAST design, both will get it. This test exposes the design mismatch.
        assert!(!(r1 > 0 && r2 > 0), "Double-delivery detected: Both consumers read the same item (Broadcast vs Queue)");
    });
}

#[test]
fn formal_mpsc_race_to_fill() {
    loom::model(|| {
        let (mut p1, mut c) = MpscQueue::<usize, CAPACITY>::new().split();
        let mut p2 = p1.clone();

        let t1 = thread::spawn(move || {
            p1.write(&[1]);
        });

        let t2 = thread::spawn(move || {
            p2.write(&[2]);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let mut out = [0; CAPACITY];
        let read = c.read(&mut out);
        
        // INVARIANT CHECK: Buffer capacity is 2. Both writes should be stored if no overwrite occurs.
        assert_eq!(read, 2, "Lost write detected: Producers overwrote each other or data uncommitted");
    });
}

#[test]
fn formal_mpmc_no_overwrite() {
    loom::model(|| {
        let (mut p, mut c) = MpmcQueue::<usize, CAPACITY>::new().split();

        // Fill buffer
        p.write(&[1, 2]);

        let t1 = thread::spawn(move || {
            // This write should either BLOCK or RETURN error if we want "No Overwrite" invariant.
            // Currently it overwrites the unread '1'.
            p.write(&[3]);
        });

        let mut out = [0; 1];
        let read = c.read(&mut out);
        
        // INVARIANT CHECK: Producer should NOT overwrite unread data.
        if read > 0 {
            assert_ne!(out[0], 3, "Safety Violation: Producer overwrote unread data '1' with '3'");
        }
        
        t1.join().unwrap();
    });
}

#[test]
fn formal_drop_consistency() {
    // Current T: Copy + Default doesn't have Drop logic, 
    // but we verify the Arc reference counts remain consistent through Loom's Arc.
    loom::model(|| {
        let queue = MpmcQueue::<usize, CAPACITY>::new();
        let (p, c) = queue.split();
        
        let t1 = thread::spawn(move || {
            let _p_cloned = p;
        });
        let t2 = thread::spawn(move || {
            let _c_cloned = c;
        });
        
        t1.join().unwrap();
        t2.join().unwrap();
    });
}
