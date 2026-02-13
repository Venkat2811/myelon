use std::mem::{align_of, size_of};

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct BenchmarkEvent<const S: usize> {
    sequence: u64,
    timestamp_ns: u64,
    intended_send_time_ns: u64,
    _pad: [u8; 40],
    payload: [u8; S],
}

impl<const S: usize> Default for BenchmarkEvent<S> {
    fn default() -> Self {
        Self {
            sequence: 0,
            timestamp_ns: 0,
            intended_send_time_ns: 0,
            _pad: [0u8; 40],
            payload: [0u8; S],
        }
    }
}

fn main() {
    println!("verify: disruptor alignment");
    // Check alignment and size for a few sizes
    for &sz in &[64usize, 512, 1024, 2048] {
        match sz {
            64 => check::<64>(),
            512 => check::<512>(),
            1024 => check::<1024>(),
            2048 => check::<2048>(),
            _ => {}
        }
    }
}

fn check<const S: usize>() {
    let a = align_of::<BenchmarkEvent<S>>();
    let s = size_of::<BenchmarkEvent<S>>();
    println!("BenchmarkEvent<{}>: align={} size={}", S, a, s);
    // Allocate initialized events and print element addresses % 64.
    let v = [BenchmarkEvent::<S>::default(); 8];
    let base = v.as_ptr() as usize;
    println!("  base_ptr={:p} mod64={}", v.as_ptr(), base % 64);
    for i in 0..8 {
        let ptr = unsafe { v.as_ptr().add(i) } as usize;
        println!("  elt[{}] ptr mod64={}", i, ptr % 64);
    }
}
