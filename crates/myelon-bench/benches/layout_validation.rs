//! Layout overhead validation — measures attach-path cost for SHM and mmap.
//!
//! Run: cargo bench -p myelon-bench --bench layout_validation

use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, MmapConsumer, MmapCursor,
    MmapProducer, MmapTransportLayout, SharedCursor,
};
use myelon_bench::events::BenchEvent;
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MmapFramedTransportConsumer,
    MmapFramedTransportProducer, MyelonWaitStrategy,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const ITERATIONS: usize = 50;
// SHM ring attach is expensive on macOS (~1.5s per attach) due to
// shared_memory crate's segment open + mmap. This budget is for the
// measurement, not a regression target. The existing disruptor-mp
// layout_validation bench has the same observation.
const BUDGET_NS_SHM: u64 = 2_000_000_000; // 2s — informational, not a gate
const BUDGET_NS_MMAP: u64 = 500_000;
const BUDGET_NS_FRAMED: u64 = 500_000;
const BUDGET_NS_CURSOR: u64 = 500_000;

type Event = BenchEvent<128>;
type Frame = FixedFrame<{ 64 * 1024 - 12 }>;

fn measure_avg_ns<F: FnMut()>(mut f: F, iterations: usize) -> u64 {
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    start.elapsed().as_nanos() as u64 / iterations as u64
}

fn main() {
    println!("=== Layout Validation Benchmark ===");
    println!("Iterations per target: {ITERATIONS}");
    println!();
    println!(
        "{:<35} {:>10} {:>10} {:>8}",
        "Target", "Avg ns/op", "Budget", "Result"
    );
    println!("{}", "-".repeat(70));

    let mut all_pass = true;

    // SHM ring attach
    {
        let segment = disruptor_mp::portable_shm_segment_name("lv_shm");
        let _producer = build_shared_single_producer::<Event>(&segment, 1024)
            .build_producer(Event::default)
            .expect("create shm ring");

        let avg_ns = measure_avg_ns(|| {
            let _consumer = attach_shared_consumer::<Event>(&segment, 1024)
                .build_consumer()
                .expect("attach");
        }, ITERATIONS);

        let pass = avg_ns <= BUDGET_NS_SHM;
        if !pass { all_pass = false; }
        println!(
            "{:<35} {:>10} {:>10} {:>8}",
            "shm-ring-attach", avg_ns, BUDGET_NS_SHM, if pass { "PASS" } else { "FAIL" }
        );
    }

    // SHM cursor attach
    {
        let name = disruptor_mp::portable_shm_segment_name("lv_cur");
        let _cursor = SharedCursor::new(&name, 0).expect("create shm cursor");

        let avg_ns = measure_avg_ns(
            || {
                let _attached = SharedCursor::attach(&name).expect("attach shm cursor");
            },
            ITERATIONS,
        );

        let pass = avg_ns <= BUDGET_NS_CURSOR;
        if !pass {
            all_pass = false;
        }
        println!(
            "{:<35} {:>10} {:>10} {:>8}",
            "shm-cursor-attach",
            avg_ns,
            BUDGET_NS_CURSOR,
            if pass { "PASS" } else { "FAIL" }
        );
    }

    // mmap ring create+attach
    {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("lv_mmap_{}_{}", std::process::id(), ts));
        let segment = format!("lv_mmap_{}", ts % 100000);
        let layout = MmapTransportLayout::new(root.clone(), segment).expect("layout");
        layout.ensure_directories().expect("dirs");

        let _producer = MmapProducer::<Event>::create(layout.clone(), 1024, Event::default)
            .expect("create mmap ring");

        let avg_ns = measure_avg_ns(|| {
            let cid = format!("c{}", std::process::id());
            let _consumer = MmapConsumer::<Event>::attach(layout.clone(), 1024, &cid)
                .expect("attach mmap");
        }, ITERATIONS);

        let pass = avg_ns <= BUDGET_NS_MMAP;
        if !pass { all_pass = false; }
        println!(
            "{:<35} {:>10} {:>10} {:>8}",
            "mmap-ring-attach", avg_ns, BUDGET_NS_MMAP, if pass { "PASS" } else { "FAIL" }
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    // mmap cursor attach
    {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("lv_mmap_cur_{}_{}", std::process::id(), ts));
        let segment = format!("lv_mmap_cur_{}", ts % 100000);
        let layout = MmapTransportLayout::new(root.clone(), segment).expect("layout");
        layout.ensure_directories().expect("dirs");

        let _cursor = MmapCursor::new(layout.readiness_cursor_config(true), 0)
            .expect("create mmap cursor");

        let avg_ns = measure_avg_ns(
            || {
                let _attached = MmapCursor::attach(layout.readiness_cursor_config(false))
                    .expect("attach mmap cursor");
            },
            ITERATIONS,
        );

        let pass = avg_ns <= BUDGET_NS_CURSOR;
        if !pass {
            all_pass = false;
        }
        println!(
            "{:<35} {:>10} {:>10} {:>8}",
            "mmap-cursor-attach",
            avg_ns,
            BUDGET_NS_CURSOR,
            if pass { "PASS" } else { "FAIL" }
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    // Framed SHM consumer attach
    {
        let segment = disruptor_mp::portable_shm_segment_name("lv_frc");
        let _producer = FramedTransportProducer::<Frame>::create(&segment, 64)
            .expect("create framed for attach");

        let avg_ns = measure_avg_ns(|| {
            let _consumer = FramedTransportConsumer::<Frame>::attach(
                &segment, 64, MyelonWaitStrategy::BusySpin,
            ).expect("attach framed");
        }, ITERATIONS);

        let pass = avg_ns <= BUDGET_NS_FRAMED;
        if !pass { all_pass = false; }
        println!(
            "{:<35} {:>10} {:>10} {:>8}",
            "framed-shm-consumer-attach", avg_ns, BUDGET_NS_FRAMED, if pass { "PASS" } else { "FAIL" }
        );
    }

    // Framed mmap consumer attach
    {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("lv_framed_mmap_{}_{}", std::process::id(), ts));
        let segment = format!("lv_framed_mmap_{}", ts % 100000);
        let layout = MmapTransportLayout::new(root.clone(), segment).expect("layout");
        let _producer = MmapFramedTransportProducer::<Frame>::create(layout.clone(), 64)
            .expect("create framed mmap producer");

        let avg_ns = measure_avg_ns(
            || {
                let cid = format!("c{}", std::process::id());
                let _consumer = MmapFramedTransportConsumer::<Frame>::attach(
                    layout.clone(),
                    64,
                    &cid,
                    MyelonWaitStrategy::BusySpin,
                )
                .expect("attach framed mmap");
            },
            ITERATIONS,
        );

        let pass = avg_ns <= BUDGET_NS_FRAMED;
        if !pass {
            all_pass = false;
        }
        println!(
            "{:<35} {:>10} {:>10} {:>8}",
            "framed-mmap-consumer-attach",
            avg_ns,
            BUDGET_NS_FRAMED,
            if pass { "PASS" } else { "FAIL" }
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    println!();
    if all_pass {
        println!("All targets PASS.");
    } else {
        println!("Some targets FAILED!");
        std::process::exit(1);
    }
}
