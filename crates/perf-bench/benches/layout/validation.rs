//! Layout overhead validation — measures attach-path cost for SHM and mmap.
//!
//! Run: cargo bench -p myelon-bench --bench layout_validation

use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, MmapConsumer, MmapCursor,
    MmapProducer, MmapTransportLayout, SharedCursor,
};
use perf_bench::events::BenchEvent;
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MmapFramedTransportConsumer,
    MmapFramedTransportProducer, MyelonWaitStrategy,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tabled::{Table, Tabled, settings::Style};

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

#[derive(Tabled)]
struct LayoutRow {
    #[tabled(rename = "Target")]
    target: String,
    #[tabled(rename = "Avg (ns/op)")]
    avg_ns: String,
    #[tabled(rename = "Budget (ns)")]
    budget: String,
    #[tabled(rename = "Result")]
    result: String,
}

fn main() {
    let _log = perf_bench::bench_log::BenchLog::default_capacity("layout_validation");
    let mut all_pass = true;
    let mut rows: Vec<LayoutRow> = Vec::new();

    macro_rules! record {
        ($name:expr, $avg:expr, $budget:expr) => {{
            let pass = $avg <= $budget;
            if !pass { all_pass = false; }
            rows.push(LayoutRow {
                target: $name.to_string(),
                avg_ns: format!("{}", $avg),
                budget: format!("{}", $budget),
                result: if pass { "PASS".into() } else { "FAIL".into() },
            });
        }};
    }

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

        record!("shm-ring-attach", avg_ns, BUDGET_NS_SHM);
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

        record!("shm-cursor-attach", avg_ns, BUDGET_NS_CURSOR);
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

        record!("mmap-ring-attach", avg_ns, BUDGET_NS_MMAP);

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

        record!("mmap-cursor-attach", avg_ns, BUDGET_NS_CURSOR);

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

        record!("framed-shm-consumer-attach", avg_ns, BUDGET_NS_FRAMED);
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

        record!("framed-mmap-consumer-attach", avg_ns, BUDGET_NS_FRAMED);

        let _ = std::fs::remove_dir_all(&root);
    }

    println!("=== Layout Validation Benchmark ===");
    println!("Iterations per target: {ITERATIONS}\n");
    println!("{}", Table::new(rows).with(Style::modern()));
    if all_pass {
        println!("All targets PASS.");
    } else {
        println!("Some targets FAILED!");
        std::process::exit(1);
    }
}
