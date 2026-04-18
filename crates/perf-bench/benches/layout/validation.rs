//! Layout overhead validation — measures producer-create and attach-path cost.
//!
//! Run: cargo bench -p myelon-bench --bench layout_validation

use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, MmapConsumer, MmapCursor, MmapProducer,
    MmapTransportLayout, SharedCursor,
};
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MmapFramedTransportConsumer,
    MmapFramedTransportProducer, MyelonWaitStrategy,
};
use myelon::typed_transport::{
    MmapTypedConsumer, MmapTypedProducer, TypedConsumer, TypedProducer,
};
use perf_bench::events::BenchEvent;
use perf_bench::reporting;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tabled::{settings::Style, Table, Tabled};

const ITERATIONS: usize = 50;
// SHM ring attach is expensive on macOS (~1.5s per attach) due to
// shared_memory crate's segment open + mmap. This budget is for the
// measurement, not a regression target. The existing disruptor-mp
// layout_validation bench has the same observation.
const BUDGET_NS_SHM_CREATE: u64 = 2_000_000_000; // 2s — informational, not a gate
const BUDGET_NS_SHM_ATTACH: u64 = 2_000_000_000; // 2s — informational, not a gate
const BUDGET_NS_MMAP_CREATE: u64 = 5_000_000;
const BUDGET_NS_MMAP_ATTACH: u64 = 500_000;
const BUDGET_NS_MYELON_SHM_CREATE: u64 = 2_000_000_000;
const BUDGET_NS_MYELON_MMAP_CREATE: u64 = 10_000_000;
const BUDGET_NS_MYELON_ATTACH: u64 = 500_000;
const BUDGET_NS_CURSOR: u64 = 500_000;
static NEXT_LAYOUT_ID: AtomicU64 = AtomicU64::new(0);

type Event = BenchEvent<128>;
type Frame = FixedFrame<{ 64 * 1024 - 12 }>;

fn measure_avg_ns<F: FnMut()>(mut f: F, iterations: usize) -> u64 {
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    start.elapsed().as_nanos() as u64 / iterations as u64
}

fn next_layout_id() -> u64 {
    NEXT_LAYOUT_ID.fetch_add(1, Ordering::Relaxed)
}

fn unique_shm_segment(prefix: &str) -> String {
    disruptor_mp::portable_shm_segment_name(&format!(
        "{prefix}_{}_{}",
        std::process::id(),
        next_layout_id()
    ))
}

fn unique_mmap_layout(prefix: &str) -> (std::path::PathBuf, MmapTransportLayout) {
    let id = next_layout_id();
    let root = std::env::temp_dir().join(format!("{prefix}_{}_{}", std::process::id(), id));
    let segment = format!("{prefix}_{id}");
    let layout = MmapTransportLayout::new(root.clone(), segment).expect("layout");
    (root, layout)
}

#[derive(Clone, Debug, Serialize)]
struct LayoutTargetResult {
    target: String,
    avg_ns: u64,
    budget_ns: u64,
    pass: bool,
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

impl From<&LayoutTargetResult> for LayoutRow {
    fn from(result: &LayoutTargetResult) -> Self {
        Self {
            target: result.target.clone(),
            avg_ns: result.avg_ns.to_string(),
            budget: result.budget_ns.to_string(),
            result: if result.pass {
                "PASS".into()
            } else {
                "FAIL".into()
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct LayoutValidationReport {
    metadata: reporting::BenchMetadata,
    iterations: usize,
    all_pass: bool,
    targets: Vec<LayoutTargetResult>,
}

fn emit_layout_report(
    report: &LayoutValidationReport,
    output_args: &reporting::ReportOutputArgs,
) -> std::io::Result<()> {
    if output_args.json_mode {
        println!(
            "{}",
            serde_json::to_string_pretty(report).expect("serialize layout report")
        );
    } else {
        let rows: Vec<LayoutRow> = report.targets.iter().map(LayoutRow::from).collect();
        println!("=== Layout Validation Benchmark ===");
        println!("Iterations per target: {}\n", report.iterations);
        println!("{}", Table::new(rows).with(Style::modern()));
        if report.all_pass {
            println!("All targets PASS.");
        } else {
            println!("Some targets FAILED!");
        }
    }

    if let Some(path) = output_args.json_out.as_deref() {
        std::fs::write(
            path,
            serde_json::to_string_pretty(report).expect("serialize layout report"),
        )?;
        eprintln!("JSON written to {path}");
    }
    if let Ok(path) = std::env::var("MYELON_BENCH_JSON_OUT") {
        std::fs::write(
            &path,
            serde_json::to_string_pretty(report).expect("serialize layout report"),
        )?;
    }

    Ok(())
}

fn main() {
    let raw_args: Vec<String> = std::env::args().collect();
    let args = match perf_bench::harness::apply_timeout_arg(&raw_args) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("layout_validation failed: {error}");
            std::process::exit(1);
        }
    };
    let output_args = reporting::ReportOutputArgs::from_args(&args);
    let _log = perf_bench::bench_log::BenchLog::default_capacity("layout_validation");
    let mut all_pass = true;
    let mut targets: Vec<LayoutTargetResult> = Vec::new();

    macro_rules! record {
        ($name:expr, $avg:expr, $budget:expr) => {{
            let pass = $avg <= $budget;
            if !pass {
                all_pass = false;
            }
            targets.push(LayoutTargetResult {
                target: $name.to_string(),
                avg_ns: $avg,
                budget_ns: $budget,
                pass,
            });
        }};
    }

    // SHM ring producer create
    {
        let avg_ns = measure_avg_ns(
            || {
                let segment = unique_shm_segment("lv_shm_prod");
                let _producer = build_shared_single_producer::<Event>(&segment, 1024)
                    .build_producer(Event::default)
                    .expect("create shm ring");
            },
            ITERATIONS,
        );

        record!("shm-ring-producer-create", avg_ns, BUDGET_NS_SHM_CREATE);
    }

    // SHM ring attach
    {
        let segment = unique_shm_segment("lv_shm");
        let _producer = build_shared_single_producer::<Event>(&segment, 1024)
            .build_producer(Event::default)
            .expect("create shm ring");

        let avg_ns = measure_avg_ns(
            || {
                let _consumer = attach_shared_consumer::<Event>(&segment, 1024)
                    .build_consumer()
                    .expect("attach");
            },
            ITERATIONS,
        );

        record!("shm-ring-attach", avg_ns, BUDGET_NS_SHM_ATTACH);
    }

    // SHM cursor attach
    {
        let name = unique_shm_segment("lv_cur");
        let _cursor = SharedCursor::new(&name, 0).expect("create shm cursor");

        let avg_ns = measure_avg_ns(
            || {
                let _attached = SharedCursor::attach(&name).expect("attach shm cursor");
            },
            ITERATIONS,
        );

        record!("shm-cursor-attach", avg_ns, BUDGET_NS_CURSOR);
    }

    // mmap ring producer create
    {
        let avg_ns = measure_avg_ns(
            || {
                let (root, layout) = unique_mmap_layout("lv_mmap_prod");
                {
                    let _producer = MmapProducer::<Event>::create(layout, 1024, Event::default)
                        .expect("create mmap ring");
                }
                let _ = std::fs::remove_dir_all(&root);
            },
            ITERATIONS,
        );

        record!("mmap-ring-producer-create", avg_ns, BUDGET_NS_MMAP_CREATE);
    }

    // mmap ring attach
    {
        let (root, layout) = unique_mmap_layout("lv_mmap");
        layout.ensure_directories().expect("dirs");

        let _producer = MmapProducer::<Event>::create(layout.clone(), 1024, Event::default)
            .expect("create mmap ring");

        let avg_ns = measure_avg_ns(
            || {
                let cid = format!("c{}", std::process::id());
                let _consumer =
                    MmapConsumer::<Event>::attach(layout.clone(), 1024, &cid).expect("attach mmap");
            },
            ITERATIONS,
        );

        record!("mmap-ring-attach", avg_ns, BUDGET_NS_MMAP_ATTACH);

        let _ = std::fs::remove_dir_all(&root);
    }

    // mmap cursor attach
    {
        let (root, layout) = unique_mmap_layout("lv_mmap_cur");
        layout.ensure_directories().expect("dirs");

        let _cursor =
            MmapCursor::new(layout.readiness_cursor_config(true), 0).expect("create mmap cursor");

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

    // Framed SHM producer create
    {
        let avg_ns = measure_avg_ns(
            || {
                let segment = unique_shm_segment("lv_framed_prod");
                let _producer = FramedTransportProducer::<Frame>::create(&segment, 64)
                    .expect("create framed shm producer");
            },
            ITERATIONS,
        );

        record!(
            "framed-shm-producer-create",
            avg_ns,
            BUDGET_NS_MYELON_SHM_CREATE
        );
    }

    // Framed SHM consumer attach
    {
        let segment = unique_shm_segment("lv_frc");
        let _producer = FramedTransportProducer::<Frame>::create(&segment, 64)
            .expect("create framed for attach");

        let avg_ns = measure_avg_ns(
            || {
                let _consumer = FramedTransportConsumer::<Frame>::attach(
                    &segment,
                    64,
                    MyelonWaitStrategy::BusySpin,
                )
                .expect("attach framed");
            },
            ITERATIONS,
        );

        record!(
            "framed-shm-consumer-attach",
            avg_ns,
            BUDGET_NS_MYELON_ATTACH
        );
    }

    // Framed mmap producer create
    {
        let avg_ns = measure_avg_ns(
            || {
                let (root, layout) = unique_mmap_layout("lv_framed_mmap_prod");
                {
                    let _producer = MmapFramedTransportProducer::<Frame>::create(layout, 64)
                        .expect("create framed mmap producer");
                }
                let _ = std::fs::remove_dir_all(&root);
            },
            ITERATIONS,
        );

        record!(
            "framed-mmap-producer-create",
            avg_ns,
            BUDGET_NS_MYELON_MMAP_CREATE
        );
    }

    // Framed mmap consumer attach
    {
        let (root, layout) = unique_mmap_layout("lv_framed_mmap");
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

        record!(
            "framed-mmap-consumer-attach",
            avg_ns,
            BUDGET_NS_MYELON_ATTACH
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    // TypedTransport SHM producer create
    {
        let avg_ns = measure_avg_ns(
            || {
                let segment = unique_shm_segment("lv_typed_prod");
                let _producer = TypedProducer::<Frame>::create(&segment, 64)
                    .expect("create typed shm producer");
            },
            ITERATIONS,
        );

        record!(
            "typed-shm-producer-create",
            avg_ns,
            BUDGET_NS_MYELON_SHM_CREATE
        );
    }

    // TypedTransport SHM consumer attach
    {
        let segment = unique_shm_segment("lv_typed");
        let _producer =
            TypedProducer::<Frame>::create(&segment, 64).expect("create typed shm producer");

        let avg_ns = measure_avg_ns(
            || {
                let _consumer =
                    TypedConsumer::<Frame>::attach(&segment, 64, MyelonWaitStrategy::BusySpin)
                        .expect("attach typed shm");
            },
            ITERATIONS,
        );

        record!("typed-shm-consumer-attach", avg_ns, BUDGET_NS_MYELON_ATTACH);
    }

    // TypedTransport mmap producer create
    {
        let avg_ns = measure_avg_ns(
            || {
                let (root, layout) = unique_mmap_layout("lv_typed_mmap_prod");
                {
                    let _producer = MmapTypedProducer::<Frame>::create(layout, 64)
                        .expect("create typed mmap producer");
                }
                let _ = std::fs::remove_dir_all(&root);
            },
            ITERATIONS,
        );

        record!(
            "typed-mmap-producer-create",
            avg_ns,
            BUDGET_NS_MYELON_MMAP_CREATE
        );
    }

    // TypedTransport mmap consumer attach
    {
        let (root, layout) = unique_mmap_layout("lv_typed_mmap");
        let _producer = MmapTypedProducer::<Frame>::create(layout.clone(), 64)
            .expect("create typed mmap producer");

        let avg_ns = measure_avg_ns(
            || {
                let cid = format!("c{}", std::process::id());
                let _consumer = MmapTypedConsumer::<Frame>::attach(
                    layout.clone(),
                    64,
                    &cid,
                    MyelonWaitStrategy::BusySpin,
                )
                .expect("attach typed mmap");
            },
            ITERATIONS,
        );

        record!(
            "typed-mmap-consumer-attach",
            avg_ns,
            BUDGET_NS_MYELON_ATTACH
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    let report = LayoutValidationReport {
        metadata: reporting::BenchMetadata::capture(),
        iterations: ITERATIONS,
        all_pass,
        targets,
    };
    emit_layout_report(&report, &output_args).expect("emit layout report");

    if !report.all_pass {
        std::process::exit(1);
    }
}
