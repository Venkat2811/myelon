//! Layout overhead validation — measures producer-create and attach-path cost.
//!
//! Run: cargo bench -p myelon-bench --bench layout_validation

use crate::cli::layout;
use crate::infra::events::BenchEvent;
use crate::infra::output::report::{self, LayoutTargetMeasurement};
use crate::infra::output::reporting;
use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, MmapConsumer, MmapCursor, MmapProducer,
    MmapTransportLayout, SharedCursor,
};
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MmapFramedTransportConsumer,
    MmapFramedTransportProducer, MyelonWaitStrategy,
};
use myelon::typed_transport::{MmapTypedConsumer, MmapTypedProducer, TypedConsumer, TypedProducer};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const ITERATIONS: usize = 50;
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

pub fn run_main() {
    let raw_args: Vec<String> = std::env::args().collect();
    let args = match crate::infra::apply_timeout_arg(&raw_args) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("layout_validation failed: {error}");
            std::process::exit(1);
        }
    };
    let output_args = reporting::ReportOutputArgs::from_args(&args);
    let _log = crate::infra::output::log::BenchLog::default_capacity("layout_validation");
    let mut all_pass = true;
    let mut targets: Vec<LayoutTargetMeasurement> = Vec::new();

    macro_rules! record {
        ($spec:expr, $avg:expr) => {{
            let measurement = LayoutTargetMeasurement::new(&$spec, $avg);
            let pass = measurement.pass;
            if !pass {
                all_pass = false;
            }
            targets.push(measurement);
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

        record!(layout::SHM_RING_PRODUCER_CREATE, avg_ns);
    }

    // SHM ring attach
    {
        let segment = unique_shm_segment("lv_shm");
        let _producer = build_shared_single_producer::<Event>(&segment, 1024)
            .build_producer(Event::default)
            .expect("create shm ring");
        let _coordination =
            SharedCursor::new(&format!("{segment}_cr"), 0).expect("create shm readiness cursor");

        let avg_ns = measure_avg_ns(
            || {
                let _consumer = attach_shared_consumer::<Event>(&segment, 1024)
                    .build_consumer()
                    .expect("attach");
            },
            ITERATIONS,
        );

        record!(layout::SHM_RING_ATTACH, avg_ns);
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

        record!(layout::SHM_CURSOR_ATTACH, avg_ns);
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

        record!(layout::MMAP_RING_PRODUCER_CREATE, avg_ns);
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

        record!(layout::MMAP_RING_ATTACH, avg_ns);

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

        record!(layout::MMAP_CURSOR_ATTACH, avg_ns);

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

        record!(layout::FRAMED_SHM_PRODUCER_CREATE, avg_ns);
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

        record!(layout::FRAMED_SHM_CONSUMER_ATTACH, avg_ns);
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

        record!(layout::FRAMED_MMAP_PRODUCER_CREATE, avg_ns);
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

        record!(layout::FRAMED_MMAP_CONSUMER_ATTACH, avg_ns);

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

        record!(layout::TYPED_SHM_PRODUCER_CREATE, avg_ns);
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

        record!(layout::TYPED_SHM_CONSUMER_ATTACH, avg_ns);
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

        record!(layout::TYPED_MMAP_PRODUCER_CREATE, avg_ns);
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

        record!(layout::TYPED_MMAP_CONSUMER_ATTACH, avg_ns);

        let _ = std::fs::remove_dir_all(&root);
    }

    let report =
        report::ReportBundle::from_layout_targets("layout_validation", ITERATIONS, &targets);
    report::emit_report(
        &report,
        &output_args,
        Some(reporting::ReportView::Summary),
        Some(reporting::ReportView::Tree),
        None,
    );

    if !all_pass {
        std::process::exit(1);
    }
}
