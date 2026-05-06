use myelon::inference::{FixedTopology, InferenceTopologyError, WorkerCount};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let prefix: String = prefix
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(4)
        .collect();
    let pid = std::process::id() % 10_000;
    let suffix = COUNTER.fetch_add(1, Ordering::Relaxed) % 100;
    let name = format!("{prefix}{pid:04}{suffix:02}");
    assert!(
        name.len() <= 14,
        "segment name exceeds macOS budget: {name}"
    );
    name
}

#[test]
fn worker_count_presets_cover_two_to_eight_workers() {
    let cases = [
        (WorkerCount::Two, 2usize),
        (WorkerCount::Three, 3),
        (WorkerCount::Four, 4),
        (WorkerCount::Five, 5),
        (WorkerCount::Six, 6),
        (WorkerCount::Seven, 7),
        (WorkerCount::Eight, 8),
    ];

    for (preset, expected) in cases {
        assert_eq!(preset.as_usize(), expected);
        assert_eq!(WorkerCount::from_usize(expected), Some(preset));
    }

    assert_eq!(WorkerCount::from_usize(1), None);
    assert_eq!(WorkerCount::from_usize(9), None);
}

#[test]
fn fixed_topology_rejects_out_of_range_worker_indexes() {
    let topology = FixedTopology::new(unique_name("ftop"), 128, WorkerCount::Two)
        .with_coordination_timeout(Duration::from_secs(2));

    match topology
        .worker_consumer_id(2)
        .expect_err("worker 2 should be invalid")
    {
        InferenceTopologyError::InvalidWorkerIndex {
            index,
            worker_count,
        } => {
            assert_eq!(index, 2);
            assert_eq!(worker_count, 2);
        }
        other => panic!("unexpected error variant: {other}"),
    }
}

#[test]
fn fixed_topology_generates_deterministic_worker_ids_and_timeout() {
    let topology = FixedTopology::new("sched_demo", 256, WorkerCount::Three)
        .with_coordination_timeout(Duration::from_secs(7));

    assert_eq!(topology.segment_name(), "sched_demo");
    assert_eq!(topology.buffer_size(), 256);
    assert_eq!(topology.worker_count().as_usize(), 3);
    assert_eq!(topology.coordination_timeout(), Duration::from_secs(7));
    assert_eq!(topology.worker_indices().collect::<Vec<_>>(), vec![0, 1, 2]);
    assert_eq!(
        topology
            .worker_consumer_id(0)
            .expect("worker 0 id should be valid"),
        "sched_demo_wk_0"
    );
    assert_eq!(
        topology
            .worker_consumer_id(2)
            .expect("worker 2 id should be valid"),
        "sched_demo_wk_2"
    );
}
