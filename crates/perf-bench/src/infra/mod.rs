//! Shared benchmark harness for multiprocess IPC benchmarks.
//!
//! Eliminates ~1,712 lines of duplicated infrastructure across 16 bench files.
//! Each benchmark implements `IpcBenchmark` for the unique parts (transport setup,
//! producer loop, consumer loop). The harness handles everything else: process
//! spawning, coordination, timing, output collection, reporting.
//!
//! Inspired by criterion's trait-based measurement and divan's declarative output.

pub mod bench;
pub mod child_runner;
pub mod config;
pub mod launch;
pub mod naming;
pub mod output;
pub mod process;

// Re-export commonly used types
pub use bench::{BenchHarness, IpcBenchmark, ScenarioChildren};
pub use child_runner::{
    collect_child_output, dispatch_child_or_exit, maybe_run_child, parse_child_metrics, BenchError,
    BenchRunResult, ChildHandler, ChildRole,
};
pub use config::{
    apply_timeout_arg, bench_timeout_duration, bench_timeout_secs, bench_timeout_secs_or,
    check_deadline, read_env_bool, read_env_string, read_env_u64, read_env_usize, spin_deadline,
    spin_deadline_or,
};
pub use launch::{launch_mmap_group, launch_shm_group, MultiConsumerSpawn};
pub use naming::{
    mmap_layout_from_env, segment_from_env, unique_mmap_root, unique_mmap_segment,
    unique_shm_segment,
};
pub use output::results::{ConsumerOutput, PhaseTiming, ProducerOutput};
pub use process::{collect_output, parse_json_output, spawn_child, wait_timeout, ProcessOutput};

/// Generate a bench `main()` that delegates to `BenchHarness`.
#[macro_export]
macro_rules! myelon_bench_main {
    ($bench:expr) => {
        fn main() {
            $crate::infra::BenchHarness::run(&$bench);
        }
    };
}
pub mod allocation;
pub mod backend;
pub mod competitors;
pub mod coordination;
pub mod events;
pub mod latency;
pub mod repeatability;
