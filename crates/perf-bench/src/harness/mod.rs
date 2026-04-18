//! Shared benchmark harness for multiprocess IPC benchmarks.
//!
//! Eliminates ~1,712 lines of duplicated infrastructure across 16 bench files.
//! Each benchmark implements `IpcBenchmark` for the unique parts (transport setup,
//! producer loop, consumer loop). The harness handles everything else: process
//! spawning, coordination, timing, output collection, reporting.
//!
//! Inspired by criterion's trait-based measurement and divan's declarative output.

pub mod env_config;
pub mod naming;
pub mod output;
pub mod process;
pub mod runner;
pub mod r#trait;

// Re-export commonly used types
pub use env_config::{read_env_bool, read_env_string, read_env_u64, read_env_usize};
pub use naming::{
    mmap_layout_from_env, segment_from_env, unique_mmap_root, unique_mmap_segment,
    unique_shm_segment,
};
pub use output::{ConsumerOutput, PhaseTiming, ProducerOutput};
pub use process::{collect_output, parse_json_output, spawn_child, wait_timeout, ProcessOutput};
pub use r#trait::{BenchHarness, IpcBenchmark, ScenarioChildren};
pub use runner::{
    collect_child_output, dispatch_child_or_exit, maybe_run_child, parse_child_metrics, BenchError,
    BenchRunResult, ChildHandler, ChildRole,
};

/// Generate a bench `main()` that delegates to `BenchHarness`.
#[macro_export]
macro_rules! myelon_bench_main {
    ($bench:expr) => {
        fn main() {
            $crate::harness::BenchHarness::run(&$bench);
        }
    };
}
