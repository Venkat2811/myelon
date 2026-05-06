//! Aggregator thread that pumps a `CountersFile` to the `metrics`-rs
//! facade on a configurable interval.
//!
//! Hot writers never touch this code; the aggregator runs on its own
//! thread, reads the counters file's `snapshot()` periodically, and
//! emits one `metrics::counter!()` call per slot. Downstream backends
//! (`metrics-exporter-prometheus`, OTLP, …) attach via the standard
//! `metrics::set_global_recorder` flow — this aggregator doesn't pick
//! a backend, it goes through the facade.
//!
//! Cost model: `O(n_slots)` atomic loads per tick (default 100 ms),
//! plus one `metrics::counter!()` registration per unique label per
//! tick. None of this runs on the producer/consumer hot path.

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::observability::{CountersFile, COUNTERS_FILE_RESERVED_BYTES};
use std::ptr::NonNull;

/// Configuration for [`AggregatorHandle`].
#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// Sleep between snapshots. Default 100 ms.
    pub interval: Duration,
    /// Optional name suffix appended to every metric name. Useful when
    /// multiple rings live in the same process and need to be told
    /// apart in the metrics backend (e.g. `"_engine"`).
    pub metric_suffix: Option<String>,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(100),
            metric_suffix: None,
        }
    }
}

/// Runs the aggregator until dropped or [`AggregatorHandle::stop`] is called.
///
/// The handle owns the worker thread; dropping joins it. Internally
/// the aggregator re-attaches to the same memory the writer initialised,
/// so the worker thread holds nothing more than a `usize` address —
/// memory ownership stays with whoever created the `CountersFile`.
pub struct AggregatorHandle {
    stop: Arc<Mutex<bool>>,
    join: Option<JoinHandle<()>>,
}

impl AggregatorHandle {
    /// Spawn an aggregator pumping `file` into the `metrics`-rs facade.
    ///
    /// # Safety
    /// `file` must outlive the returned handle. The worker thread
    /// re-attaches to the same memory by raw pointer; the caller is
    /// responsible for keeping the underlying mapping alive (which is
    /// the normal contract for a SHM-resident counters file).
    pub unsafe fn spawn(file: &CountersFile, config: AggregatorConfig) -> Self {
        let stop = Arc::new(Mutex::new(false));
        let stop_for_worker = Arc::clone(&stop);

        // Capture the address as `usize` so the closure is `Send`.
        // SAFETY (closure):
        //   `addr` points into a mapping the caller guarantees outlives
        //   this handle; we re-attach via `CountersFile::attach`, which
        //   only does relaxed reads.
        let addr = file.base_address();
        let interval = config.interval;
        let suffix = config.metric_suffix.clone();

        let join = std::thread::Builder::new()
            .name("disruptor-mp-aggregator".into())
            .spawn(move || worker_loop(addr, interval, suffix, stop_for_worker))
            .expect("spawn aggregator thread");

        Self {
            stop,
            join: Some(join),
        }
    }

    /// Signal the aggregator thread to exit at the next tick. Joining
    /// is deferred until the handle is dropped.
    pub fn stop(&self) {
        let mut guard = self.stop.lock().unwrap();
        *guard = true;
    }
}

impl Drop for AggregatorHandle {
    fn drop(&mut self) {
        self.stop();
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

fn worker_loop(addr: usize, interval: Duration, suffix: Option<String>, stop: Arc<Mutex<bool>>) {
    // Re-attach in the worker thread. If validation fails we silently
    // exit — the writer hasn't initialised the region yet (or the
    // pointer is bad). The aggregator never panics.
    let ptr = match NonNull::new(addr as *mut u8) {
        Some(p) => p,
        None => return,
    };
    let file = match unsafe { CountersFile::attach(ptr) } {
        Ok(f) => f,
        Err(_) => return,
    };

    loop {
        if *stop.lock().unwrap() {
            break;
        }
        for c in file.snapshot() {
            let name = match &suffix {
                Some(s) => format!("disruptor_mp_{}{}", c.label, s),
                None => format!("disruptor_mp_{}", c.label),
            };
            metrics::counter!(name).absolute(c.value);
        }
        std::thread::sleep(interval);
    }
    // Final flush so the last snapshot is visible to the metrics
    // backend before the process tears down.
    for c in file.snapshot() {
        let name = match &suffix {
            Some(s) => format!("disruptor_mp_{}{}", c.label, s),
            None => format!("disruptor_mp_{}", c.label),
        };
        metrics::counter!(name).absolute(c.value);
    }
    let _ = COUNTERS_FILE_RESERVED_BYTES; // silence unused-import warning when this file is the only `metrics`-feature consumer.
}

// Internal helper. Lives in the module so we can give `CountersFile`
// the `base_address()` method without exposing it more broadly than
// needed.
impl CountersFile {
    /// Address of the underlying region as a `usize`. Used by the
    /// aggregator to ferry the pointer across thread boundaries
    /// without fighting the borrow checker.
    pub(crate) fn base_address(&self) -> usize {
        self.base.as_ptr() as usize
    }
}
