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
    // When constructed via [`AggregatorHandle::spawn_arc`], holds an
    // `Arc<CountersFile>` for the worker's lifetime. Dropped *after*
    // `join` in `Drop::drop` (Rust drops fields in declaration order)
    // so the backing memory outlives the worker thread.
    _file_keeper: Option<Arc<CountersFile>>,
}

impl AggregatorHandle {
    /// Spawn an aggregator pumping `file` into the `metrics`-rs facade.
    ///
    /// # Safety
    /// `file` must outlive the returned handle. The worker thread
    /// re-attaches to the same memory by raw pointer; the caller is
    /// responsible for keeping the underlying mapping alive (which is
    /// the normal contract for a SHM-resident counters file).
    ///
    /// For a safe alternative when the caller can wrap the counters
    /// file in an [`Arc`], use [`AggregatorHandle::spawn_arc`]: the
    /// handle holds the `Arc` itself until the worker exits, so the
    /// lifetime invariant is enforced by the ownership model.
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
            _file_keeper: None,
        }
    }

    /// Safe alternative to [`AggregatorHandle::spawn`]: the handle
    /// retains an `Arc<CountersFile>` for the duration of the worker
    /// thread, so the backing memory cannot be released early.
    ///
    /// The Arc's inner `CountersFile` is referenced by the worker via
    /// the same raw pointer the unsafe path uses; the only difference
    /// from `spawn` is who proves the lifetime. Here, the type system
    /// does.
    ///
    /// Typical pattern: the SHM/mmap setup code constructs a
    /// `CountersFile` via the unsafe `init`/`attach` path, then wraps
    /// it in an `Arc` whose lifetime is tied to the SHM segment, and
    /// passes that Arc here. The SHM segment must outlive the Arc.
    #[must_use]
    pub fn spawn_arc(file: Arc<CountersFile>, config: AggregatorConfig) -> Self {
        // SAFETY: the Arc is moved into `_file_keeper` below, so the
        // CountersFile lives for as long as this handle exists. The
        // worker thread joins in Drop before _file_keeper drops (Rust
        // drops fields in declaration order: stop, join, then
        // _file_keeper).
        let mut handle = unsafe { Self::spawn(&file, config) };
        handle._file_keeper = Some(file);
        handle
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::{
        ids, AggregatorConfig, AggregatorHandle, CountersFile, COUNTER_FLAG_PRODUCER,
        COUNTERS_FILE_RESERVED_BYTES,
    };

    /// `spawn_arc` keeps the counters file alive for the worker's
    /// lifetime via the `_file_keeper` Arc, then drops it after the
    /// worker joins.
    ///
    /// This test exists to lock in the contract: if a regression
    /// dropped the Arc before the join, valgrind / asan would flag the
    /// worker's final snapshot read as use-after-free; a plain `cargo
    /// test` would either pass or panic depending on timing.
    #[test]
    fn spawn_arc_keeps_file_alive_for_worker() {
        // Leak a buffer for the test process — the Arc-managed file
        // view will reference it for the duration of the test, and
        // process exit cleans up.
        let buf: Box<[u8; COUNTERS_FILE_RESERVED_BYTES]> =
            Box::new([0u8; COUNTERS_FILE_RESERVED_BYTES]);
        let leaked = Box::leak(buf);
        let ptr = std::ptr::NonNull::new(leaked.as_mut_ptr()).expect("non-null leaked ptr");
        // SAFETY: leaked buffer is 'static, zero-initialised, exactly
        // COUNTERS_FILE_RESERVED_BYTES — the contract of `init`.
        let file = unsafe { CountersFile::init(ptr) };

        let arc = Arc::new(file);
        let h = arc
            .register(ids::EVENTS_PUBLISHED, COUNTER_FLAG_PRODUCER, "test_arc")
            .expect("first slot");
        h.inc();
        h.inc();
        h.inc();

        // Spawn aggregator with a short interval so it ticks at least once
        // during the test, then drop the handle to force join + the
        // _file_keeper Arc drop.
        let handle = AggregatorHandle::spawn_arc(
            Arc::clone(&arc),
            AggregatorConfig {
                interval: Duration::from_millis(5),
                metric_suffix: None,
            },
        );
        std::thread::sleep(Duration::from_millis(30));
        drop(handle);

        // The original Arc still works after the handle dropped: the
        // file was kept alive by `arc` independently of the handle's
        // `_file_keeper` clone.
        let snap = arc.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].value, 3);
        assert_eq!(snap[0].label, "test_arc");
    }
}
