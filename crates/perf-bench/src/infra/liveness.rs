//! Required-consumer liveness wiring for the pingpong harness.
//!
//! The bench binary's `--liveness on` flag forwards `MYELON_BENCH_LIVENESS=on`
//! into the env. Each layer reads it via [`liveness_enabled`], and when
//! the flag is on:
//!
//! - calls `enable_required_consumer_liveness` on the constructed
//!   producer with [`liveness_config`] (sensible perf-bench defaults), and
//! - routes its publish calls through `publish_managed` instead of
//!   `publish` so the liveness layer is actually consulted on the hot
//!   path. Without that swap, enabling liveness is a no-op (the
//!   non-managed `publish` ignores the policy).
//!
//! Off by default — the unmanaged path stays the baseline so the
//! comparison between liveness-on and liveness-off is meaningful.

use std::sync::OnceLock;
use std::time::Duration;

use disruptor_mp::{RequiredConsumerFailureAction, RequiredConsumerLivenessConfig};

static LIVENESS_ON: OnceLock<bool> = OnceLock::new();

/// Returns `true` if `MYELON_BENCH_LIVENESS=on` is set in the
/// environment.
///
/// Cached via `OnceLock` so it's safe (and cheap) to call from a
/// hot publish loop. The first call reads the env var; every
/// subsequent call is one atomic load. The env var is read in the
/// orchestrator process (set by `--liveness on` in `pingpong.rs`)
/// and inherited by every spawned child, so the cached value
/// matches everywhere.
#[inline]
pub fn liveness_enabled() -> bool {
    *LIVENESS_ON.get_or_init(|| std::env::var(crate::infra::env::LIVENESS).as_deref() == Ok("on"))
}

/// Build a [`RequiredConsumerLivenessConfig`] tuned for perf-bench timings.
///
/// The library default `progress_timeout` is 250 ms, which
/// is enormous on a hot ring; bench loops process millions of
/// events per second and a healthy consumer should advance every
/// few microseconds. We tighten the timing knobs proportionally so
/// liveness can fire while a bench is in flight, but leave the
/// failure action at `GracefulShutdown` (the default) so the
/// harness sees a clean error rather than blocking.
pub fn liveness_config(consumer_ids: &[&str]) -> RequiredConsumerLivenessConfig {
    let ids: Vec<String> = consumer_ids.iter().map(|s| (*s).to_string()).collect();
    RequiredConsumerLivenessConfig {
        required_consumer_ids: ids,
        startup_wait_timeout: Duration::from_secs(5),
        progress_timeout: Duration::from_millis(50),
        progress_check_interval: Duration::from_millis(2),
        shutdown_grace_period: Duration::from_secs(1),
        failure_action: RequiredConsumerFailureAction::GracefulShutdown,
        alert_hook: None,
    }
}
