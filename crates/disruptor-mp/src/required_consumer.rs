use crate::Sequence;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Action to take when a required consumer stops making progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RequiredConsumerFailureAction {
    /// Shut the topology down cleanly and surface an error to the producer.
    #[default]
    GracefulShutdown,
}

/// Producer-side alert emitted when a required consumer stops advancing while gating progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredConsumerAlert {
    /// Required consumer ID that has stopped advancing.
    pub consumer_id: String,
    /// Last observed committed sequence for the stalled consumer.
    pub last_sequence: Sequence,
    /// Elapsed stall duration at the time the alert was emitted.
    pub stalled_for: Duration,
}

/// Optional embedding hook invoked when producer-side stall alerting fires.
pub type RequiredConsumerAlertHook = Arc<dyn Fn(&RequiredConsumerAlert) + Send + Sync + 'static>;

/// Producer-side liveness policy for a required consumer set.
#[derive(Clone)]
pub struct RequiredConsumerLivenessConfig {
    /// Stable consumer IDs that must exist and keep advancing.
    pub required_consumer_ids: Vec<String>,
    /// Maximum time to wait for all required consumers at startup.
    pub startup_wait_timeout: Duration,
    /// Maximum time a required consumer may stop advancing while gating producer progress.
    pub progress_timeout: Duration,
    /// Coarse interval for producer-side progress checks while blocked.
    pub progress_check_interval: Duration,
    /// Extra time to allow same-ID recovery after a stall alert before failing the topology.
    pub shutdown_grace_period: Duration,
    /// Action to take after the grace period expires.
    pub failure_action: RequiredConsumerFailureAction,
    /// Optional callback invoked when the producer first detects a required-consumer stall.
    pub alert_hook: Option<RequiredConsumerAlertHook>,
}

impl fmt::Debug for RequiredConsumerLivenessConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequiredConsumerLivenessConfig")
            .field("required_consumer_ids", &self.required_consumer_ids)
            .field("startup_wait_timeout", &self.startup_wait_timeout)
            .field("progress_timeout", &self.progress_timeout)
            .field("progress_check_interval", &self.progress_check_interval)
            .field("shutdown_grace_period", &self.shutdown_grace_period)
            .field("failure_action", &self.failure_action)
            .field("alert_hook", &self.alert_hook.as_ref().map(|_| "Some(..)"))
            .finish()
    }
}

impl RequiredConsumerLivenessConfig {
    /// Create a liveness policy with conservative defaults.
    pub fn new(required_consumer_ids: Vec<String>) -> Self {
        assert!(
            !required_consumer_ids.is_empty(),
            "required_consumer_ids must not be empty"
        );
        Self {
            required_consumer_ids,
            startup_wait_timeout: Duration::from_secs(5),
            progress_timeout: Duration::from_millis(250),
            progress_check_interval: Duration::from_millis(5),
            shutdown_grace_period: Duration::from_secs(1),
            failure_action: RequiredConsumerFailureAction::GracefulShutdown,
            alert_hook: None,
        }
    }

    /// Override the startup wait timeout for required consumer discovery.
    pub fn with_startup_wait_timeout(mut self, timeout: Duration) -> Self {
        assert!(timeout > Duration::ZERO, "startup_wait_timeout must be positive");
        self.startup_wait_timeout = timeout;
        self
    }

    /// Override the stall detection timeout for a required consumer.
    pub fn with_progress_timeout(mut self, timeout: Duration) -> Self {
        assert!(timeout > Duration::ZERO, "progress_timeout must be positive");
        self.progress_timeout = timeout;
        self
    }

    /// Override how often the producer re-checks required-consumer progress while blocked.
    pub fn with_progress_check_interval(mut self, interval: Duration) -> Self {
        assert!(
            interval > Duration::ZERO,
            "progress_check_interval must be positive"
        );
        self.progress_check_interval = interval;
        self
    }

    /// Override the grace period allowed for same-ID recovery after a stall alert.
    pub fn with_shutdown_grace_period(mut self, period: Duration) -> Self {
        self.shutdown_grace_period = period;
        self
    }

    /// Register a callback hook for producer-side stall alerts.
    pub fn with_alert_hook(mut self, hook: RequiredConsumerAlertHook) -> Self {
        self.alert_hook = Some(hook);
        self
    }
}

/// Producer-visible failure when required-consumer liveness cannot be maintained.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RequiredConsumerError {
    /// One or more required consumers never appeared during producer startup.
    #[error("required consumers did not appear before startup timeout: {missing:?}")]
    StartupTimeout {
        /// Required consumer IDs that never appeared before startup timed out.
        missing: Vec<String>,
    },
    /// A required consumer stopped advancing long enough that the producer shut the topology down.
    #[error(
        "required consumer `{consumer_id}` stopped advancing at sequence {last_sequence} for {stalled_for:?}; graceful shutdown triggered"
    )]
    GracefulShutdownTriggered {
        /// Required consumer ID that stopped advancing.
        consumer_id: String,
        /// Last observed committed sequence for the stalled consumer.
        last_sequence: Sequence,
        /// Total time since the stalled consumer last advanced.
        stalled_for: Duration,
    },
}

#[derive(Debug, Clone)]
struct RequiredConsumerProgress {
    last_observed_sequence: Sequence,
    last_progress_at: Instant,
    stall_started_at: Option<Instant>,
    alert_emitted: bool,
}

/// Internal producer-side state for required-consumer liveness.
#[derive(Debug)]
pub(crate) struct RequiredConsumerLivenessState {
    config: RequiredConsumerLivenessConfig,
    consumers: HashMap<String, RequiredConsumerProgress>,
    startup_completed: bool,
    last_check_at: Instant,
    terminal_error: Option<RequiredConsumerError>,
}

impl RequiredConsumerLivenessState {
    pub(crate) fn new(config: RequiredConsumerLivenessConfig) -> Self {
        let now = Instant::now();
        let consumers = config
            .required_consumer_ids
            .iter()
            .cloned()
            .map(|consumer_id| {
                (
                    consumer_id,
                    RequiredConsumerProgress {
                        last_observed_sequence: -1,
                        last_progress_at: now,
                        stall_started_at: None,
                        alert_emitted: false,
                    },
                )
            })
            .collect();
        Self {
            config,
            consumers,
            startup_completed: false,
            last_check_at: now,
            terminal_error: None,
        }
    }

    pub(crate) fn startup_completed(&self) -> bool {
        self.startup_completed
    }

    pub(crate) fn startup_wait_timeout(&self) -> Duration {
        self.config.startup_wait_timeout
    }

    pub(crate) fn required_consumer_ids(&self) -> impl Iterator<Item = &str> {
        self.config
            .required_consumer_ids
            .iter()
            .map(std::string::String::as_str)
    }

    pub(crate) fn terminal_error(&self) -> Option<RequiredConsumerError> {
        self.terminal_error.clone()
    }

    pub(crate) fn mark_startup_completed(&mut self, now: Instant) {
        self.startup_completed = true;
        self.last_check_at = now;
    }

    pub(crate) fn missing_required_consumers(
        &self,
        mut is_present: impl FnMut(&str) -> bool,
    ) -> Vec<String> {
        self.required_consumer_ids()
            .filter(|consumer_id| !is_present(consumer_id))
            .map(str::to_string)
            .collect()
    }

    pub(crate) fn should_check(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_check_at) >= self.config.progress_check_interval
    }

    pub(crate) fn evaluate_blocked(
        &mut self,
        now: Instant,
        producer_sequence: Sequence,
        mut observe_sequence: impl FnMut(&str) -> Option<Sequence>,
    ) -> Option<RequiredConsumerError> {
        if let Some(error) = self.terminal_error() {
            return Some(error);
        }
        if !self.should_check(now) {
            return None;
        }
        self.last_check_at = now;

        for consumer_id in self.config.required_consumer_ids.clone() {
            let observed_sequence = observe_sequence(&consumer_id);
            let progress = self
                .consumers
                .get_mut(&consumer_id)
                .expect("required consumer progress must exist");

            if let Some(sequence) = observed_sequence {
                if sequence > progress.last_observed_sequence {
                    progress.last_observed_sequence = sequence;
                    progress.last_progress_at = now;
                    progress.stall_started_at = None;
                    progress.alert_emitted = false;
                    continue;
                }

                if sequence >= producer_sequence {
                    progress.last_progress_at = now;
                    progress.stall_started_at = None;
                    progress.alert_emitted = false;
                    continue;
                }
            }

            let stalled_for = now.saturating_duration_since(progress.last_progress_at);
            if stalled_for < self.config.progress_timeout {
                continue;
            }

            let stall_started_at = progress.stall_started_at.get_or_insert(now);
            if !progress.alert_emitted {
                let alert = RequiredConsumerAlert {
                    consumer_id: consumer_id.clone(),
                    last_sequence: progress.last_observed_sequence,
                    stalled_for,
                };
                eprintln!(
                    "Required consumer stall detected: consumer_id={consumer_id} last_sequence={} stalled_for={stalled_for:?}",
                    progress.last_observed_sequence
                );
                if let Some(hook) = &self.config.alert_hook {
                    hook(&alert);
                }
                progress.alert_emitted = true;
            }

            if now.saturating_duration_since(*stall_started_at) < self.config.shutdown_grace_period
            {
                continue;
            }

            match self.config.failure_action {
                RequiredConsumerFailureAction::GracefulShutdown => {
                    let error = RequiredConsumerError::GracefulShutdownTriggered {
                        consumer_id: consumer_id.clone(),
                        last_sequence: progress.last_observed_sequence,
                        stalled_for,
                    };
                    self.terminal_error = Some(error.clone());
                    return Some(error);
                }
            }
        }

        None
    }

    pub(crate) fn seed_progress(
        &mut self,
        now: Instant,
        mut observe_sequence: impl FnMut(&str) -> Option<Sequence>,
    ) {
        for consumer_id in self.config.required_consumer_ids.clone() {
            let observed_sequence = observe_sequence(&consumer_id).unwrap_or(-1);
            let progress = self
                .consumers
                .get_mut(&consumer_id)
                .expect("required consumer progress must exist");
            progress.last_observed_sequence = observed_sequence;
            progress.last_progress_at = now;
            progress.stall_started_at = None;
            progress.alert_emitted = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn test_config() -> RequiredConsumerLivenessConfig {
        RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
            .with_progress_timeout(Duration::from_millis(10))
            .with_progress_check_interval(Duration::from_millis(1))
            .with_shutdown_grace_period(Duration::from_millis(5))
    }

    #[test]
    fn reports_missing_required_consumers() {
        let state = RequiredConsumerLivenessState::new(test_config());
        let missing = state.missing_required_consumers(|consumer_id| consumer_id == "c1");
        assert_eq!(missing, vec!["c2".to_string()]);
    }

    #[test]
    fn stalled_consumer_requires_grace_period_before_shutdown() {
        let mut state = RequiredConsumerLivenessState::new(test_config());
        let start = Instant::now();
        state.seed_progress(start, |_| Some(7));
        state.mark_startup_completed(start);

        let alert = state.evaluate_blocked(
            start + Duration::from_millis(11),
            8,
            |consumer_id| if consumer_id == "c1" { Some(7) } else { Some(8) },
        );
        assert!(alert.is_none(), "alert phase should not shutdown immediately");

        let shutdown = state.evaluate_blocked(
            start + Duration::from_millis(17),
            9,
            |consumer_id| if consumer_id == "c1" { Some(7) } else { Some(9) },
        );
        assert!(matches!(
            shutdown,
            Some(RequiredConsumerError::GracefulShutdownTriggered { consumer_id, .. })
            if consumer_id == "c1"
        ));
    }

    #[test]
    fn progress_resets_stall_tracking() {
        let mut state = RequiredConsumerLivenessState::new(test_config());
        let start = Instant::now();
        state.seed_progress(start, |_| Some(3));
        state.mark_startup_completed(start);

        let _ = state.evaluate_blocked(
            start + Duration::from_millis(11),
            4,
            |consumer_id| if consumer_id == "c1" { Some(3) } else { Some(4) },
        );

        let recovered = state.evaluate_blocked(
            start + Duration::from_millis(12),
            5,
            |consumer_id| if consumer_id == "c1" { Some(5) } else { Some(4) },
        );
        assert!(recovered.is_none());

        let still_alive = state.evaluate_blocked(
            start + Duration::from_millis(16),
            5,
            |consumer_id| if consumer_id == "c1" { Some(5) } else { Some(4) },
        );
        assert!(still_alive.is_none(), "progress should reset the stall window");
    }

    #[test]
    fn caught_up_consumers_do_not_trip_stall_detection() {
        let mut state = RequiredConsumerLivenessState::new(test_config());
        let start = Instant::now();
        state.seed_progress(start, |consumer_id| if consumer_id == "c1" { Some(4) } else { Some(0) });
        state.mark_startup_completed(start);

        let alert = state.evaluate_blocked(
            start + Duration::from_millis(17),
            4,
            |consumer_id| if consumer_id == "c1" { Some(4) } else { Some(0) },
        );
        assert!(alert.is_none(), "first blocked observation should only start the grace window");

        let shutdown = state.evaluate_blocked(
            start + Duration::from_millis(23),
            4,
            |consumer_id| if consumer_id == "c1" { Some(4) } else { Some(0) },
        );

        assert!(matches!(
            shutdown,
            Some(RequiredConsumerError::GracefulShutdownTriggered { consumer_id, .. })
            if consumer_id == "c2"
        ));
    }

    #[test]
    fn alert_hook_fires_once_per_stall_window() {
        let alerts: Arc<Mutex<Vec<RequiredConsumerAlert>>> = Arc::new(Mutex::new(Vec::new()));
        let hook_alerts = Arc::clone(&alerts);
        let mut state = RequiredConsumerLivenessState::new(
            test_config().with_alert_hook(Arc::new(move |alert| {
                hook_alerts.lock().unwrap().push(alert.clone());
            })),
        );
        let start = Instant::now();
        state.seed_progress(start, |_| Some(7));
        state.mark_startup_completed(start);

        let first = state.evaluate_blocked(
            start + Duration::from_millis(11),
            8,
            |consumer_id| if consumer_id == "c1" { Some(7) } else { Some(8) },
        );
        assert!(first.is_none(), "first stalled observation should only alert");

        let second = state.evaluate_blocked(
            start + Duration::from_millis(13),
            8,
            |consumer_id| if consumer_id == "c1" { Some(7) } else { Some(8) },
        );
        assert!(second.is_none(), "same stall window should not emit a second alert");

        let recorded = alerts.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1, "stall hook should fire exactly once");
        assert_eq!(
            recorded[0],
            RequiredConsumerAlert {
                consumer_id: "c1".into(),
                last_sequence: 7,
                stalled_for: Duration::from_millis(11),
            }
        );
    }
}
