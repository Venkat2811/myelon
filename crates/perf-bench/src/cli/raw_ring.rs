use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::ReportOutputArgs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawRingClass {
    All,
    Message,
    Signal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawRingMode {
    Throughput,
    Co,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawRingScenarioKind {
    Message,
    Signal,
}

#[derive(Debug, Clone)]
pub struct RawRingScenarioSpec {
    pub label: &'static str,
    pub backend: BackendKind,
    pub kind: RawRingScenarioKind,
    pub events: u64,
    pub buffer: usize,
    pub warmup: u64,
    pub consumers: usize,
    pub record_latency: bool,
    pub producer_role: &'static str,
    pub consumer_role: &'static str,
    pub event_bytes: usize,
    pub target_rate: u64,
}

#[derive(Debug, Clone)]
pub struct RawRingSelection {
    class: RawRingClass,
    mode: RawRingMode,
    consumer_filter: Option<usize>,
    target_rate: Option<u64>,
    signal_events_override: Option<u64>,
    pub output_args: ReportOutputArgs,
}

impl RawRingSelection {
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let class = match find_arg_value(args, "--class").as_deref().unwrap_or("all") {
            "all" => RawRingClass::All,
            "message" => RawRingClass::Message,
            "signal" => RawRingClass::Signal,
            other => return Err(format!("unsupported class: {other}")),
        };
        let mode = match find_arg_value(args, "--mode")
            .as_deref()
            .unwrap_or("throughput")
        {
            "throughput" => RawRingMode::Throughput,
            "co" => RawRingMode::Co,
            other => return Err(format!("unsupported mode: {other}")),
        };
        let consumer_filter = match find_arg_value(args, "--consumers").as_deref() {
            None | Some("all") => None,
            Some(value) => Some(
                value
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --consumers value {value}: {error}"))?,
            ),
        };
        let target_rate = find_arg_value(args, "--target-rate")
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --target-rate value {value}: {error}"))
            })
            .transpose()?;
        let signal_events_override = find_arg_value(args, "--events")
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --events value {value}: {error}"))
            })
            .transpose()?;

        if mode == RawRingMode::Co && target_rate.is_none() {
            return Err("--mode co requires --target-rate".into());
        }
        if mode != RawRingMode::Co && target_rate.is_some() {
            return Err("--target-rate requires --mode co".into());
        }
        if mode == RawRingMode::Co && class == RawRingClass::Signal {
            return Err("CO mode is only supported for --class message".into());
        }

        Ok(Self {
            class,
            mode,
            consumer_filter,
            target_rate,
            signal_events_override,
            output_args: ReportOutputArgs::from_args(args),
        })
    }

    pub fn run_message(&self) -> bool {
        matches!(self.class, RawRingClass::All | RawRingClass::Message)
    }

    pub fn run_signal(&self) -> bool {
        self.mode != RawRingMode::Co
            && matches!(self.class, RawRingClass::All | RawRingClass::Signal)
    }

    pub fn target_rate(&self) -> u64 {
        self.target_rate.unwrap_or(0)
    }

    pub fn signal_events(&self, default_events: u64) -> u64 {
        self.signal_events_override.unwrap_or(default_events)
    }

    pub fn scenario_specs(
        &self,
        backend: BackendKind,
        signal_multi_events: u64,
    ) -> Vec<RawRingScenarioSpec> {
        base_specs(backend, signal_multi_events)
            .into_iter()
            .filter_map(|mut scenario| {
                self.apply_overrides(&mut scenario);
                if !self.should_run(&scenario) {
                    return None;
                }
                if scenario.kind == RawRingScenarioKind::Message {
                    scenario.target_rate = self.target_rate();
                }
                Some(scenario)
            })
            .collect()
    }

    fn should_run(&self, scenario: &RawRingScenarioSpec) -> bool {
        let kind_allowed = match scenario.kind {
            RawRingScenarioKind::Message => self.run_message(),
            RawRingScenarioKind::Signal => self.run_signal(),
        };
        let consumer_matches = self
            .consumer_filter
            .is_none_or(|consumers| consumers == scenario.consumers);
        kind_allowed && (scenario.consumers == 1 || consumer_matches)
    }

    fn apply_overrides(&self, scenario: &mut RawRingScenarioSpec) {
        if scenario.kind == RawRingScenarioKind::Message {
            return;
        }
        if let Some(signal_events) = self.signal_events_override {
            scenario.events = signal_events;
            scenario.warmup = (signal_events / 100).clamp(100_000, 1_000_000);
        }
        // Allow the signal binary (or any caller) to force latency recording
        // for signal scenarios via env var.
        if std::env::var("PERF_BENCH_SIGNAL_RECORD_LATENCY")
            .ok()
            .as_deref()
            == Some("1")
        {
            scenario.record_latency = true;
        }
    }
}

fn base_specs(backend: BackendKind, signal_multi_events: u64) -> Vec<RawRingScenarioSpec> {
    vec![
        message_spec(
            backend.clone(),
            "message_1p1c_144B",
            100_000,
            1024,
            1_000,
            1,
            true,
        ),
        message_spec(
            backend.clone(),
            "message_1p2c_144B",
            100_000,
            1024,
            1_000,
            2,
            true,
        ),
        message_spec(
            backend.clone(),
            "message_1p3c_144B",
            100_000,
            1024,
            1_000,
            3,
            true,
        ),
        message_spec(
            backend.clone(),
            "message_1p4c_144B",
            100_000,
            2048,
            1_000,
            4,
            true,
        ),
        message_spec(
            backend.clone(),
            "message_1p6c_144B",
            100_000,
            4096,
            1_000,
            6,
            false,
        ),
        message_spec(
            backend.clone(),
            "message_1p8c_144B",
            100_000,
            4096,
            1_000,
            8,
            false,
        ),
        message_spec(
            backend.clone(),
            "message_1p12c_144B",
            100_000,
            4096,
            1_000,
            12,
            false,
        ),
        signal_spec(
            backend.clone(),
            "signal_1p1c_64B",
            10_000_000,
            65_536,
            100_000,
            1,
        ),
        signal_spec(
            backend.clone(),
            "signal_1p2c_64B",
            signal_multi_events,
            65_536,
            100_000,
            2,
        ),
        signal_spec(
            backend.clone(),
            "signal_1p4c_64B",
            signal_multi_events,
            65_536,
            100_000,
            4,
        ),
        signal_spec(
            backend.clone(),
            "signal_1p6c_64B",
            signal_multi_events,
            65_536,
            100_000,
            6,
        ),
        signal_spec(
            backend.clone(),
            "signal_1p8c_64B",
            signal_multi_events,
            65_536,
            100_000,
            8,
        ),
        signal_spec(
            backend,
            "signal_1p12c_64B",
            signal_multi_events,
            65_536,
            100_000,
            12,
        ),
    ]
}

fn message_spec(
    backend: BackendKind,
    label: &'static str,
    events: u64,
    buffer: usize,
    warmup: u64,
    consumers: usize,
    record_latency: bool,
) -> RawRingScenarioSpec {
    let (producer_role, consumer_role) = match (&backend, consumers > 1) {
        (BackendKind::Shm, false) => ("msg_producer", "msg_consumer"),
        (BackendKind::Shm, true) => ("msg_multi_producer", "msg_multi_consumer"),
        (BackendKind::Mmap, false) => ("mmap_msg_producer", "mmap_msg_consumer"),
        (BackendKind::Mmap, true) => ("mmap_multi_msg_producer", "mmap_multi_msg_consumer"),
        _ => ("msg_producer", "msg_consumer"),
    };

    RawRingScenarioSpec {
        label,
        backend,
        kind: RawRingScenarioKind::Message,
        events,
        buffer,
        warmup,
        consumers,
        record_latency,
        producer_role,
        consumer_role,
        event_bytes: 144,
        target_rate: 0,
    }
}

fn signal_spec(
    backend: BackendKind,
    label: &'static str,
    events: u64,
    buffer: usize,
    warmup: u64,
    consumers: usize,
) -> RawRingScenarioSpec {
    let (producer_role, consumer_role) = match (&backend, consumers > 1) {
        (BackendKind::Shm, false) => ("sig_producer", "sig_consumer"),
        (BackendKind::Shm, true) => ("sig_multi_producer", "sig_multi_consumer"),
        (BackendKind::Mmap, false) => ("mmap_sig_producer", "mmap_sig_consumer"),
        (BackendKind::Mmap, true) => ("mmap_multi_sig_producer", "mmap_multi_sig_consumer"),
        _ => ("sig_producer", "sig_consumer"),
    };

    RawRingScenarioSpec {
        label,
        backend,
        kind: RawRingScenarioKind::Signal,
        events,
        buffer,
        warmup,
        consumers,
        record_latency: false,
        producer_role,
        consumer_role,
        event_bytes: 64,
        target_rate: 0,
    }
}

fn find_arg_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_parses_co_mode_and_filters() {
        let args = vec![
            "bench".to_string(),
            "--class".to_string(),
            "message".to_string(),
            "--mode".to_string(),
            "co".to_string(),
            "--target-rate".to_string(),
            "20000".to_string(),
            "--consumers".to_string(),
            "2".to_string(),
        ];
        let selection = RawRingSelection::parse(&args).expect("parse selection");
        assert!(selection.run_message());
        assert!(!selection.run_signal());
        assert_eq!(selection.target_rate(), 20_000);

        let scenarios = selection.scenario_specs(BackendKind::Shm, 10_000_000);
        assert_eq!(scenarios.len(), 2);
        assert_eq!(scenarios[0].label, "message_1p1c_144B");
        assert_eq!(scenarios[1].label, "message_1p2c_144B");
        assert_eq!(scenarios[0].target_rate, 20_000);
        assert_eq!(scenarios[1].target_rate, 20_000);
        assert_eq!(scenarios[1].producer_role, "msg_multi_producer");
    }

    #[test]
    fn scenario_override_updates_signal_events_and_warmup() {
        let args = vec![
            "bench".to_string(),
            "--events".to_string(),
            "50000000".to_string(),
        ];
        let selection = RawRingSelection::parse(&args).expect("parse selection");
        let scenarios = selection.scenario_specs(BackendKind::Mmap, 1_000_000);
        let scenario = scenarios
            .into_iter()
            .find(|scenario| scenario.label == "signal_1p1c_64B")
            .expect("signal scenario");
        assert_eq!(scenario.events, 50_000_000);
        assert_eq!(scenario.warmup, 500_000);
        assert_eq!(scenario.producer_role, "mmap_sig_producer");
    }
}
