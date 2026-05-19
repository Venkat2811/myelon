use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::ReportOutputArgs;

use super::pingpong;

pub const RAW_EVENT_HEADER_BYTES: usize = 16;
pub const RAW_EVENT_SLOT_ALIGNMENT_BYTES: usize = 64;

pub const fn raw_payload_bytes(event_bytes: usize) -> usize {
    event_bytes.saturating_sub(RAW_EVENT_HEADER_BYTES)
}

pub const fn aligned_slot_bytes(event_bytes: usize) -> usize {
    if event_bytes == 0 {
        return 0;
    }
    let rem = event_bytes % RAW_EVENT_SLOT_ALIGNMENT_BYTES;
    if rem == 0 {
        event_bytes
    } else {
        event_bytes + (RAW_EVENT_SLOT_ALIGNMENT_BYTES - rem)
    }
}

pub fn message_label(consumers: usize, event_bytes: usize) -> String {
    let slot_bytes = aligned_slot_bytes(event_bytes);
    if slot_bytes == event_bytes {
        format!(
            "message_1p{}c_{}",
            consumers,
            pingpong::human_size(event_bytes)
        )
    } else {
        format!(
            "message_1p{}c_{}req_{}slot",
            consumers,
            pingpong::human_size(event_bytes),
            pingpong::human_size(slot_bytes)
        )
    }
}

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
    pub label: String,
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
    num_messages_override: Option<u64>,
    warmup_override: Option<u64>,
    size_override: Option<usize>,
    buffer_override: Option<usize>,
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
        let num_messages_override = find_arg_value(args, "--num-messages")
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --num-messages value {value}: {error}"))
            })
            .transpose()?;
        let warmup_override = find_arg_value(args, "--warmup")
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --warmup value {value}: {error}"))
            })
            .transpose()?;
        let size_override = find_arg_value(args, "--size")
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --size value {value}: {error}"))
            })
            .transpose()?;
        let buffer_override = find_arg_value(args, "--buffer-size")
            .or_else(|| find_arg_value(args, "--buffer"))
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --buffer-size value {value}: {error}"))
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
            num_messages_override,
            warmup_override,
            size_override,
            buffer_override,
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

    pub fn event_bytes(&self) -> usize {
        self.size_override.unwrap_or(144)
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
        let consumer_matches = match self.consumer_filter {
            Some(consumers) => consumers == scenario.consumers,
            None => true,
        };
        kind_allowed && consumer_matches
    }

    fn apply_overrides(&self, scenario: &mut RawRingScenarioSpec) {
        if scenario.kind == RawRingScenarioKind::Message {
            if let Some(size) = self.size_override {
                scenario.event_bytes = size;
                scenario.buffer = pingpong::default_buffer_size(size);
                scenario.label = message_label(scenario.consumers, size);
            }
            if let Some(n) = self.num_messages_override {
                scenario.events = n;
            }
            if let Some(w) = self.warmup_override {
                scenario.warmup = w;
            }
            if let Some(buffer) = self.buffer_override {
                scenario.buffer = buffer;
            }
            return;
        }
        if let Some(signal_events) = self.signal_events_override {
            scenario.events = signal_events;
            scenario.warmup = if let Some(warmup) = self.warmup_override {
                warmup
            } else {
                (signal_events / 100).clamp(100_000, 1_000_000)
            };
        } else if let Some(warmup) = self.warmup_override {
            scenario.warmup = warmup;
        }
        if let Some(buffer) = self.buffer_override {
            scenario.buffer = buffer;
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
            &message_label(1, 144),
            100_000,
            1024,
            1_000,
            1,
            true,
        ),
        message_spec(
            backend.clone(),
            &message_label(2, 144),
            100_000,
            1024,
            1_000,
            2,
            true,
        ),
        message_spec(
            backend.clone(),
            &message_label(3, 144),
            100_000,
            1024,
            1_000,
            3,
            true,
        ),
        message_spec(
            backend.clone(),
            &message_label(4, 144),
            100_000,
            2048,
            1_000,
            4,
            true,
        ),
        message_spec(
            backend.clone(),
            &message_label(6, 144),
            100_000,
            4096,
            1_000,
            6,
            false,
        ),
        message_spec(
            backend.clone(),
            &message_label(8, 144),
            100_000,
            4096,
            1_000,
            8,
            false,
        ),
        message_spec(
            backend.clone(),
            &message_label(12, 144),
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
    label: &str,
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
        label: label.to_string(),
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
    label: &str,
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
        label: label.to_string(),
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

/// Supported total event sizes for `BenchEvent` dispatching.
///
/// Each value is the total size in bytes. The const generic payload = total - 16
/// (`BenchEvent` header: 8B sequence + 8B `timestamp_ns`).
pub const SUPPORTED_EVENT_SIZES: &[usize] = &[
    32, 64, 128, 144, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 524288,
    1048576, 2097152, 8388608, 16777216, 33554432, 67108864,
];

/// Returns true if `event_bytes` is a supported total event size for dispatch.
pub fn is_supported_event_size(event_bytes: usize) -> bool {
    SUPPORTED_EVENT_SIZES.contains(&event_bytes)
}

/// Dispatches to a generic function based on the `BenchEvent` payload size (`event_bytes` - 16).
///
/// Usage:
/// ```ignore
/// dispatch_bench_event!(event_bytes, |<SIZE>| {
///     some_generic_function::<SIZE>(args)
/// })
/// ```
#[macro_export]
macro_rules! dispatch_bench_event {
    ($event_bytes:expr, |<$N:ident>| $body:expr) => {
        match $event_bytes {
            32 => {
                const $N: usize = 16;
                $body
            }
            64 => {
                const $N: usize = 48;
                $body
            }
            128 => {
                const $N: usize = 112;
                $body
            }
            144 => {
                const $N: usize = 128;
                $body
            }
            256 => {
                const $N: usize = 240;
                $body
            }
            512 => {
                const $N: usize = 496;
                $body
            }
            1024 => {
                const $N: usize = 1008;
                $body
            }
            2048 => {
                const $N: usize = 2032;
                $body
            }
            4096 => {
                const $N: usize = 4080;
                $body
            }
            8192 => {
                const $N: usize = 8176;
                $body
            }
            16384 => {
                const $N: usize = 16368;
                $body
            }
            32768 => {
                const $N: usize = 32752;
                $body
            }
            65536 => {
                const $N: usize = 65520;
                $body
            }
            131072 => {
                const $N: usize = 131056;
                $body
            }
            524288 => {
                const $N: usize = 524272;
                $body
            }
            1048576 => {
                const $N: usize = 1048560;
                $body
            }
            2097152 => {
                const $N: usize = 2097136;
                $body
            }
            8388608 => {
                const $N: usize = 8388592;
                $body
            }
            16777216 => {
                const $N: usize = 16777200;
                $body
            }
            33554432 => {
                const $N: usize = 33554416;
                $body
            }
            67108864 => {
                const $N: usize = 67108848;
                $body
            }
            other => Err(format!(
                "unsupported event size: {other}B (supported: {})",
                $crate::cli::raw_ring::SUPPORTED_EVENT_SIZES
                    .iter()
                    .map(|s| format!("{s}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .into()),
        }
    };
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
    fn aligned_slot_bytes_rounds_up_to_cache_line() {
        assert_eq!(aligned_slot_bytes(32), 64);
        assert_eq!(aligned_slot_bytes(64), 64);
        assert_eq!(aligned_slot_bytes(144), 192);
        assert_eq!(aligned_slot_bytes(2048), 2048);
    }

    #[test]
    fn message_label_marks_non_exact_slot_sizes() {
        assert_eq!(message_label(4, 32), "message_1p4c_32Breq_64Bslot");
        assert_eq!(message_label(2, 144), "message_1p2c_144Breq_192Bslot");
        assert_eq!(message_label(8, 2048), "message_1p8c_2KB");
    }

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
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].label, "message_1p2c_144Breq_192Bslot");
        assert_eq!(scenarios[0].target_rate, 20_000);
        assert_eq!(scenarios[0].producer_role, "msg_multi_producer");
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

    #[test]
    fn signal_warmup_override_applies_without_events_override() {
        let args = vec![
            "bench".to_string(),
            "--warmup".to_string(),
            "250".to_string(),
        ];
        let selection = RawRingSelection::parse(&args).expect("parse selection");
        let scenarios = selection.scenario_specs(BackendKind::Shm, 10_000_000);
        let scenario = scenarios
            .into_iter()
            .find(|entry| entry.label == "signal_1p1c_64B")
            .expect("signal scenario");
        assert_eq!(scenario.warmup, 250);
    }

    #[test]
    fn size_override_updates_message_scenarios() {
        let args = vec![
            "bench".to_string(),
            "--class".to_string(),
            "message".to_string(),
            "--size".to_string(),
            "2048".to_string(),
            "--consumers".to_string(),
            "4".to_string(),
        ];
        let selection = RawRingSelection::parse(&args).expect("parse selection");
        assert_eq!(selection.event_bytes(), 2048);

        let scenarios = selection.scenario_specs(BackendKind::Shm, 10_000_000);
        assert_eq!(scenarios.len(), 1);
        for scenario in &scenarios {
            assert_eq!(scenario.event_bytes, 2048);
            assert_eq!(scenario.label, "message_1p4c_2KB");
        }
        // 2048B => default_buffer_size(2048) = 2048
        assert_eq!(scenarios[0].buffer, pingpong::default_buffer_size(2048));
    }

    #[test]
    fn size_override_does_not_affect_signal_scenarios() {
        let args = vec![
            "bench".to_string(),
            "--size".to_string(),
            "4096".to_string(),
        ];
        let selection = RawRingSelection::parse(&args).expect("parse selection");
        let scenarios = selection.scenario_specs(BackendKind::Shm, 10_000_000);
        let signal = scenarios
            .iter()
            .find(|s| s.label == "signal_1p1c_64B")
            .expect("signal scenario");
        assert_eq!(signal.event_bytes, 64);
    }
}
