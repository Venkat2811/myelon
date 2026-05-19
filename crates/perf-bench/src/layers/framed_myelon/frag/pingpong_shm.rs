use crate::cli::myelon_pingpong::{
    FramedPingPongScenarioSpec, FramedPingPongSelection, PingPongMode,
};
use crate::infra::coordination::UnifiedCoordination;
use crate::infra::events::nanos_now;
use crate::infra::latency::LatencyRecorder;
use crate::infra::liveness::{liveness_config, liveness_enabled};
use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::{self, BenchReport, BenchTransportSpec};
use crate::infra::{
    self, segment_from_env, spawn_child, unique_shm_segment, ConsumerOutput, IpcBenchmark,
    ProducerOutput, ScenarioChildren,
};

/// Same `publish_or_managed!` pattern as the raw layers — see
/// `crates/perf-bench/src/layers/raw/disruptor_mp/pingpong_shm.rs`
/// for the rationale. `FramedTransportProducer` exposes both
/// `publish` and `publish_managed`, with the same `&[u8] + kind`
/// signature.
macro_rules! publish_or_managed {
    ($producer:expr, $payload:expr, $kind:expr) => {{
        if $crate::infra::liveness::liveness_enabled() {
            $producer.publish_managed($payload, $kind)?;
        } else {
            $producer.publish($payload, $kind);
        }
    }};
}
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MyelonWaitStrategy,
    ReassemblyBuffer,
};
use std::env;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;
const ATTACH_TIMEOUT: Duration = Duration::from_secs(30);
const PING_ECHO_ID: &str = "ping_echo";
const PONG_INITIATOR_ID: &str = "pong_init";

#[derive(Clone)]
struct Scenario {
    payload_label: &'static str,
    payload_bytes: usize,
    messages: u64,
    warmup: u64,
    buffer_depth: usize,
    wait_strategy: String,
    mode: PingPongMode,
    target_rate: u64,
    producer_role: &'static str,
    consumer_role: &'static str,
}

struct ShmEnv {
    ping_segment: String,
    pong_segment: String,
    coordination_segment: String,
    payload_bytes: usize,
    messages: u64,
    warmup: u64,
    buffer_depth: usize,
    wait_strategy: String,
    target_rate: u64,
}

fn read_env() -> ShmEnv {
    ShmEnv {
        ping_segment: segment_from_env("PING_SEGMENT"),
        pong_segment: segment_from_env("PONG_SEGMENT"),
        coordination_segment: segment_from_env("COORDINATION_SEGMENT"),
        payload_bytes: infra::read_env_usize("BENCH_PAYLOAD_BYTES", 64),
        messages: infra::read_env_u64("BENCH_MESSAGES", 100_000),
        warmup: infra::read_env_u64("BENCH_WARMUP", 10_000),
        buffer_depth: infra::read_env_usize("BENCH_BUFFER_DEPTH", 4096),
        wait_strategy: env::var("BENCH_WAIT_STRATEGY").unwrap_or_else(|_| "busyspin".into()),
        target_rate: infra::read_env_u64("BENCH_TARGET_RATE", 0),
    }
}

fn parse_wait_strategy(value: &str) -> Result<MyelonWaitStrategy, String> {
    match value.to_ascii_lowercase().as_str() {
        "busyspin" => Ok(MyelonWaitStrategy::BusySpin),
        "block" => Ok(MyelonWaitStrategy::Block),
        other => Err(format!("unsupported wait strategy: {other}")),
    }
}

fn attach_consumer_with_timeout(
    segment: &str,
    depth: usize,
    consumer_id: &str,
    wait_strategy: MyelonWaitStrategy,
) -> Result<FramedTransportConsumer<Frame>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + ATTACH_TIMEOUT;
    loop {
        match FramedTransportConsumer::<Frame>::attach_with_consumer_id(
            segment,
            depth,
            consumer_id,
            wait_strategy,
        ) {
            Ok(consumer) => return Ok(consumer),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("attach failed for {consumer_id}: {error}").into()),
        }
    }
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0u64, |acc, value| acc.wrapping_add(*value as u64))
}

fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|idx| (idx % 251) as u8).collect()
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let env = read_env();
    let wait_strategy = parse_wait_strategy(&env.wait_strategy)?;
    let coordination = UnifiedCoordination::create(&env.coordination_segment)?;
    let mut pong_producer = FramedTransportProducer::<Frame>::create_with_consumers(
        &env.pong_segment,
        env.buffer_depth,
        1,
    )?;
    // RFC-0017.5: pong_producer publishes back to the initiator's
    // PONG_INITIATOR_ID consumer.
    if liveness_enabled() {
        pong_producer.enable_required_consumer_liveness(liveness_config(&[PONG_INITIATOR_ID]));
    }
    coordination
        .data()
        .producer_ready
        .store(1, Ordering::Release);

    let mut ping_consumer = attach_consumer_with_timeout(
        &env.ping_segment,
        env.buffer_depth,
        PING_ECHO_ID,
        wait_strategy,
    )?;
    coordination
        .data()
        .echo_attached
        .store(1, Ordering::Release);
    coordination.data().echo_ready.store(1, Ordering::Release);

    if !coordination.wait_for_consumer_attached(ATTACH_TIMEOUT) {
        return Err("timeout waiting for initiator attach".into());
    }
    if !pong_producer.discover_consumer_id(PONG_INITIATOR_ID, ATTACH_TIMEOUT) {
        return Err("timeout discovering pong initiator consumer".into());
    }

    let total = env.warmup + env.messages;
    let mut measured_start = None;
    let mut reassembly = ReassemblyBuffer::new(env.payload_bytes.max(256 * 1024));
    for index in 0..total {
        if coordination.is_shutdown() {
            return Err("shutdown requested during echo loop".into());
        }
        ping_consumer.recv_message_blocking_leased(
            &mut reassembly,
            |_kind, payload| -> Result<(), Box<dyn std::error::Error>> {
                if index == env.warmup {
                    measured_start = Some(Instant::now());
                }
                publish_or_managed!(pong_producer, payload, 1);
                Ok(())
            },
        )?;
    }

    let elapsed = measured_start.unwrap_or_else(Instant::now).elapsed();
    let output = ProducerOutput::from_elapsed(env.messages, elapsed, env.payload_bytes);
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let env = read_env();
    let wait_strategy = parse_wait_strategy(&env.wait_strategy)?;
    let coordination =
        UnifiedCoordination::attach_with_timeout(&env.coordination_segment, ATTACH_TIMEOUT)?;
    let mut ping_producer = FramedTransportProducer::<Frame>::create_with_consumers(
        &env.ping_segment,
        env.buffer_depth,
        1,
    )?;
    // RFC-0017.5: ping_producer publishes to the echo's
    // PING_ECHO_ID consumer.
    if liveness_enabled() {
        ping_producer.enable_required_consumer_liveness(liveness_config(&[PING_ECHO_ID]));
    }
    let mut pong_consumer = attach_consumer_with_timeout(
        &env.pong_segment,
        env.buffer_depth,
        PONG_INITIATOR_ID,
        wait_strategy,
    )?;
    coordination
        .data()
        .consumer_attached
        .store(1, Ordering::Release);

    if !coordination.wait_for_echo_ready(ATTACH_TIMEOUT) {
        return Err("timeout waiting for echo readiness".into());
    }
    if !ping_producer.discover_consumer_id(PING_ECHO_ID, ATTACH_TIMEOUT) {
        return Err("timeout discovering ping echo consumer".into());
    }

    let payload = make_payload(env.payload_bytes);
    let expected_checksum = checksum_bytes(&payload);
    let expected_total = expected_checksum.wrapping_mul(env.messages);
    let mut recorder = LatencyRecorder::default_range();
    let mut checksum_total = 0u64;
    let total = env.warmup + env.messages;
    let mut measured_start = None;
    let mut reassembly = ReassemblyBuffer::new(env.payload_bytes.max(256 * 1024));
    let interval_ns = crate::infra::co_interval_ns(env.target_rate);
    let base_ns = nanos_now();

    for index in 0..total {
        let measured_index = index.saturating_sub(env.warmup);
        let send_ns = if let Some(interval) = interval_ns {
            if index >= env.warmup {
                let intended_ns = base_ns.saturating_add(measured_index.saturating_mul(interval));
                while nanos_now() < intended_ns {
                    std::hint::spin_loop();
                }
                intended_ns
            } else {
                nanos_now()
            }
        } else {
            nanos_now()
        };

        if index == env.warmup {
            measured_start = Some(Instant::now());
        }

        publish_or_managed!(ping_producer, &payload, 1);
        pong_consumer.recv_message_blocking_leased(
            &mut reassembly,
            |_kind, response| -> Result<(), Box<dyn std::error::Error>> {
                if index < env.warmup {
                    return Ok(());
                }

                if response.len() != payload.len() {
                    return Err(format!(
                        "unexpected echoed payload size: got {} expected {}",
                        response.len(),
                        payload.len()
                    )
                    .into());
                }

                checksum_total = checksum_total.wrapping_add(checksum_bytes(response));
                recorder.record_delta(send_ns, nanos_now());
                Ok(())
            },
        )?;
    }

    coordination.signal_shutdown();

    if checksum_total != expected_total {
        return Err(
            format!("checksum mismatch: got {checksum_total} expected {expected_total}").into(),
        );
    }

    let elapsed = measured_start.unwrap_or_else(Instant::now).elapsed();
    let latency = recorder.stats().ok_or("no latency samples recorded")?;
    let output =
        ConsumerOutput::from_elapsed(0, env.messages, elapsed, env.payload_bytes, checksum_total)
            .with_latency(latency);
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "pingpong_framed_shm"
    }

    fn scenario_name(&self) -> String {
        format!("pingpong_framed_1p1c_{}", self.payload_label)
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        "framed"
    }

    fn transport_metadata(&self) -> BenchTransportSpec {
        let mut spec = reporting::BenchTransportSpec::unified_pingpong()
            .with_zero_copy(false)
            .with_framing("fixed_64k");
        spec.discovery_mode = Some("explicit_consumer_id".to_string());
        spec
    }

    fn measurement_mode(&self) -> String {
        self.mode.measurement_label(self.target_rate)
    }

    fn wait_strategy(&self) -> &str {
        match self.wait_strategy.to_ascii_lowercase().as_str() {
            "block" => "Block",
            _ => "BusySpin",
        }
    }

    fn message_size_bytes(&self) -> usize {
        self.payload_bytes
    }

    fn buffer_depth(&self) -> usize {
        self.buffer_depth
    }

    fn num_messages(&self) -> u64 {
        self.messages
    }

    fn warmup_messages(&self) -> u64 {
        self.warmup
    }

    fn num_consumers(&self) -> usize {
        1
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(300)
    }

    fn throughput_unit(&self) -> &str {
        "msgs/s"
    }

    fn consumer_summary_label(&self) -> &str {
        "initiator"
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, infra::BenchError> {
        let ping_segment = unique_shm_segment("mppf_ping");
        let pong_segment = unique_shm_segment("mppf_pong");
        let coordination_segment = unique_shm_segment("mppf_coord");
        let env_common = vec![
            ("PING_SEGMENT", ping_segment),
            ("PONG_SEGMENT", pong_segment),
            ("COORDINATION_SEGMENT", coordination_segment),
            ("BENCH_PAYLOAD_BYTES", self.payload_bytes.to_string()),
            ("BENCH_MESSAGES", self.messages.to_string()),
            ("BENCH_WARMUP", self.warmup.to_string()),
            ("BENCH_BUFFER_DEPTH", self.buffer_depth.to_string()),
            ("BENCH_WAIT_STRATEGY", self.wait_strategy.clone()),
            ("BENCH_TARGET_RATE", self.target_rate.to_string()),
        ];

        let producer = spawn_child(exe, self.producer_role, &env_common);
        let consumer = spawn_child(exe, self.consumer_role, &env_common);
        Ok(ScenarioChildren::new(producer, vec![consumer]))
    }
}

impl Scenario {
    fn from_spec(spec: &FramedPingPongScenarioSpec, selection: &FramedPingPongSelection) -> Self {
        Self {
            payload_label: spec.payload_label,
            payload_bytes: spec.payload_bytes,
            messages: spec.messages,
            warmup: spec.warmup,
            buffer_depth: spec.buffer_depth,
            wait_strategy: spec.wait_strategy.clone(),
            mode: selection.mode,
            target_rate: spec.target_rate,
            producer_role: spec.producer_role,
            consumer_role: spec.consumer_role,
        }
    }
}

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("pingpong_framed_shm_echo", producer_process),
    infra::ChildRole::new("pingpong_framed_shm_initiator", consumer_process),
];

pub struct PingPongFramedShmBench;

impl infra::BenchHarness for PingPongFramedShmBench {
    fn bench_name(&self) -> &'static str {
        "pingpong_framed_shm"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let selection = FramedPingPongSelection::parse(args)?;

        if !selection.output_args.json_mode {
            println!("=== Myelon Framed SHM Ping-Pong ===");
            println!("Mode: {}", selection.mode.description());
            if selection.target_rate > 0 {
                println!("Target rate: {} msgs/s", selection.target_rate);
            }
            println!("Transport: FramedTransport over SHM, strict 1p1c");
            println!();
        }

        let mut report = BenchReport::new();
        for spec in selection.scenario_specs(BackendKind::Shm) {
            report.add(Scenario::from_spec(&spec, &selection).run_benchmark()?);
        }

        reporting::emit_report(
            &report,
            &selection.output_args,
            Some(reporting::ReportView::Summary),
            None,
            None,
        );
        Ok(())
    }
}
