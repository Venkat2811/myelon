//! End-to-end Codec benchmark over SHM typed transport.
//!
//! Measures: struct -> Codec::encode -> TypedTransport publish -> ring ->
//!           TypedTransport consume -> Codec::decode -> struct access.
//!
//! This bench intentionally uses `TypedProducer` / `TypedConsumer` so it
//! measures the RFC 0012 API surface, not ad-hoc encode/decode calls.

use myelon::codec::Codec;
use myelon::transport::{
    FrameMeta, FramedTransportConsumer, FramedTransportFrame, FramedTransportProducer,
    MyelonWaitStrategy,
};
use myelon::typed_transport::{TypedConsumer, TypedProducer};
use perf_bench::codec_payloads::{
    checksum_payloads, encoded_len, make_payloads, BincodeBatch, FlatbufBatch, RkyvBatch,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::format_throughput;
use perf_bench::harness::{
    self, segment_from_env, spawn_child, unique_shm_segment, ConsumerOutput, IpcBenchmark,
    PhaseTiming, ProducerOutput, ScenarioChildren,
};
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, BenchReport};
use std::cell::Cell;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = TimedFrame<FRAME_DATA_BYTES>;
const BUFFER_DEPTH: usize = 1024;
const TARGET_CONSUMERS: [usize; 6] = [1, 2, 4, 6, 8, 12];

thread_local! {
    static INTENDED_SEND_TIMESTAMP_NS: Cell<Option<u64>> = Cell::new(None);
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct TimedFrame<const DATA_BYTES: usize> {
    len: u32,
    kind: u8,
    flags: u8,
    msg_id: u32,
    timestamp_ns: u64,
    data: [u8; DATA_BYTES],
}

impl<const DATA_BYTES: usize> Default for TimedFrame<DATA_BYTES> {
    fn default() -> Self {
        Self {
            len: 0,
            kind: 0,
            flags: 0,
            msg_id: 0,
            timestamp_ns: 0,
            data: [0u8; DATA_BYTES],
        }
    }
}

impl<const DATA_BYTES: usize> FramedTransportFrame for TimedFrame<DATA_BYTES> {
    fn payload_capacity() -> usize {
        DATA_BYTES
    }

    fn frame_meta(&self) -> FrameMeta<'_> {
        FrameMeta {
            len: self.len as usize,
            kind: self.kind,
            flags: self.flags,
            msg_id: self.msg_id,
            timestamp_ns: Some(self.timestamp_ns),
            data: &self.data[..self.len as usize],
        }
    }

    fn write_frame(&mut self, payload: &[u8], kind: u8, msg_id: u32, flags: u8) {
        assert!(payload.len() <= DATA_BYTES);
        self.len = payload.len() as u32;
        self.kind = kind;
        self.flags = flags;
        self.msg_id = msg_id;
        self.timestamp_ns = current_send_timestamp_ns();
        self.data[..payload.len()].copy_from_slice(payload);
    }
}

fn wall_clock_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos() as u64
}

fn current_send_timestamp_ns() -> u64 {
    INTENDED_SEND_TIMESTAMP_NS
        .with(|cell| cell.get())
        .unwrap_or_else(wall_clock_ns)
}

fn set_intended_send_timestamp(timestamp_ns: Option<u64>) {
    INTENDED_SEND_TIMESTAMP_NS.with(|cell| cell.set(timestamp_ns));
}

fn scaled_buffer(consumers: usize) -> usize {
    BUFFER_DEPTH
        .max(consumers.next_power_of_two() * 256)
        .next_power_of_two()
}

fn read_codec_env() -> (String, usize, u64, String, usize, usize, u64) {
    let codec = env::var("BENCH_CODEC").expect("BENCH_CODEC");
    let batch_size = env::var("BENCH_BATCH_SIZE")
        .expect("BENCH_BATCH_SIZE")
        .parse()
        .expect("batch size");
    let messages = env::var("BENCH_MESSAGES")
        .expect("BENCH_MESSAGES")
        .parse()
        .expect("message count");
    let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
    let buffer_depth = env::var("BENCH_BUFFER_DEPTH")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(BUFFER_DEPTH);
    let num_consumers = env::var("BENCH_NUM_CONSUMERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let target_rate = env::var("BENCH_TARGET_RATE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    (
        codec,
        batch_size,
        messages,
        segment,
        buffer_depth,
        num_consumers,
        target_rate,
    )
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (codec, batch_size, messages, segment, buffer_depth, num_consumers, target_rate) =
        read_codec_env();
    let phase_timing = env::var("BENCH_PHASE_TIMING")
        .ok()
        .map_or(false, |v| v == "1");
    let payloads = make_payloads(batch_size);
    let encoded_bytes = encoded_len(&codec, &payloads);

    if phase_timing {
        // Phase timing: use FramedTransportProducer directly to time encode vs transport
        let mut producer = FramedTransportProducer::<Frame>::create_with_consumers(
            &segment,
            buffer_depth,
            num_consumers,
        )?;
        let coord = BenchmarkCoordination::create(&segment)?;
        if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
            return Err(format!("timeout waiting for {num_consumers} consumers").into());
        }
        producer.discover_consumers(Duration::from_secs(3));

        let mut encode_ns = 0u64;
        let mut transport_ns = 0u64;
        let start = Instant::now();

        macro_rules! phase_loop {
            ($batch:expr) => {
                for i in 0..messages {
                    let t0 = Instant::now();
                    let encoded = $batch.encode()?;
                    let t1 = Instant::now();
                    producer.publish(encoded.as_ref(), (i % 256) as u8);
                    let t2 = Instant::now();
                    encode_ns += (t1 - t0).as_nanos() as u64;
                    transport_ns += (t2 - t1).as_nanos() as u64;
                }
            };
        }

        match codec.as_str() {
            "bincode" => {
                let b = BincodeBatch(payloads);
                phase_loop!(b);
            }
            "rkyv" => {
                let b = RkyvBatch(payloads);
                phase_loop!(b);
            }
            "flatbuf" => {
                let b = FlatbufBatch(payloads);
                phase_loop!(b);
            }
            other => return Err(format!("unknown codec: {other}").into()),
        }

        let elapsed = start.elapsed();
        let mut output = ProducerOutput::from_elapsed(messages, elapsed, encoded_bytes);
        output.phase_timing = Some(PhaseTiming {
            encode_avg_ns: Some(encode_ns as f64 / messages as f64),
            transport_write_avg_ns: Some(transport_ns as f64 / messages as f64),
            transport_read_avg_ns: None,
            decode_avg_ns: None,
        });
        println!("{}", serde_json::to_string(&output)?);
        coord.signal_producer_done(messages as i64);
        coord.wait_for_consumers_done(num_consumers, Duration::from_secs(30));
    } else {
        let mut producer =
            TypedProducer::<Frame>::create_with_consumers(&segment, buffer_depth, num_consumers)?;
        let coord = BenchmarkCoordination::create(&segment)?;
        if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
            return Err(format!("timeout waiting for {num_consumers} consumers").into());
        }
        producer.discover_consumers(Duration::from_secs(3));

        let start = Instant::now();
        if target_rate > 0 {
            let interval_ns = 1_000_000_000u64 / target_rate;
            let base_ns = wall_clock_ns();
            macro_rules! co_loop {
                ($payload:expr) => {
                    for i in 0..messages {
                        let encoded = $payload.encode()?;
                        let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
                        while wall_clock_ns() < intended_ns {
                            std::hint::spin_loop();
                        }
                        set_intended_send_timestamp(Some(intended_ns));
                        producer.publish_raw(encoded.as_ref(), (i % 256) as u8);
                        set_intended_send_timestamp(None);
                    }
                };
            }

            match codec.as_str() {
                "bincode" => {
                    let payload = BincodeBatch(payloads);
                    co_loop!(payload);
                }
                "rkyv" => {
                    let payload = RkyvBatch(payloads);
                    co_loop!(payload);
                }
                "flatbuf" => {
                    let payload = FlatbufBatch(payloads);
                    co_loop!(payload);
                }
                other => return Err(format!("unknown codec: {other}").into()),
            }
            set_intended_send_timestamp(None);
        } else {
            match codec.as_str() {
                "bincode" => {
                    let payload = BincodeBatch(payloads);
                    for i in 0..messages {
                        producer.publish(&payload, (i % 256) as u8)?;
                    }
                }
                "rkyv" => {
                    let payload = RkyvBatch(payloads);
                    for i in 0..messages {
                        producer.publish(&payload, (i % 256) as u8)?;
                    }
                }
                "flatbuf" => {
                    let payload = FlatbufBatch(payloads);
                    for i in 0..messages {
                        producer.publish(&payload, (i % 256) as u8)?;
                    }
                }
                other => return Err(format!("unknown codec: {other}").into()),
            }
        }
        let elapsed = start.elapsed();
        let output = ProducerOutput::from_elapsed(messages, elapsed, encoded_bytes);
        println!("{}", serde_json::to_string(&output)?);
        coord.signal_producer_done(messages as i64);
        coord.wait_for_consumers_done(num_consumers, Duration::from_secs(30));
    }
    Ok(())
}

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (codec, batch_size, messages, segment, buffer_depth, _num_consumers, _target_rate) =
        read_codec_env();
    let consumer_id = env::var("CONSUMER_ID")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let phase_timing = env::var("BENCH_PHASE_TIMING")
        .ok()
        .map_or(false, |v| v == "1");
    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let encoded_bytes = encoded_len(&codec, &make_payloads(batch_size));

    if phase_timing {
        // Phase timing: use FramedTransportConsumer directly to time transport vs decode
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut consumer = loop {
            match FramedTransportConsumer::<Frame>::attach(
                &segment,
                buffer_depth,
                MyelonWaitStrategy::BusySpin,
            ) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25))
                }
                Err(e) => return Err(format!("consumer attach failed: {e}").into()),
            }
        };
        coord.signal_consumer_ready();

        let mut recv_ns = 0u64;
        let mut decode_ns = 0u64;
        let mut latency = LatencyRecorder::default_range();
        let start = Instant::now();
        let mut consumed = 0u64;
        let mut checksum = 0u64;

        while consumed < messages {
            let t0 = Instant::now();
            let (meta, raw_bytes) = consumer.recv_message_blocking_with_meta();
            let t1 = Instant::now();
            if let Some(lat_ns) = meta.one_way_latency_ns() {
                latency.record(lat_ns);
            }
            let payload_sum = match codec.as_str() {
                "bincode" => {
                    let batch = BincodeBatch::decode(&raw_bytes)?;
                    checksum_payloads(&batch.0)
                }
                "rkyv" => {
                    let batch = RkyvBatch::decode(&raw_bytes)?;
                    checksum_payloads(&batch.0)
                }
                "flatbuf" => {
                    let batch = FlatbufBatch::decode(&raw_bytes)?;
                    checksum_payloads(&batch.0)
                }
                other => return Err(format!("unknown codec: {other}").into()),
            };
            let t2 = Instant::now();
            black_box(payload_sum);
            checksum = checksum.wrapping_add(payload_sum);
            recv_ns += (t1 - t0).as_nanos() as u64;
            decode_ns += (t2 - t1).as_nanos() as u64;
            consumed += 1;
        }

        let elapsed = start.elapsed();
        let timing = PhaseTiming {
            encode_avg_ns: None,
            transport_write_avg_ns: None,
            transport_read_avg_ns: Some(recv_ns as f64 / consumed as f64),
            decode_avg_ns: Some(decode_ns as f64 / consumed as f64),
        };
        let mut output =
            ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, encoded_bytes, checksum)
                .with_phase_timing(timing);
        if let Some(stats) = latency.stats() {
            output = output.with_latency(stats);
        }
        println!("{}", serde_json::to_string(&output)?);
        coord.signal_consumer_done(consumed as i64);
    } else {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut consumer = loop {
            match TypedConsumer::<Frame>::attach(
                &segment,
                buffer_depth,
                MyelonWaitStrategy::BusySpin,
            ) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25))
                }
                Err(e) => return Err(format!("consumer attach failed: {e}").into()),
            }
        };
        coord.signal_consumer_ready();

        let mut latency = LatencyRecorder::default_range();
        let start = Instant::now();
        let mut consumed = 0u64;
        let mut checksum = 0u64;

        // Count-based termination: consume exactly `messages` to avoid deadlock
        // on recv_message_blocking after all messages consumed.
        while consumed < messages {
            let payload_sum = match codec.as_str() {
                "bincode" => {
                    let (_, b, meta): (u8, BincodeBatch, _) = consumer.recv_with_meta()?;
                    if let Some(lat_ns) = meta.one_way_latency_ns() {
                        latency.record(lat_ns);
                    }
                    checksum_payloads(&b.0)
                }
                "rkyv" => {
                    let (_, b, meta): (u8, RkyvBatch, _) = consumer.recv_with_meta()?;
                    if let Some(lat_ns) = meta.one_way_latency_ns() {
                        latency.record(lat_ns);
                    }
                    checksum_payloads(&b.0)
                }
                "flatbuf" => {
                    let (_, b, meta): (u8, FlatbufBatch, _) = consumer.recv_with_meta()?;
                    if let Some(lat_ns) = meta.one_way_latency_ns() {
                        latency.record(lat_ns);
                    }
                    checksum_payloads(&b.0)
                }
                other => return Err(format!("unknown codec: {other}").into()),
            };
            black_box(payload_sum);
            checksum = checksum.wrapping_add(payload_sum);
            consumed += 1;
        }

        let elapsed = start.elapsed();
        let mut output =
            ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, encoded_bytes, checksum);
        if let Some(stats) = latency.stats() {
            output = output.with_latency(stats);
        }
        println!("{}", serde_json::to_string(&output)?);
        coord.signal_consumer_done(consumed as i64);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Scenario {
    codec: &'static str,
    batch_size: usize,
    messages: u64,
    buffer: usize,
    consumers: usize,
    phase_timing: bool,
    target_rate: u64,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "codec_e2e_shm"
    }

    fn scenario_name(&self) -> String {
        let suffix = if self.phase_timing { "_phase" } else { "" };
        format!(
            "codec_e2e_1p{}c_{}seq_{}{}",
            self.consumers, self.batch_size, self.codec, suffix
        )
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        "typed"
    }

    fn codec(&self) -> Option<&str> {
        Some(self.codec)
    }

    fn message_size_bytes(&self) -> usize {
        encoded_len(self.codec, &make_payloads(self.batch_size))
    }

    fn buffer_depth(&self) -> usize {
        self.buffer
    }

    fn num_messages(&self) -> u64 {
        self.messages
    }

    fn num_consumers(&self) -> usize {
        self.consumers
    }

    fn throughput_unit(&self) -> &str {
        "msgs/s"
    }

    fn producer_label(&self) -> String {
        format!(
            "{}/{}/{}c prod",
            self.codec, self.batch_size, self.consumers
        )
    }

    fn consumer_label(&self, consumer_id: usize) -> String {
        if self.consumers == 1 {
            format!("{}/{} cons", self.codec, self.batch_size)
        } else {
            format!(
                "{}/{}/{}c cons{}",
                self.codec, self.batch_size, self.consumers, consumer_id
            )
        }
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let segment = unique_shm_segment(&format!(
            "{}_{}_{}c",
            self.codec, self.batch_size, self.consumers
        ));
        let mut envs = vec![
            ("BENCHMARK_SEGMENT_NAME", segment.clone()),
            ("BENCH_CODEC", self.codec.to_string()),
            ("BENCH_BATCH_SIZE", self.batch_size.to_string()),
            ("BENCH_MESSAGES", self.messages.to_string()),
            ("BENCH_BUFFER_DEPTH", self.buffer.to_string()),
            ("BENCH_NUM_CONSUMERS", self.consumers.to_string()),
        ];
        if self.phase_timing {
            envs.push(("BENCH_PHASE_TIMING", "1".to_string()));
        }
        if self.target_rate > 0 {
            envs.push(("BENCH_TARGET_RATE", self.target_rate.to_string()));
        }

        let producer = spawn_child(exe, "codec_producer", &envs);
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut consumer_envs = envs.clone();
                consumer_envs.push(("CONSUMER_ID", consumer_id.to_string()));
                spawn_child(exe, "codec_consumer", &consumer_envs)
            })
            .collect();
        Ok(ScenarioChildren::new(producer, consumers))
    }

    fn aggregate_latency(
        &self,
        consumers: &[ConsumerOutput],
    ) -> Option<perf_bench::latency::LatencyStats> {
        consumers
            .iter()
            .filter_map(|entry| entry.latency.clone())
            .max_by_key(|stats| stats.p99_ns)
    }

    fn print_summary_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        latency: Option<&perf_bench::latency::LatencyStats>,
    ) {
        let latency_suffix = latency
            .map(|stats| format!("  {}", stats.summary()))
            .unwrap_or_default();
        if self.phase_timing {
            let consumer = consumers.first().expect("consumer metrics");
            let prod_timing = producer
                .phase_timing
                .as_ref()
                .expect("producer phase timing missing");
            let cons_timing = consumer
                .phase_timing
                .as_ref()
                .expect("consumer phase timing missing");
            let encode_us = prod_timing.encode_avg_ns.unwrap_or(0.0) / 1000.0;
            let write_us = prod_timing.transport_write_avg_ns.unwrap_or(0.0) / 1000.0;
            let read_us = cons_timing.transport_read_avg_ns.unwrap_or(0.0) / 1000.0;
            let decode_us = cons_timing.decode_avg_ns.unwrap_or(0.0) / 1000.0;
            println!(
                "  codec={:<8} batch={:<3}  encode: {:>8.1}μs  write: {:>8.1}μs  read: {:>8.1}μs  decode: {:>8.1}μs  | prod: {:>8} cons: {:>8}{}",
                self.codec,
                self.batch_size,
                encode_us,
                write_us,
                read_us,
                decode_us,
                format_throughput(producer.throughput_ops_sec),
                format_throughput(consumer.throughput_ops_sec),
                latency_suffix,
            );
        } else {
            let avg_consumer_ops = self.average_consumer_ops(consumers);
            let mode_label = if self.target_rate > 0 {
                format!("co@{}", format_throughput(self.target_rate as f64))
            } else {
                "tput".to_string()
            };
            println!(
                "  codec={:<8} batch={:<3} consumers={:<2} mode={:<10} producer: {:>10} msgs/s  avg cons: {:>10} msgs/s{}",
                self.codec,
                self.batch_size,
                self.consumers,
                mode_label,
                format_throughput(producer.throughput_ops_sec),
                format_throughput(avg_consumer_ops),
                latency_suffix,
            );
        }
    }

    fn build_result_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        latency: Option<perf_bench::latency::LatencyStats>,
    ) -> reporting::BenchResult {
        let avg_consumer_ops = self.average_consumer_ops(consumers);
        let mut result = reporting::make_result(
            "codec_e2e_shm",
            &self.scenario_name(),
            "shm",
            "typed",
            Some(self.codec),
            "BusySpin",
            self.message_size_bytes(),
            self.buffer,
            self.messages,
            0,
            self.consumers,
            producer.throughput_ops_sec,
            avg_consumer_ops,
            latency,
        );
        if self.phase_timing {
            result.measurement_mode = "batch_timing".to_string();
        } else if self.target_rate > 0 {
            result.measurement_mode = format!("co_aware@{}", self.target_rate);
        }
        result
    }
}

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("codec_producer", producer_process),
    harness::ChildRole::new("codec_consumer", consumer_process),
];

struct CodecE2eShmBench;

impl harness::BenchHarness for CodecE2eShmBench {
    fn bench_name(&self) -> &'static str {
        "codec_e2e_shm"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let mode = args
            .windows(2)
            .find(|w| w[0] == "--mode")
            .map(|w| w[1].as_str())
            .unwrap_or("throughput");
        let codec_filter = args
            .windows(2)
            .find(|w| w[0] == "--codec")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let batch_filter = args
            .windows(2)
            .find(|w| w[0] == "--batch")
            .map(|w| w[1].parse::<usize>().expect("batch filter"));
        let consumers_filter = args
            .windows(2)
            .find(|w| w[0] == "--consumers")
            .map(|w| w[1].parse::<usize>().expect("consumer filter"));
        let target_rate = args
            .windows(2)
            .find(|w| w[0] == "--target-rate")
            .map(|w| w[1].parse::<u64>().expect("target rate"));
        if !matches!(mode, "throughput" | "phase_timing" | "co") {
            return Err(format!("unsupported mode: {mode}").into());
        }
        if mode == "co" && target_rate.is_none() {
            return Err("--mode co requires --target-rate".into());
        }
        if mode != "co" && target_rate.is_some() {
            return Err("--target-rate requires --mode co".into());
        }
        let phase_timing = mode == "phase_timing";
        let target_rate = target_rate.unwrap_or(0);
        let output_args = reporting::ReportOutputArgs::from_args(args);

        if !output_args.json_mode {
            println!("=== Codec E2E SHM Benchmark ===");
            println!("Transport: TypedTransport over SHM");
            println!(
                "Mode: {}",
                if phase_timing {
                    "phase_timing (encode/transport/decode)"
                } else if target_rate > 0 {
                    "co_aware"
                } else {
                    "throughput"
                }
            );
            if target_rate > 0 {
                println!("Target rate: {} msgs/s", target_rate);
            }
            println!("Payload: Vec<TestPayload> with Sequence-like Vec fields");
            println!();
        }

        let batches = [(8usize, 50_000u64), (64, 20_000), (256, 10_000)];
        let mut report = BenchReport::new();
        let consumer_counts: &[usize] = if phase_timing {
            &[1]
        } else {
            &TARGET_CONSUMERS
        };
        for (batch_size, messages) in batches {
            if batch_filter.is_some_and(|batch| batch != batch_size) {
                continue;
            }
            for codec in ["bincode", "rkyv", "flatbuf"] {
                if codec_filter != "all" && codec_filter != codec {
                    continue;
                }
                for &consumers in consumer_counts {
                    if consumers_filter.is_some_and(|value| value != consumers) {
                        continue;
                    }
                    report.add(
                        Scenario {
                            codec,
                            batch_size,
                            messages,
                            buffer: scaled_buffer(consumers),
                            consumers,
                            phase_timing,
                            target_rate,
                        }
                        .run_benchmark()?,
                    );
                }
            }
        }

        reporting::emit_report(
            &report,
            &output_args,
            Some(reporting::ReportView::Summary),
            None,
            None,
        );
        Ok(())
    }
}

perf_bench::myelon_bench_main!(CodecE2eShmBench);
