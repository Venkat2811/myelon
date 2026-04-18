//! End-to-end Codec benchmark over SHM typed transport.
//!
//! Measures: struct -> Codec::encode -> TypedTransport publish -> ring ->
//!           TypedTransport consume -> Codec::decode -> struct access.
//!
//! This bench intentionally uses `TypedProducer` / `TypedConsumer` so it
//! measures the RFC 0012 API surface, not ad-hoc encode/decode calls.

use myelon::codec::Codec;
use myelon::transport::{FixedFrame, FramedTransportProducer, MyelonWaitStrategy};
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
use perf_bench::reporting::{self, BenchReport};
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;
const BUFFER_DEPTH: usize = 1024;

fn read_codec_env() -> (String, usize, u64, String) {
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
    (codec, batch_size, messages, segment)
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (codec, batch_size, messages, segment) = read_codec_env();
    let phase_timing = env::var("BENCH_PHASE_TIMING")
        .ok()
        .map_or(false, |v| v == "1");
    let payloads = make_payloads(batch_size);
    let encoded_bytes = encoded_len(&codec, &payloads);

    if phase_timing {
        // Phase timing: use FramedTransportProducer directly to time encode vs transport
        let mut producer = FramedTransportProducer::<Frame>::create(&segment, BUFFER_DEPTH)?;
        let coord = BenchmarkCoordination::create(&segment)?;
        if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
            return Err("timeout waiting for consumer".into());
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
        coord.wait_for_consumers_done(1, Duration::from_secs(30));
    } else {
        let mut producer = TypedProducer::<Frame>::create(&segment, BUFFER_DEPTH)?;
        let coord = BenchmarkCoordination::create(&segment)?;
        if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
            return Err("timeout waiting for consumer".into());
        }
        producer.discover_consumers(Duration::from_secs(3));

        let start = Instant::now();
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
        let elapsed = start.elapsed();
        let output = ProducerOutput::from_elapsed(messages, elapsed, encoded_bytes);
        println!("{}", serde_json::to_string(&output)?);
        coord.signal_producer_done(messages as i64);
        coord.wait_for_consumers_done(1, Duration::from_secs(30));
    }
    Ok(())
}

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (codec, batch_size, messages, segment) = read_codec_env();
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
        use myelon::transport::FramedTransportConsumer;
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut consumer = loop {
            match FramedTransportConsumer::<Frame>::attach(
                &segment,
                BUFFER_DEPTH,
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
        let start = Instant::now();
        let mut consumed = 0u64;
        let mut checksum = 0u64;

        while consumed < messages {
            let t0 = Instant::now();
            let (_kind, raw_bytes) = consumer.recv_message_blocking();
            let t1 = Instant::now();
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
        let output =
            ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, encoded_bytes, checksum)
                .with_phase_timing(timing);
        println!("{}", serde_json::to_string(&output)?);
        coord.signal_consumer_done(consumed as i64);
    } else {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut consumer = loop {
            match TypedConsumer::<Frame>::attach(
                &segment,
                BUFFER_DEPTH,
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

        let start = Instant::now();
        let mut consumed = 0u64;
        let mut checksum = 0u64;

        // Count-based termination: consume exactly `messages` to avoid deadlock
        // on recv_message_blocking after all messages consumed.
        while consumed < messages {
            let payload_sum = match codec.as_str() {
                "bincode" => {
                    let (_, b): (u8, BincodeBatch) = consumer.recv()?;
                    checksum_payloads(&b.0)
                }
                "rkyv" => {
                    let (_, b): (u8, RkyvBatch) = consumer.recv()?;
                    checksum_payloads(&b.0)
                }
                "flatbuf" => {
                    let (_, b): (u8, FlatbufBatch) = consumer.recv()?;
                    checksum_payloads(&b.0)
                }
                other => return Err(format!("unknown codec: {other}").into()),
            };
            black_box(payload_sum);
            checksum = checksum.wrapping_add(payload_sum);
            consumed += 1;
        }

        let elapsed = start.elapsed();
        let output =
            ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, encoded_bytes, checksum);
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
    phase_timing: bool,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "codec_e2e_shm"
    }

    fn scenario_name(&self) -> String {
        format!("codec_e2e_{}seq_{}", self.batch_size, self.codec)
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
        BUFFER_DEPTH
    }

    fn num_messages(&self) -> u64 {
        self.messages
    }

    fn num_consumers(&self) -> usize {
        1
    }

    fn throughput_unit(&self) -> &str {
        "msgs/s"
    }

    fn producer_label(&self) -> String {
        format!("{}/{} prod", self.codec, self.batch_size)
    }

    fn consumer_label(&self, _consumer_id: usize) -> String {
        format!("{}/{} cons", self.codec, self.batch_size)
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let segment = unique_shm_segment(&format!("{}_{}", self.codec, self.batch_size));
        let mut envs = vec![
            ("BENCHMARK_SEGMENT_NAME", segment.clone()),
            ("BENCH_CODEC", self.codec.to_string()),
            ("BENCH_BATCH_SIZE", self.batch_size.to_string()),
            ("BENCH_MESSAGES", self.messages.to_string()),
        ];
        if self.phase_timing {
            envs.push(("BENCH_PHASE_TIMING", "1".to_string()));
        }

        let producer = spawn_child(exe, "codec_producer", &envs);
        let consumer = spawn_child(exe, "codec_consumer", &envs);
        Ok(ScenarioChildren::new(producer, vec![consumer]))
    }

    fn print_summary_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        _latency: Option<&perf_bench::latency::LatencyStats>,
    ) {
        let consumer = consumers.first().expect("consumer metrics");
        if self.phase_timing {
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
                "  codec={:<8} batch={:<3}  encode: {:>8.1}μs  write: {:>8.1}μs  read: {:>8.1}μs  decode: {:>8.1}μs  | prod: {:>8} cons: {:>8}",
                self.codec,
                self.batch_size,
                encode_us,
                write_us,
                read_us,
                decode_us,
                format_throughput(producer.throughput_ops_sec),
                format_throughput(consumer.throughput_ops_sec),
            );
        } else {
            println!(
                "  codec={:<8} batch={:<3} producer: {:>10} msgs/s  consumer: {:>10} msgs/s",
                self.codec,
                self.batch_size,
                format_throughput(producer.throughput_ops_sec),
                format_throughput(consumer.throughput_ops_sec),
            );
        }
    }

    fn build_result_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        _latency: Option<perf_bench::latency::LatencyStats>,
    ) -> reporting::BenchResult {
        let consumer = consumers.first().expect("consumer metrics");
        reporting::make_result(
            "codec_e2e_shm",
            &self.scenario_name(),
            "shm",
            "typed",
            Some(self.codec),
            "BusySpin",
            self.message_size_bytes(),
            BUFFER_DEPTH,
            consumer.events_consumed,
            0,
            1,
            producer.throughput_ops_sec,
            consumer.throughput_ops_sec,
            None,
        )
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
        let phase_timing = mode == "phase_timing";
        let output_args = reporting::ReportOutputArgs::from_args(args);

        if !output_args.json_mode {
            println!("=== Codec E2E SHM Benchmark ===");
            println!("Transport: TypedTransport over SHM");
            println!(
                "Mode: {}",
                if phase_timing {
                    "phase_timing (encode/transport/decode)"
                } else {
                    "throughput"
                }
            );
            println!("Payload: Vec<TestPayload> with Sequence-like Vec fields");
            println!();
        }

        let scenarios = [(8usize, 50_000u64), (64, 20_000), (256, 10_000)];
        let mut report = BenchReport::new();
        for (batch_size, messages) in scenarios {
            for codec in ["bincode", "rkyv", "flatbuf"] {
                report.add(
                    Scenario {
                        codec,
                        batch_size,
                        messages,
                        phase_timing,
                    }
                    .run_benchmark()?,
                );
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
