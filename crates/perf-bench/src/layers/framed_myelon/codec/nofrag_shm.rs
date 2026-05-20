//! Codec benchmark WITHOUT fragmentation — ring slot sized to fit encoded payload.
//!
//! This is the production-representative number: raw disruptor ring with slot
//! size >= encoded payload, no `FramedTransport`, no fragmentation overhead.
//! Isolates pure codec overhead + raw ring transit.
//!
//! Slot sizes: 8KB (batch=8), 64KB (batch=64), 256KB (batch=256)
//!
//! Run: cargo bench -p myelon-bench --bench `codec_nofrag_shm`
//! Single batch: cargo bench -p myelon-bench --bench `codec_nofrag_shm` -- --batch 256

use crate::infra::coordination::BenchmarkCoordination;
use crate::infra::events::{format_throughput, nanos_now};
use crate::infra::latency::LatencyRecorder;
use crate::infra::output::reporting::{self, BenchReport};
use crate::infra::{
    self, discovery_scan_rounds, read_env_string, segment_from_env, warm_discovery_scans,
    ConsumerOutput, IpcBenchmark, ProducerOutput, ScenarioChildren,
};
use crate::layers::framed_myelon::codec::payloads::{
    checksum_payloads, make_payloads, BincodeBatch, FlatbufBatch, RkyvBatch,
};
use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use myelon::codec::Codec;
use std::hint::black_box;
use std::time::{Duration, Instant};

fn scaled_buffer_depth(base_buffer: usize, consumers: usize) -> usize {
    let min_depth = consumers.next_power_of_two().max(1) * 256;
    base_buffer.max(min_depth).next_power_of_two()
}

// ============================================================
// Slot types — each sized to fit the encoded payload with margin
// ============================================================

/// 8KB slot for batch=8 (~4.5KB encoded)
#[repr(C)]
#[derive(Clone, Copy)]
struct Slot8K {
    len: u32,
    timestamp: u64,
    data: [u8; 8192 - 12],
}
impl Default for Slot8K {
    fn default() -> Self {
        Self {
            len: 0,
            timestamp: 0,
            data: [0; 8192 - 12],
        }
    }
}

/// 64KB slot for batch=64 (~37KB encoded)
#[repr(C)]
#[derive(Clone, Copy)]
struct Slot64K {
    len: u32,
    timestamp: u64,
    data: [u8; 65536 - 12],
}
impl Default for Slot64K {
    fn default() -> Self {
        Self {
            len: 0,
            timestamp: 0,
            data: [0; 65536 - 12],
        }
    }
}

/// 256KB slot for batch=256 (~145KB encoded)
#[repr(C)]
#[derive(Clone, Copy)]
struct Slot256K {
    len: u32,
    timestamp: u64,
    data: [u8; 262144 - 12],
}
impl Default for Slot256K {
    fn default() -> Self {
        Self {
            len: 0,
            timestamp: 0,
            data: [0; 262144 - 12],
        }
    }
}

// ============================================================
// Generic producer/consumer using macro to handle different slot sizes
// ============================================================

macro_rules! impl_nofrag_bench {
    ($slot_type:ty, $slot_data_len:expr, $producer_fn:ident, $consumer_fn:ident) => {
        fn $producer_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env(crate::infra::env::SEGMENT_NAME);
            let codec = read_env_string(crate::infra::env::CODEC, "rkyv");
            let batch_size = infra::read_env_usize(crate::infra::env::BATCH_SIZE, 8);
            let messages = infra::read_env_u64(crate::infra::env::MESSAGES, 50_000);
            let buffer_depth = infra::read_env_usize(crate::infra::env::BUFFER_DEPTH, 4096);
            let num_consumers = infra::read_env_usize(crate::infra::env::NUM_CONSUMERS, 1);
            let target_rate = infra::read_env_u64(crate::infra::env::TARGET_RATE, 0);

            let payloads = make_payloads(batch_size);

            let mut producer = build_shared_single_producer::<$slot_type>(&segment, buffer_depth)
                .enable_discovery(num_consumers)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$slot_type>::default())?;

            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }
            warm_discovery_scans(
                || producer.min_gating_sequence(),
                discovery_scan_rounds(num_consumers),
            );

            let mut encode_ns = 0u64;
            let mut transport_ns = 0u64;
            let base_ns = nanos_now();
            let interval_ns = if target_rate > 0 {
                1_000_000_000u64 / target_rate
            } else {
                0
            };

            macro_rules! run_loop {
                ($batch:expr) => {
                    for i in 0..messages {
                        let intended_ns = if target_rate > 0 {
                            Some(base_ns.saturating_add(i.saturating_mul(interval_ns)))
                        } else {
                            None
                        };
                        let t0 = Instant::now();
                        let encoded = $batch.encode()?;
                        let encoded_bytes: &[u8] = encoded.as_ref();
                        let t1 = Instant::now();
                        if let Some(intended_ns) = intended_ns {
                            while nanos_now() < intended_ns {
                                std::hint::spin_loop();
                            }
                        }
                        let len = encoded_bytes.len();
                        assert!(
                            len <= $slot_data_len,
                            "payload {}B exceeds slot {}B",
                            len,
                            $slot_data_len
                        );
                        producer.publish(|slot| {
                            slot.len = len as u32;
                            slot.timestamp = intended_ns.unwrap_or_else(nanos_now);
                            slot.data[..len].copy_from_slice(encoded_bytes);
                        });
                        let t2 = Instant::now();
                        encode_ns += (t1 - t0).as_nanos() as u64;
                        transport_ns += (t2 - t1).as_nanos() as u64;
                    }
                };
            }

            let start = Instant::now();
            match codec.as_str() {
                "bincode" => {
                    let b = BincodeBatch(payloads);
                    run_loop!(b);
                }
                "rkyv" => {
                    let b = RkyvBatch(payloads);
                    run_loop!(b);
                }
                "flatbuf" => {
                    let b = FlatbufBatch(payloads);
                    run_loop!(b);
                }
                other => return Err(format!("unknown codec: {other}").into()),
            }
            let elapsed = start.elapsed();

            let mut output = infra::ProducerOutput::from_elapsed(
                messages,
                elapsed,
                std::mem::size_of::<$slot_type>(),
            );
            output.phase_timing = Some(infra::PhaseTiming {
                encode_avg_ns: Some(encode_ns as f64 / messages as f64),
                transport_write_avg_ns: Some(transport_ns as f64 / messages as f64),
                transport_read_avg_ns: None,
                decode_avg_ns: None,
            });
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_producer_done(messages as i64);
            coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
            Ok(())
        }

        fn $consumer_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env(crate::infra::env::SEGMENT_NAME);
            let codec = read_env_string(crate::infra::env::CODEC, "rkyv");
            let consumer_id = infra::read_env_usize(crate::infra::env::CONSUMER_ID, 0);
            let messages = infra::read_env_u64(crate::infra::env::MESSAGES, 50_000);
            let buffer_depth = infra::read_env_usize(crate::infra::env::BUFFER_DEPTH, 4096);

            let coord =
                BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let config = SharedMemoryConfig {
                name: segment,
                buffer_size: buffer_depth,
                element_size: std::mem::size_of::<$slot_type>(),
                create: false,
            };
            let mut consumer =
                SharedDisruptorBuilder::<$slot_type>::new(config).build_consumer()?;
            coord.signal_consumer_ready();

            let mut latency = LatencyRecorder::default_range();
            let mut decode_ns = 0u64;
            let start = Instant::now();
            let deadline = infra::spin_deadline();
            let mut consumed = 0u64;
            let mut checksum = 0u64;

            while consumed < messages {
                consumer.process_available(|slot, _seq| {
                    let now = nanos_now();
                    if slot.timestamp > 0 {
                        latency.record_delta(slot.timestamp, now);
                    }
                    let len = slot.len as usize;
                    let bytes = &slot.data[..len];
                    let t0 = Instant::now();
                    let payload_sum = match codec.as_str() {
                        "bincode" => {
                            let batch = BincodeBatch::decode(bytes).unwrap();
                            checksum_payloads(&batch.0)
                        }
                        "rkyv" => {
                            let batch = RkyvBatch::decode(bytes).unwrap();
                            checksum_payloads(&batch.0)
                        }
                        "flatbuf" => {
                            let batch = FlatbufBatch::decode(bytes).unwrap();
                            checksum_payloads(&batch.0)
                        }
                        _ => 0,
                    };
                    let t1 = Instant::now();
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    decode_ns += (t1 - t0).as_nanos() as u64;
                    consumed += 1;
                });
                if consumed < messages {
                    infra::check_deadline(deadline, concat!(stringify!($consumer_fn), " measured"));
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.elapsed();

            let timing = infra::PhaseTiming {
                encode_avg_ns: None,
                transport_write_avg_ns: None,
                transport_read_avg_ns: None,
                decode_avg_ns: Some(decode_ns as f64 / consumed as f64),
            };
            let output = if let Some(stats) = latency.stats() {
                infra::ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$slot_type>(),
                    checksum,
                )
                .with_latency(stats)
                .with_phase_timing(timing)
            } else {
                infra::ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$slot_type>(),
                    checksum,
                )
                .with_phase_timing(timing)
            };
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

impl_nofrag_bench!(Slot8K, 8180, producer_8k, consumer_8k);
impl_nofrag_bench!(Slot64K, 65524, producer_64k, consumer_64k);
impl_nofrag_bench!(Slot256K, 262132, producer_256k, consumer_256k);

// ============================================================
// Orchestrator
// ============================================================

#[derive(Clone, Copy)]
struct Scenario {
    codec: &'static str,
    batch_size: usize,
    messages: u64,
    buffer_depth: usize,
    consumers: usize,
    target_rate: u64,
}

impl Scenario {
    fn slot_bytes(&self) -> usize {
        match self.batch_size {
            8 => 8192,
            64 => 65536,
            _ => 262144,
        }
    }

    fn producer_role(&self) -> &'static str {
        match self.batch_size {
            8 => "nf_prod_8k",
            64 => "nf_prod_64k",
            256 => "nf_prod_256k",
            _ => panic!("unsupported batch size: {}", self.batch_size),
        }
    }

    fn consumer_role(&self) -> &'static str {
        match self.batch_size {
            8 => "nf_cons_8k",
            64 => "nf_cons_64k",
            256 => "nf_cons_256k",
            _ => panic!("unsupported batch size: {}", self.batch_size),
        }
    }

    fn avg_decode_us(&self, consumers: &[ConsumerOutput]) -> f64 {
        if consumers.is_empty() {
            return 0.0;
        }
        consumers
            .iter()
            .map(|consumer| {
                consumer
                    .phase_timing
                    .as_ref()
                    .and_then(|timing| timing.decode_avg_ns)
                    .unwrap_or(0.0)
            })
            .sum::<f64>()
            / consumers.len() as f64
            / 1000.0
    }
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "codec_nofrag_shm"
    }

    fn scenario_name(&self) -> String {
        format!(
            "nofrag_1p{}c_{}seq_{}",
            self.consumers, self.batch_size, self.codec
        )
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        "raw_ring+codec"
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        reporting::BenchTransportSpec::benchmark_shm(self.consumers)
            .with_zero_copy(matches!(self.codec, "rkyv" | "flatbuf"))
            .with_framing("none")
    }

    fn codec(&self) -> Option<&str> {
        Some(self.codec)
    }

    fn measurement_mode(&self) -> String {
        if self.target_rate > 0 {
            format!("co_aware@{}", self.target_rate)
        } else {
            "max_throughput".to_string()
        }
    }

    fn message_size_bytes(&self) -> usize {
        self.slot_bytes()
    }

    fn buffer_depth(&self) -> usize {
        self.buffer_depth
    }

    fn num_messages(&self) -> u64 {
        self.messages
    }

    fn num_consumers(&self) -> usize {
        self.consumers
    }

    fn throughput_unit(&self) -> &str {
        "ops/s"
    }

    fn producer_label(&self) -> String {
        format!("{}/{} prod", self.codec, self.batch_size)
    }

    fn consumer_label(&self, consumer_id: usize) -> String {
        format!("{}/{} cons{consumer_id}", self.codec, self.batch_size)
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, infra::BenchError> {
        let segment = infra::unique_shm_segment(&format!("nf_{}_{}", self.codec, self.batch_size));
        let envs = vec![
            (crate::infra::env::TARGET_RATE, self.target_rate.to_string()),
            (crate::infra::env::SEGMENT_NAME, segment.clone()),
            (crate::infra::env::CODEC, self.codec.to_string()),
            (crate::infra::env::BATCH_SIZE, self.batch_size.to_string()),
            (crate::infra::env::MESSAGES, self.messages.to_string()),
            (
                crate::infra::env::BUFFER_DEPTH,
                self.buffer_depth.to_string(),
            ),
            (crate::infra::env::NUM_CONSUMERS, self.consumers.to_string()),
        ];

        let producer = infra::spawn_child(exe, self.producer_role(), &envs);
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut consumer_envs = envs.clone();
                consumer_envs.push((crate::infra::env::CONSUMER_ID, consumer_id.to_string()));
                infra::spawn_child(exe, self.consumer_role(), &consumer_envs)
            })
            .collect();
        Ok(ScenarioChildren::new(producer, consumers))
    }

    fn aggregate_latency(
        &self,
        consumers: &[ConsumerOutput],
    ) -> Option<crate::infra::latency::LatencyStats> {
        consumers
            .iter()
            .filter_map(|entry| entry.latency.clone())
            .max_by_key(|stats| stats.p99_ns)
    }

    fn print_summary_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        latency: Option<&crate::infra::latency::LatencyStats>,
    ) {
        let prod_timing = producer
            .phase_timing
            .as_ref()
            .expect("producer phase timing missing");
        let encode_us = prod_timing.encode_avg_ns.unwrap_or(0.0) / 1000.0;
        let write_us = prod_timing.transport_write_avg_ns.unwrap_or(0.0) / 1000.0;
        let decode_us = self.avg_decode_us(consumers);
        let lat_str = latency
            .map(|stats| stats.summary())
            .unwrap_or_else(|| "-".to_string());
        let avg_consumer_ops = self.average_consumer_ops(consumers);
        let consumer_label = if self.consumers == 1 {
            "cons"
        } else {
            "avg cons"
        };
        let mode_label = if self.target_rate > 0 {
            format!("CO@{}", format_throughput(self.target_rate as f64))
        } else {
            "tput".to_string()
        };

        println!(
            "  {:<8} mode={:<10} cons={:<2} batch={:<3} slot={:>4}KB  enc: {:>6.1}μs  write: {:>6.1}μs  dec: {:>6.1}μs  | prod: {:>8}  {}: {:>8}  {}",
            self.codec,
            mode_label,
            self.consumers,
            self.batch_size,
            self.slot_bytes() / 1024,
            encode_us,
            write_us,
            decode_us,
            format_throughput(producer.throughput_ops_sec),
            consumer_label,
            format_throughput(avg_consumer_ops),
            lat_str,
        );
    }
}

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("nf_prod_8k", producer_8k),
    infra::ChildRole::new("nf_cons_8k", consumer_8k),
    infra::ChildRole::new("nf_prod_64k", producer_64k),
    infra::ChildRole::new("nf_cons_64k", consumer_64k),
    infra::ChildRole::new("nf_prod_256k", producer_256k),
    infra::ChildRole::new("nf_cons_256k", consumer_256k),
];

pub struct CodecNoFragShmBench;

impl infra::BenchHarness for CodecNoFragShmBench {
    fn bench_name(&self) -> &'static str {
        "codec_nofrag_shm"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let codec_arg = args
            .windows(2)
            .find(|w| w[0] == "--codec")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let batch_arg = args
            .windows(2)
            .find(|w| w[0] == "--batch")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let consumers_arg = args
            .windows(2)
            .find(|w| w[0] == "--consumers")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let mode = args
            .windows(2)
            .find(|w| w[0] == "--mode")
            .map(|w| w[1].as_str())
            .unwrap_or("throughput");
        let target_rate = args
            .windows(2)
            .find(|w| w[0] == "--target-rate")
            .map(|w| w[1].parse::<u64>().expect("target rate"));
        if !matches!(mode, "throughput" | "co") {
            return Err(format!("unsupported mode: {mode}").into());
        }
        if mode == "co" && target_rate.is_none() {
            return Err("--mode co requires --target-rate".into());
        }
        if mode != "co" && target_rate.is_some() {
            return Err("--target-rate requires --mode co".into());
        }
        let target_rate = target_rate.unwrap_or(0);
        let output_args = reporting::ReportOutputArgs::from_args(args);

        if !output_args.json_mode {
            println!("=== Codec No-Frag SHM Benchmark ===");
            println!("Transport: raw disruptor ring (slot sized to payload, ZERO fragmentation)");
            println!("This is the production-representative number.");
            println!(
                "Mode: {}",
                if target_rate > 0 {
                    "co_aware"
                } else {
                    "throughput"
                }
            );
            if target_rate > 0 {
                println!("Target rate: {} ops/s", target_rate);
            }
            println!();
        }

        let scenarios = [
            (8usize, 100_000u64, 16384usize),
            (64, 50_000, 4096),
            (256, 20_000, 2048),
        ];
        let consumer_counts = [1usize, 2, 4, 6, 8, 12];

        let mut report = BenchReport::new();
        for (batch_size, messages, buffer_depth) in scenarios {
            if batch_arg == "all" || batch_arg.parse::<usize>().ok() == Some(batch_size) {
                for consumers in consumer_counts {
                    let consumers_match = consumers_arg == "all"
                        || consumers_arg.parse::<usize>().ok() == Some(consumers);
                    if !consumers_match {
                        continue;
                    }
                    for codec in ["bincode", "rkyv", "flatbuf"] {
                        if codec_arg != "all" && codec_arg != codec {
                            continue;
                        }
                        report.add(
                            Scenario {
                                codec,
                                batch_size,
                                messages,
                                buffer_depth: scaled_buffer_depth(buffer_depth, consumers),
                                consumers,
                                target_rate,
                            }
                            .run_benchmark()?,
                        );
                    }
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
