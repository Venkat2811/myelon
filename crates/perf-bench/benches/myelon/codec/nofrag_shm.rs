//! Codec benchmark WITHOUT fragmentation — ring slot sized to fit encoded payload.
//!
//! This is the production-representative number: raw disruptor ring with slot
//! size >= encoded payload, no FramedTransport, no fragmentation overhead.
//! Isolates pure codec overhead + raw ring transit.
//!
//! Slot sizes: 8KB (batch=8), 64KB (batch=64), 256KB (batch=256)
//!
//! Run: cargo bench -p myelon-bench --bench codec_nofrag_shm
//! Single batch: cargo bench -p myelon-bench --bench codec_nofrag_shm -- --batch 256

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use myelon::codec::Codec;
use perf_bench::codec_payloads::{
    checksum_payloads, make_payloads, BincodeBatch, FlatbufBatch, RkyvBatch,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::{format_throughput, nanos_now};
use perf_bench::harness::{self, ConsumerOutput, IpcBenchmark, ProducerOutput, ScenarioChildren};
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, BenchReport};
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

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
            let segment = env::var("BENCHMARK_SEGMENT_NAME").expect("BENCHMARK_SEGMENT_NAME");
            let codec = env::var("BENCH_CODEC").expect("BENCH_CODEC");
            let batch_size = harness::read_env_usize("BENCH_BATCH_SIZE", 8);
            let messages = harness::read_env_u64("BENCH_MESSAGES", 50_000);
            let buffer_depth = harness::read_env_usize("BENCH_BUFFER_DEPTH", 4096);

            let payloads = make_payloads(batch_size);

            let mut producer = build_shared_single_producer::<$slot_type>(&segment, buffer_depth)
                .enable_discovery(1)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$slot_type>::default())?;

            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
                return Err("timeout waiting for consumer".into());
            }
            for _ in 0..20 {
                let _ = producer.min_gating_sequence();
                std::thread::sleep(Duration::from_millis(2));
            }

            let mut encode_ns = 0u64;
            let mut transport_ns = 0u64;

            macro_rules! run_loop {
                ($batch:expr) => {
                    for _ in 0..messages {
                        let t0 = Instant::now();
                        let encoded = $batch.encode()?;
                        let encoded_bytes: &[u8] = encoded.as_ref();
                        let t1 = Instant::now();
                        let len = encoded_bytes.len();
                        assert!(
                            len <= $slot_data_len,
                            "payload {}B exceeds slot {}B",
                            len,
                            $slot_data_len
                        );
                        producer.publish(|slot| {
                            slot.len = len as u32;
                            slot.timestamp = nanos_now();
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

            let mut output = harness::ProducerOutput::from_elapsed(
                messages,
                elapsed,
                std::mem::size_of::<$slot_type>(),
            );
            output.phase_timing = Some(harness::PhaseTiming {
                encode_avg_ns: Some(encode_ns as f64 / messages as f64),
                transport_write_avg_ns: Some(transport_ns as f64 / messages as f64),
                transport_read_avg_ns: None,
                decode_avg_ns: None,
            });
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_producer_done(messages as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
            Ok(())
        }

        fn $consumer_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = env::var("BENCHMARK_SEGMENT_NAME").expect("BENCHMARK_SEGMENT_NAME");
            let codec = env::var("BENCH_CODEC").expect("BENCH_CODEC");
            let consumer_id = harness::read_env_usize("BENCH_CONSUMER_ID", 0);
            let messages = harness::read_env_u64("BENCH_MESSAGES", 50_000);
            let buffer_depth = harness::read_env_usize("BENCH_BUFFER_DEPTH", 4096);

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
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.elapsed();

            let timing = harness::PhaseTiming {
                encode_avg_ns: None,
                transport_write_avg_ns: None,
                transport_read_avg_ns: None,
                decode_avg_ns: Some(decode_ns as f64 / consumed as f64),
            };
            let output = if let Some(stats) = latency.stats() {
                harness::ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$slot_type>(),
                    checksum,
                )
                .with_latency(stats)
                .with_phase_timing(timing)
            } else {
                harness::ConsumerOutput::from_elapsed(
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
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "codec_nofrag_shm"
    }

    fn scenario_name(&self) -> String {
        format!("nofrag_{}seq_{}", self.batch_size, self.codec)
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        "raw_ring+codec"
    }

    fn codec(&self) -> Option<&str> {
        Some(self.codec)
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
        1
    }

    fn throughput_unit(&self) -> &str {
        "ops/s"
    }

    fn producer_label(&self) -> String {
        format!("{}/{} prod", self.codec, self.batch_size)
    }

    fn consumer_label(&self, _consumer_id: usize) -> String {
        format!("{}/{} cons", self.codec, self.batch_size)
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let segment =
            harness::unique_shm_segment(&format!("nf_{}_{}", self.codec, self.batch_size));
        let envs = vec![
            ("BENCHMARK_SEGMENT_NAME", segment.clone()),
            ("BENCH_CODEC", self.codec.to_string()),
            ("BENCH_BATCH_SIZE", self.batch_size.to_string()),
            ("BENCH_MESSAGES", self.messages.to_string()),
            ("BENCH_BUFFER_DEPTH", self.buffer_depth.to_string()),
        ];

        let producer = harness::spawn_child(exe, self.producer_role(), &envs);
        let mut consumer_envs = envs.clone();
        consumer_envs.push(("BENCH_CONSUMER_ID", "0".to_string()));
        let consumer = harness::spawn_child(exe, self.consumer_role(), &consumer_envs);
        Ok(ScenarioChildren::new(producer, vec![consumer]))
    }

    fn print_summary_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        latency: Option<&perf_bench::latency::LatencyStats>,
    ) {
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
        let decode_us = cons_timing.decode_avg_ns.unwrap_or(0.0) / 1000.0;
        let lat_str = latency
            .map(|stats| stats.summary())
            .unwrap_or_else(|| "-".to_string());

        println!(
            "  {:<8} batch={:<3} slot={:>4}KB  enc: {:>6.1}μs  write: {:>6.1}μs  dec: {:>6.1}μs  | prod: {:>8}  cons: {:>8}  {}",
            self.codec,
            self.batch_size,
            self.slot_bytes() / 1024,
            encode_us,
            write_us,
            decode_us,
            format_throughput(producer.throughput_ops_sec),
            format_throughput(consumer.throughput_ops_sec),
            lat_str,
        );
    }

    fn build_result_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        latency: Option<perf_bench::latency::LatencyStats>,
    ) -> reporting::BenchResult {
        let consumer = consumers.first().expect("consumer metrics");
        reporting::make_result(
            "codec_nofrag_shm",
            &self.scenario_name(),
            "shm",
            "raw_ring+codec",
            Some(self.codec),
            "BusySpin",
            self.slot_bytes(),
            self.buffer_depth,
            self.messages,
            0,
            1,
            producer.throughput_ops_sec,
            consumer.throughput_ops_sec,
            latency,
        )
    }
}

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("nf_prod_8k", producer_8k),
    harness::ChildRole::new("nf_cons_8k", consumer_8k),
    harness::ChildRole::new("nf_prod_64k", producer_64k),
    harness::ChildRole::new("nf_cons_64k", consumer_64k),
    harness::ChildRole::new("nf_prod_256k", producer_256k),
    harness::ChildRole::new("nf_cons_256k", consumer_256k),
];

struct CodecNoFragShmBench;

impl harness::BenchHarness for CodecNoFragShmBench {
    fn bench_name(&self) -> &'static str {
        "codec_nofrag_shm"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let batch_arg = args
            .windows(2)
            .find(|w| w[0] == "--batch")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let output_args = reporting::ReportOutputArgs::from_args(args);

        if !output_args.json_mode {
            println!("=== Codec No-Frag SHM Benchmark ===");
            println!("Transport: raw disruptor ring (slot sized to payload, ZERO fragmentation)");
            println!("This is the production-representative number.");
            println!();
        }

        let scenarios = [
            (8usize, 100_000u64, 16384usize),
            (64, 50_000, 4096),
            (256, 20_000, 2048),
        ];

        let mut report = BenchReport::new();
        for (batch_size, messages, buffer_depth) in scenarios {
            if batch_arg == "all" || batch_arg.parse::<usize>().ok() == Some(batch_size) {
                for codec in ["bincode", "rkyv", "flatbuf"] {
                    report.add(
                        Scenario {
                            codec,
                            batch_size,
                            messages,
                            buffer_depth,
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

perf_bench::myelon_bench_main!(CodecNoFragShmBench);
