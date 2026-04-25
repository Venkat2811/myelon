use super::model::{
    BackendKind, CodecKind, ConsumerAggregate, ConsumerMetrics, CoordinationKind, DerivedMetrics,
    DiscoveryKind, FramingKind, LayoutOutcome, MeasurementKind, ProducerMetrics, ReportBundle,
    RunMetadata, ScenarioConfig, ScenarioFamily, ScenarioIdentity, ScenarioOutcome, ScenarioReport,
    ThroughputOutcome, TransportSpec, VerificationMetrics, WaitStrategyKind, WorkloadConfig,
    ZeroCopyKind,
};
use crate::infra::output::reporting::{
    BenchConfig, BenchConsumerResult, BenchMetadata, BenchReport, BenchResult, BenchResults,
    LayoutValidationMetrics,
};

pub trait ReportBundleCompat {
    fn to_report(&self) -> ReportBundle;
}

pub trait BenchReportCompat {
    fn to_report_v1_compat(&self) -> BenchReport;
}

impl ReportBundleCompat for BenchReport {
    fn to_report(&self) -> ReportBundle {
        let finalized = self.finalized();
        ReportBundle {
            metadata: run_metadata(&finalized.metadata),
            scenarios: finalized.results.iter().map(adapt_scenario).collect(),
        }
    }
}

impl BenchReportCompat for ReportBundle {
    fn to_report_v1_compat(&self) -> BenchReport {
        BenchReport {
            metadata: bench_metadata(&self.metadata),
            results: self.scenarios.iter().map(adapt_scenario_back).collect(),
        }
    }
}

fn adapt_scenario(result: &BenchResult) -> ScenarioReport {
    let (suite, benchmark) = parse_benchmark_id(&result.benchmark_id);
    ScenarioReport {
        identity: ScenarioIdentity {
            benchmark_id: result.benchmark_id.clone(),
            suite,
            benchmark: benchmark.clone(),
            family: infer_family(&benchmark),
            scenario: result.scenario.clone(),
            backend: parse_backend(&result.backend),
            layer: result.layer.clone(),
            codec: result.codec.as_deref().map(parse_codec),
        },
        config: ScenarioConfig {
            measurement: parse_measurement(&result.measurement_mode),
            transport: TransportSpec {
                wait_strategy: parse_wait_strategy(&result.wait_strategy),
                coordination: result
                    .config
                    .coordination
                    .as_deref()
                    .map(parse_coordination),
                discovery: result.config.discovery_mode.as_deref().map(parse_discovery),
                framing: result.config.framing.as_deref().map(parse_framing),
                zero_copy: result.config.zero_copy.map(parse_zero_copy),
            },
            workload: WorkloadConfig {
                message_size_bytes: result.config.message_size_bytes,
                payload_bytes: result.config.payload_bytes,
                buffer_depth: result.config.buffer_depth,
                num_messages: result.config.num_messages,
                warmup_messages: result.config.warmup_messages,
                num_producers: result.config.num_producers,
                num_consumers: result.config.num_consumers,
                batch_size: batch_size_from_result(result),
            },
        },
        outcome: adapt_outcome(result),
        metadata: run_metadata(&result.metadata),
    }
}

fn adapt_outcome(result: &BenchResult) -> ScenarioOutcome {
    if let Some(layout) = &result.results.layout_validation {
        return ScenarioOutcome::Layout(LayoutOutcome {
            avg_ns: layout.avg_ns,
            budget_ns: layout.budget_ns,
            iterations: layout.iterations,
            pass: layout.pass,
        });
    }

    ScenarioOutcome::Throughput(ThroughputOutcome {
        producer: ProducerMetrics {
            throughput_ops_sec: result.results.producer_throughput_ops_sec,
            bandwidth_bytes_sec: result.results.producer_bandwidth_bytes_sec,
            data_rate_gbps: result.results.producer_data_rate_gbps,
            data_rate_mbps: result.results.data_rate_mbps,
        },
        consumers: ConsumerAggregate {
            average_throughput_ops_sec: result.results.consumer_throughput_ops_sec,
            min_throughput_ops_sec: result.results.consumer_min_throughput_ops_sec,
            max_throughput_ops_sec: result.results.consumer_max_throughput_ops_sec,
            total_throughput_ops_sec: result.results.consumer_total_throughput_ops_sec,
            average_bandwidth_bytes_sec: result.results.consumer_avg_bandwidth_bytes_sec,
            average_data_rate_gbps: result.results.consumer_avg_data_rate_gbps,
            checksum_total: result.results.consumer_checksum_total,
        },
        per_consumer: result
            .results
            .per_consumer
            .iter()
            .map(|entry| ConsumerMetrics {
                consumer_id: entry.consumer_id,
                throughput_ops_sec: entry.throughput_ops_sec,
                events_consumed: entry.events_consumed,
                bandwidth_bytes_sec: entry.bandwidth_bytes_sec,
                data_rate_gbps: entry.data_rate_gbps,
                checksum: entry.checksum,
                latency: entry.latency.clone(),
                phase_timing: entry.phase_timing.clone(),
            })
            .collect(),
        verification: VerificationMetrics {
            passed: result.results.verification_passed,
            messages_processed: result.results.messages_processed,
        },
        latency: result.latency.clone(),
        phase_timing: result.results.phase_timing.clone(),
        derived: DerivedMetrics {
            pct_of_raw_ring: result.results.pct_of_raw_ring,
            delta_vs_raw_ring_pct: result.results.delta_vs_raw_ring_pct,
            speedup_vs_bincode: result.results.speedup_vs_bincode,
            delta_vs_bincode_pct: result.results.delta_vs_bincode_pct,
            access_avg_ns: result.results.access_avg_ns,
            access_vs_decode_speedup: result.results.access_vs_decode_speedup,
            alloc_count: result.results.alloc_count,
            alloc_bytes: result.results.alloc_bytes,
            hw_bandwidth_limit_gbps: result.results.hw_bandwidth_limit_gbps,
            hw_efficiency_pct: result.results.hw_efficiency_pct,
        },
    })
}

fn run_metadata(metadata: &BenchMetadata) -> RunMetadata {
    RunMetadata {
        timestamp: metadata.timestamp.clone(),
        platform: metadata.platform.clone(),
        cpu: metadata.cpu.clone(),
        git_commit: metadata.git_commit.clone(),
        rust_version: metadata.rust_version.clone(),
    }
}

fn bench_metadata(metadata: &RunMetadata) -> BenchMetadata {
    BenchMetadata {
        timestamp: metadata.timestamp.clone(),
        platform: metadata.platform.clone(),
        cpu: metadata.cpu.clone(),
        git_commit: metadata.git_commit.clone(),
        rust_version: metadata.rust_version.clone(),
    }
}

fn adapt_scenario_back(scenario: &ScenarioReport) -> BenchResult {
    match &scenario.outcome {
        ScenarioOutcome::Throughput(outcome) => BenchResult {
            benchmark_id: scenario.identity.benchmark_id.clone(),
            scenario: scenario.identity.scenario.clone(),
            backend: backend_name(&scenario.identity.backend),
            layer: scenario.identity.layer.clone(),
            codec: scenario.identity.codec.as_ref().map(codec_name),
            measurement_mode: measurement_name(&scenario.config.measurement),
            wait_strategy: wait_strategy_name(&scenario.config.transport.wait_strategy),
            config: BenchConfig {
                message_size_bytes: scenario.config.workload.message_size_bytes,
                payload_bytes: scenario.config.workload.payload_bytes,
                buffer_depth: scenario.config.workload.buffer_depth,
                num_messages: scenario.config.workload.num_messages,
                warmup_messages: scenario.config.workload.warmup_messages,
                num_producers: scenario.config.workload.num_producers,
                num_consumers: scenario.config.workload.num_consumers,
                coordination: scenario
                    .config
                    .transport
                    .coordination
                    .as_ref()
                    .map(coordination_name),
                discovery_mode: scenario
                    .config
                    .transport
                    .discovery
                    .as_ref()
                    .map(discovery_name),
                zero_copy: scenario
                    .config
                    .transport
                    .zero_copy
                    .as_ref()
                    .map(zero_copy_enabled),
                framing: scenario.config.transport.framing.as_ref().map(framing_name),
            },
            results: BenchResults {
                producer_throughput_ops_sec: outcome.producer.throughput_ops_sec,
                consumer_throughput_ops_sec: outcome.consumers.average_throughput_ops_sec,
                data_rate_mbps: outcome.producer.data_rate_mbps,
                messages_processed: outcome.verification.messages_processed,
                verification_passed: outcome.verification.passed,
                producer_bandwidth_bytes_sec: outcome.producer.bandwidth_bytes_sec,
                producer_data_rate_gbps: outcome.producer.data_rate_gbps,
                consumer_avg_bandwidth_bytes_sec: outcome.consumers.average_bandwidth_bytes_sec,
                consumer_avg_data_rate_gbps: outcome.consumers.average_data_rate_gbps,
                consumer_min_throughput_ops_sec: outcome.consumers.min_throughput_ops_sec,
                consumer_max_throughput_ops_sec: outcome.consumers.max_throughput_ops_sec,
                consumer_total_throughput_ops_sec: outcome.consumers.total_throughput_ops_sec,
                consumer_checksum_total: outcome.consumers.checksum_total,
                phase_timing: outcome.phase_timing.clone(),
                pct_of_raw_ring: outcome.derived.pct_of_raw_ring,
                delta_vs_raw_ring_pct: outcome.derived.delta_vs_raw_ring_pct,
                speedup_vs_bincode: outcome.derived.speedup_vs_bincode,
                delta_vs_bincode_pct: outcome.derived.delta_vs_bincode_pct,
                access_avg_ns: outcome.derived.access_avg_ns,
                access_vs_decode_speedup: outcome.derived.access_vs_decode_speedup,
                alloc_count: outcome.derived.alloc_count,
                alloc_bytes: outcome.derived.alloc_bytes,
                hw_bandwidth_limit_gbps: outcome.derived.hw_bandwidth_limit_gbps,
                hw_efficiency_pct: outcome.derived.hw_efficiency_pct,
                per_consumer: outcome
                    .per_consumer
                    .iter()
                    .map(|entry| BenchConsumerResult {
                        consumer_id: entry.consumer_id,
                        throughput_ops_sec: entry.throughput_ops_sec,
                        events_consumed: entry.events_consumed,
                        bandwidth_bytes_sec: entry.bandwidth_bytes_sec,
                        data_rate_gbps: entry.data_rate_gbps,
                        checksum: entry.checksum,
                        latency: entry.latency.clone(),
                        phase_timing: entry.phase_timing.clone(),
                    })
                    .collect(),
                layout_validation: None,
            },
            latency: outcome.latency.clone(),
            metadata: bench_metadata(&scenario.metadata),
        },
        ScenarioOutcome::Layout(layout) => BenchResult {
            benchmark_id: scenario.identity.benchmark_id.clone(),
            scenario: scenario.identity.scenario.clone(),
            backend: backend_name(&scenario.identity.backend),
            layer: scenario.identity.layer.clone(),
            codec: scenario.identity.codec.as_ref().map(codec_name),
            measurement_mode: measurement_name(&scenario.config.measurement),
            wait_strategy: wait_strategy_name(&scenario.config.transport.wait_strategy),
            config: BenchConfig {
                message_size_bytes: scenario.config.workload.message_size_bytes,
                payload_bytes: scenario.config.workload.payload_bytes,
                buffer_depth: scenario.config.workload.buffer_depth,
                num_messages: scenario.config.workload.num_messages,
                warmup_messages: scenario.config.workload.warmup_messages,
                num_producers: scenario.config.workload.num_producers,
                num_consumers: scenario.config.workload.num_consumers,
                coordination: scenario
                    .config
                    .transport
                    .coordination
                    .as_ref()
                    .map(coordination_name),
                discovery_mode: scenario
                    .config
                    .transport
                    .discovery
                    .as_ref()
                    .map(discovery_name),
                zero_copy: scenario
                    .config
                    .transport
                    .zero_copy
                    .as_ref()
                    .map(zero_copy_enabled),
                framing: scenario.config.transport.framing.as_ref().map(framing_name),
            },
            results: BenchResults {
                producer_throughput_ops_sec: 0.0,
                consumer_throughput_ops_sec: 0.0,
                data_rate_mbps: 0.0,
                messages_processed: 0,
                verification_passed: layout.pass,
                producer_bandwidth_bytes_sec: None,
                producer_data_rate_gbps: None,
                consumer_avg_bandwidth_bytes_sec: None,
                consumer_avg_data_rate_gbps: None,
                consumer_min_throughput_ops_sec: None,
                consumer_max_throughput_ops_sec: None,
                consumer_total_throughput_ops_sec: None,
                consumer_checksum_total: None,
                phase_timing: None,
                pct_of_raw_ring: None,
                delta_vs_raw_ring_pct: None,
                speedup_vs_bincode: None,
                delta_vs_bincode_pct: None,
                access_avg_ns: None,
                access_vs_decode_speedup: None,
                alloc_count: None,
                alloc_bytes: None,
                hw_bandwidth_limit_gbps: None,
                hw_efficiency_pct: None,
                per_consumer: Vec::new(),
                layout_validation: Some(LayoutValidationMetrics {
                    avg_ns: layout.avg_ns,
                    budget_ns: layout.budget_ns,
                    pass: layout.pass,
                    iterations: layout.iterations,
                }),
            },
            latency: None,
            metadata: bench_metadata(&scenario.metadata),
        },
    }
}

fn parse_benchmark_id(benchmark_id: &str) -> (String, String) {
    let mut parts = benchmark_id.split('/');
    let suite = parts.next().unwrap_or("perf-bench").to_string();
    let benchmark = parts.next().unwrap_or(benchmark_id).to_string();
    (suite, benchmark)
}

fn infer_family(benchmark: &str) -> ScenarioFamily {
    match benchmark {
        "raw_ring_shm" | "raw_ring_mmap" => ScenarioFamily::RawRing,
        "wait_strategy_shm" | "wait_strategy_mmap" => ScenarioFamily::WaitStrategy,
        "pingpong_shm"
        | "pingpong_mmap"
        | "pingpong_raw_myelon_shm"
        | "pingpong_raw_myelon_mmap"
        | "pingpong_framed_shm"
        | "pingpong_framed_mmap"
        | "pingpong_codec_shm"
        | "pingpong_codec_mmap"
        | "pingpong_typed_zero_copy_shm"
        | "pingpong_typed_zero_copy_mmap" => ScenarioFamily::PingPong,
        "framed_shm" | "framed_mmap" => ScenarioFamily::Framed,
        "codec_e2e_shm" | "codec_e2e_mmap" => ScenarioFamily::CodecE2E,
        "codec_nofrag_shm" => ScenarioFamily::CodecNoFrag,
        "monster_sweep_shm" | "monster_sweep_mmap" => ScenarioFamily::MonsterSweep,
        "myelon_layers" => ScenarioFamily::MyelonLayerSweep,
        "myelon_framed_sweep" => ScenarioFamily::MyelonFramedSweep,
        "nofrag_all" => ScenarioFamily::NofragSweep,
        "layout_validation" => ScenarioFamily::LayoutValidation,
        other => ScenarioFamily::Unknown(other.to_string()),
    }
}

fn parse_backend(backend: &str) -> BackendKind {
    match backend {
        "shm" => BackendKind::Shm,
        "mmap" => BackendKind::Mmap,
        "layout" => BackendKind::Layout,
        other => BackendKind::Unknown(other.to_string()),
    }
}

fn backend_name(backend: &BackendKind) -> String {
    match backend {
        BackendKind::Shm => "shm".to_string(),
        BackendKind::Mmap => "mmap".to_string(),
        BackendKind::Layout => "layout".to_string(),
        BackendKind::Unknown(other) => other.clone(),
    }
}

fn parse_codec(codec: &str) -> CodecKind {
    match codec {
        "bincode" => CodecKind::Bincode,
        "rkyv" => CodecKind::Rkyv,
        "flatbuf" => CodecKind::Flatbuf,
        other => CodecKind::Unknown(other.to_string()),
    }
}

fn codec_name(codec: &CodecKind) -> String {
    match codec {
        CodecKind::Bincode => "bincode".to_string(),
        CodecKind::Rkyv => "rkyv".to_string(),
        CodecKind::Flatbuf => "flatbuf".to_string(),
        CodecKind::Unknown(other) => other.clone(),
    }
}

fn parse_wait_strategy(wait: &str) -> WaitStrategyKind {
    match wait {
        "BusySpin" => WaitStrategyKind::BusySpin,
        "BusySpinWithSpinLoopHint" => WaitStrategyKind::BusySpinWithSpinLoopHint,
        "Block" => WaitStrategyKind::Block,
        "Sleep" => WaitStrategyKind::Sleep,
        other => WaitStrategyKind::Unknown(other.to_string()),
    }
}

fn wait_strategy_name(wait: &WaitStrategyKind) -> String {
    match wait {
        WaitStrategyKind::BusySpin => "BusySpin".to_string(),
        WaitStrategyKind::BusySpinWithSpinLoopHint => "BusySpinWithSpinLoopHint".to_string(),
        WaitStrategyKind::Block => "Block".to_string(),
        WaitStrategyKind::Sleep => "Sleep".to_string(),
        WaitStrategyKind::Unknown(other) => other.clone(),
    }
}

fn parse_coordination(coordination: &str) -> CoordinationKind {
    match coordination {
        "BenchmarkCoordination" => CoordinationKind::BenchmarkCoordination,
        "UnifiedCoordination" => CoordinationKind::UnifiedCoordination,
        "mmap_builtin" => CoordinationKind::MmapBuiltin,
        other => CoordinationKind::Unknown(other.to_string()),
    }
}

fn coordination_name(coordination: &CoordinationKind) -> String {
    match coordination {
        CoordinationKind::BenchmarkCoordination => "BenchmarkCoordination".to_string(),
        CoordinationKind::UnifiedCoordination => "UnifiedCoordination".to_string(),
        CoordinationKind::MmapBuiltin => "mmap_builtin".to_string(),
        CoordinationKind::Unknown(other) => other.clone(),
    }
}

fn parse_discovery(discovery: &str) -> DiscoveryKind {
    if discovery == "disabled" {
        return DiscoveryKind::Disabled;
    }

    if let Some(consumers) = discovery
        .strip_prefix("enabled(")
        .and_then(|rest| rest.strip_suffix(')'))
        .and_then(|value| value.parse::<usize>().ok())
    {
        return DiscoveryKind::Enabled { consumers };
    }

    DiscoveryKind::Unknown(discovery.to_string())
}

fn discovery_name(discovery: &DiscoveryKind) -> String {
    match discovery {
        DiscoveryKind::Disabled => "disabled".to_string(),
        DiscoveryKind::Enabled { consumers } => format!("enabled({consumers})"),
        DiscoveryKind::Unknown(other) => other.clone(),
    }
}

fn parse_framing(framing: &str) -> FramingKind {
    match framing {
        "none" => FramingKind::None,
        "fixed_64k" => FramingKind::Fixed64K,
        "fixed_64k_batch" => FramingKind::Fixed64KBatch,
        "right_sized" => FramingKind::RightSized,
        other => FramingKind::Unknown(other.to_string()),
    }
}

fn framing_name(framing: &FramingKind) -> String {
    match framing {
        FramingKind::None => "none".to_string(),
        FramingKind::Fixed64K => "fixed_64k".to_string(),
        FramingKind::Fixed64KBatch => "fixed_64k_batch".to_string(),
        FramingKind::RightSized => "right_sized".to_string(),
        FramingKind::Unknown(other) => other.clone(),
    }
}

fn parse_zero_copy(enabled: bool) -> ZeroCopyKind {
    if enabled {
        ZeroCopyKind::Enabled
    } else {
        ZeroCopyKind::Disabled
    }
}

fn zero_copy_enabled(zero_copy: &ZeroCopyKind) -> bool {
    matches!(zero_copy, ZeroCopyKind::Enabled)
}

fn parse_measurement(mode: &str) -> MeasurementKind {
    match mode {
        "max_throughput" => MeasurementKind::MaxThroughput,
        "batch_timing" => MeasurementKind::BatchTiming,
        "layout_validation" => MeasurementKind::LayoutValidation,
        other => other
            .strip_prefix("co_aware@")
            .and_then(|rate| rate.parse::<u64>().ok())
            .map(|target_rate| MeasurementKind::CoAware { target_rate })
            .unwrap_or_else(|| MeasurementKind::Unknown(other.to_string())),
    }
}

fn measurement_name(measurement: &MeasurementKind) -> String {
    match measurement {
        MeasurementKind::MaxThroughput => "max_throughput".to_string(),
        MeasurementKind::BatchTiming => "batch_timing".to_string(),
        MeasurementKind::LayoutValidation => "layout_validation".to_string(),
        MeasurementKind::CoAware { target_rate } => format!("co_aware@{target_rate}"),
        MeasurementKind::Unknown(other) => other.clone(),
    }
}

fn batch_size_from_result(result: &BenchResult) -> Option<usize> {
    let benchmark = result.benchmark_id.split('/').nth(1).unwrap_or("");
    if benchmark == "codec_nofrag_shm" {
        return result
            .scenario
            .split('_')
            .find_map(|part| part.strip_suffix('b'))
            .and_then(|value| value.parse::<usize>().ok());
    }

    if benchmark == "codec_e2e_shm" || benchmark == "codec_e2e_mmap" {
        return result
            .scenario
            .split('_')
            .find_map(|part| part.strip_prefix('b'))
            .and_then(|value| value.parse::<usize>().ok());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::layout;
    use crate::infra::latency::LatencyRecorder;
    use crate::infra::output::report::{LayoutTargetMeasurement, ReportBundle};
    use crate::infra::output::reporting;
    use crate::infra::output::results::{ConsumerOutput, PhaseTiming, ProducerOutput};
    use std::time::Duration;

    #[expect(
        clippy::too_many_arguments,
        reason = "test fixture builder keeps benchmark dimensions explicit at callsites"
    )]
    fn make_test_result(
        bench_name: &str,
        scenario: &str,
        backend: &str,
        layer: &str,
        codec: Option<&str>,
        measurement_mode: &str,
        transport: reporting::BenchTransportSpec,
        msg_bytes: usize,
        payload_bytes: usize,
        buffer_depth: usize,
        num_messages: u64,
        warmup_messages: u64,
        num_consumers: usize,
        prod_ops: f64,
        cons_ops: f64,
        latency: Option<crate::infra::latency::LatencyStats>,
    ) -> reporting::BenchResult {
        reporting::make_result(reporting::BenchResultSpec {
            bench_name: bench_name.to_string(),
            scenario: scenario.to_string(),
            backend: backend.to_string(),
            layer: layer.to_string(),
            codec: codec.map(|value| value.to_string()),
            measurement_mode: measurement_mode.to_string(),
            wait_strategy: "BusySpin".to_string(),
            transport,
            message_size_bytes: msg_bytes,
            payload_bytes,
            buffer_depth,
            num_messages,
            warmup_messages,
            num_producers: 1,
            num_consumers,
            producer_throughput_ops_sec: prod_ops,
            consumer_throughput_ops_sec: cons_ops,
            latency,
        })
    }

    #[test]
    fn adapts_throughput_result_with_structured_metrics() {
        let mut recorder = LatencyRecorder::default_range();
        recorder.record(200);
        recorder.record(400);
        let latency = recorder.stats().expect("latency stats");

        let mut result = make_test_result(
            "codec_e2e_shm",
            "rkyv_b8_1p2c",
            "shm",
            "raw_ring+codec",
            Some("rkyv"),
            "co_aware@20000",
            reporting::BenchTransportSpec::benchmark_shm(2)
                .with_zero_copy(true)
                .with_framing("none"),
            2048,
            2048,
            4096,
            50_000,
            5_000,
            2,
            123_456.0,
            120_000.0,
            Some(latency.clone()),
        );

        let producer = ProducerOutput {
            throughput_ops_sec: 123_456.0,
            elapsed_secs: 1.0,
            events_produced: 50_000,
            bandwidth_bytes_sec: 252_837_888.0,
            data_rate_gbps: 0.252837888,
            phase_timing: Some(PhaseTiming {
                encode_avg_ns: Some(100.0),
                transport_write_avg_ns: Some(200.0),
                transport_read_avg_ns: Some(300.0),
                decode_avg_ns: Some(400.0),
            }),
        };
        let consumer0 =
            ConsumerOutput::from_elapsed(0, 50_000, Duration::from_millis(400), 2048, 11)
                .with_latency(latency.clone());
        let consumer1 =
            ConsumerOutput::from_elapsed(1, 50_000, Duration::from_millis(420), 2048, 17);
        reporting::attach_child_metrics(&mut result, &producer, &[consumer0, consumer1]);
        result.results.access_avg_ns = Some(42.5);
        result.results.access_vs_decode_speedup = Some(4.25);
        result.results.alloc_count = Some(0);
        result.results.alloc_bytes = Some(0);

        let mut report = reporting::BenchReport::new();
        report.add(result);
        let bundle = report.to_report();

        assert_eq!(bundle.scenarios.len(), 1);
        let scenario = &bundle.scenarios[0];
        assert_eq!(scenario.identity.benchmark, "codec_e2e_shm");
        assert_eq!(scenario.identity.family, ScenarioFamily::CodecE2E);
        assert_eq!(scenario.identity.backend, BackendKind::Shm);
        assert_eq!(scenario.identity.codec, Some(CodecKind::Rkyv));
        assert_eq!(
            scenario.config.measurement,
            MeasurementKind::CoAware {
                target_rate: 20_000
            }
        );
        assert_eq!(scenario.config.workload.batch_size, Some(8));

        let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
            panic!("expected throughput outcome");
        };
        assert_eq!(outcome.per_consumer.len(), 2);
        assert_eq!(outcome.verification.messages_processed, 50_000);
        assert_eq!(outcome.producer.throughput_ops_sec, 123_456.0);
        assert_eq!(outcome.consumers.checksum_total, Some(28));
        assert!(outcome.phase_timing.is_some());
        assert!(outcome.latency.is_some());
        assert_eq!(outcome.derived.access_avg_ns, Some(42.5));
        assert_eq!(outcome.derived.access_vs_decode_speedup, Some(4.25));
        assert_eq!(outcome.derived.alloc_count, Some(0));
        assert_eq!(outcome.derived.alloc_bytes, Some(0));
    }

    #[test]
    fn adapts_layout_result_as_first_class_layout_outcome() {
        let report = ReportBundle::from_layout_targets(
            "layout_validation",
            50,
            &[LayoutTargetMeasurement::new(
                &layout::TYPED_SHM_CONSUMER_ATTACH,
                12_345,
            )],
        );

        let bundle = report;
        assert_eq!(bundle.scenarios.len(), 1);
        let scenario = &bundle.scenarios[0];
        assert_eq!(scenario.identity.family, ScenarioFamily::LayoutValidation);
        assert_eq!(
            scenario.config.measurement,
            MeasurementKind::LayoutValidation
        );

        let ScenarioOutcome::Layout(outcome) = &scenario.outcome else {
            panic!("expected layout outcome");
        };
        assert_eq!(outcome.avg_ns, 12_345);
        assert_eq!(outcome.iterations, 50);
        assert!(outcome.pass);
    }

    #[test]
    fn round_trips_report_bundle_back_into_v1_compat_report() {
        let mut result = make_test_result(
            "raw_ring_shm",
            "signal_1p2c_64B",
            "shm",
            "raw_ring",
            None,
            "max_throughput",
            reporting::BenchTransportSpec::benchmark_shm(2)
                .with_zero_copy(false)
                .with_framing("none"),
            64,
            64,
            65_536,
            1_000_000,
            100_000,
            2,
            10_000_000.0,
            9_500_000.0,
            None,
        );
        result.results.consumer_checksum_total = Some(18);
        result.results.access_avg_ns = Some(18.0);
        result.results.access_vs_decode_speedup = Some(3.5);
        result.results.alloc_count = Some(0);
        result.results.alloc_bytes = Some(0);

        let mut report = reporting::BenchReport::new();
        report.add(result);

        let compat = report.to_report().to_report_v1_compat();
        assert_eq!(compat.results.len(), 1);
        assert_eq!(
            compat.results[0].benchmark_id,
            "myelon-bench/raw_ring_shm/signal_1p2c_64B"
        );
        assert_eq!(compat.results[0].backend, "shm");
        assert_eq!(compat.results[0].results.consumer_checksum_total, Some(18));
        assert_eq!(compat.results[0].results.access_avg_ns, Some(18.0));
        assert_eq!(
            compat.results[0].results.access_vs_decode_speedup,
            Some(3.5)
        );
        assert_eq!(compat.results[0].results.alloc_count, Some(0));
        assert_eq!(compat.results[0].results.alloc_bytes, Some(0));
    }

    #[test]
    fn pingpong_typed_zero_copy_benches_map_to_pingpong_family() {
        assert_eq!(
            infer_family("pingpong_typed_zero_copy_shm"),
            ScenarioFamily::PingPong
        );
        assert_eq!(
            infer_family("pingpong_typed_zero_copy_mmap"),
            ScenarioFamily::PingPong
        );
    }
}
