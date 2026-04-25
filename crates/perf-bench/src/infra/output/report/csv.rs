use super::model::{CodecKind, MeasurementKind, ReportBundle, ScenarioOutcome};

impl ReportBundle {
    pub fn write_csv(&self, path: &str) -> std::io::Result<()> {
        let mut csv = String::from(
            "scenario,backend,layer,codec,mode,strategy,msg_bytes,payload_bytes,buffer,consumers,\
             coordination,discovery,zero_copy,framing,prod_ops,cons_ops,mbps,cons_min_ops,\
             cons_max_ops,cons_total_ops,checksum_total,pct_raw,delta_raw_pct,speedup_vs_bincode,\
             delta_bincode_pct,access_avg_ns,access_vs_decode_speedup,alloc_count,alloc_bytes,\
             hw_limit_gbps,hw_efficiency_pct,p1_ns,p10_ns,p25_ns,p50_ns,p75_ns,p90_ns,p95_ns,p99_ns,p999_ns,p9999_ns,p99999_ns,p999999_ns,verified,\
             layout_avg_ns,layout_budget_ns,layout_pass,layout_iterations\n",
        );

        for scenario in &self.scenarios {
            let codec = scenario
                .identity
                .codec
                .as_ref()
                .map(codec_label)
                .unwrap_or_default();
            let measurement = measurement_label(&scenario.config.measurement);
            let wait_strategy = wait_strategy_label(scenario);
            let coordination = coordination_label(scenario);
            let discovery = discovery_label(scenario);
            let zero_copy = zero_copy_label(scenario);
            let framing = framing_label(scenario);

            match &scenario.outcome {
                ScenarioOutcome::Throughput(outcome) => {
                    let lat_field = |f: fn(&crate::infra::latency::LatencyStats) -> u64| -> String {
                        outcome
                            .latency
                            .as_ref()
                            .map(|stats| f(stats).to_string())
                            .unwrap_or_default()
                    };
                    let opt_f64 =
                        |value: Option<f64>| value.map(|v| format!("{v:.0}")).unwrap_or_default();
                    let opt_u64 =
                        |value: Option<u64>| value.map(|v| v.to_string()).unwrap_or_default();
                    let row = [
                        scenario.identity.scenario.clone(),
                        backend_label(scenario),
                        scenario.identity.layer.clone(),
                        codec,
                        measurement,
                        wait_strategy,
                        scenario.config.workload.message_size_bytes.to_string(),
                        scenario.config.workload.payload_bytes.to_string(),
                        scenario.config.workload.buffer_depth.to_string(),
                        scenario.config.workload.num_consumers.to_string(),
                        coordination,
                        discovery,
                        zero_copy,
                        framing,
                        format!("{:.0}", outcome.producer.throughput_ops_sec),
                        format!("{:.0}", outcome.consumers.average_throughput_ops_sec),
                        format!("{:.3}", outcome.producer.data_rate_mbps),
                        opt_f64(outcome.consumers.min_throughput_ops_sec),
                        opt_f64(outcome.consumers.max_throughput_ops_sec),
                        opt_f64(outcome.consumers.total_throughput_ops_sec),
                        opt_u64(outcome.consumers.checksum_total),
                        outcome
                            .derived
                            .pct_of_raw_ring
                            .map(|value| format!("{value:.2}"))
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .delta_vs_raw_ring_pct
                            .map(|value| format!("{value:.2}"))
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .speedup_vs_bincode
                            .map(|value| format!("{value:.4}"))
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .delta_vs_bincode_pct
                            .map(|value| format!("{value:.2}"))
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .access_avg_ns
                            .map(|value| format!("{value:.3}"))
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .access_vs_decode_speedup
                            .map(|value| format!("{value:.4}"))
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .alloc_count
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .alloc_bytes
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .hw_bandwidth_limit_gbps
                            .map(|value| format!("{value:.3}"))
                            .unwrap_or_default(),
                        outcome
                            .derived
                            .hw_efficiency_pct
                            .map(|value| format!("{value:.2}"))
                            .unwrap_or_default(),
                        lat_field(|stats| stats.p1_ns),
                        lat_field(|stats| stats.p10_ns),
                        lat_field(|stats| stats.p25_ns),
                        lat_field(|stats| stats.p50_ns),
                        lat_field(|stats| stats.p75_ns),
                        lat_field(|stats| stats.p90_ns),
                        lat_field(|stats| stats.p95_ns),
                        lat_field(|stats| stats.p99_ns),
                        lat_field(|stats| stats.p999_ns),
                        lat_field(|stats| stats.p9999_ns),
                        lat_field(|stats| stats.p99999_ns),
                        lat_field(|stats| stats.p999999_ns),
                        outcome.verification.passed.to_string(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                    ];
                    csv.push_str(&row.join(","));
                    csv.push('\n');
                }
                ScenarioOutcome::Layout(layout) => {
                    let row = [
                        scenario.identity.scenario.clone(),
                        backend_label(scenario),
                        scenario.identity.layer.clone(),
                        codec,
                        measurement,
                        wait_strategy,
                        scenario.config.workload.message_size_bytes.to_string(),
                        scenario.config.workload.payload_bytes.to_string(),
                        scenario.config.workload.buffer_depth.to_string(),
                        scenario.config.workload.num_consumers.to_string(),
                        coordination,
                        discovery,
                        zero_copy,
                        framing,
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                        layout.pass.to_string(),
                        layout.avg_ns.to_string(),
                        layout.budget_ns.to_string(),
                        layout.pass.to_string(),
                        layout.iterations.to_string(),
                    ];
                    csv.push_str(&row.join(","));
                    csv.push('\n');
                }
            }
        }

        std::fs::write(path, csv)
    }
}

fn backend_label(report: &super::model::ScenarioReport) -> String {
    match &report.identity.backend {
        super::model::BackendKind::Shm => "shm".to_string(),
        super::model::BackendKind::Mmap => "mmap".to_string(),
        super::model::BackendKind::Layout => "layout".to_string(),
        super::model::BackendKind::Unknown(other) => other.clone(),
    }
}

fn codec_label(codec: &CodecKind) -> String {
    match codec {
        CodecKind::Bincode => "bincode".to_string(),
        CodecKind::Rkyv => "rkyv".to_string(),
        CodecKind::Flatbuf => "flatbuf".to_string(),
        CodecKind::Unknown(other) => other.clone(),
    }
}

fn measurement_label(measurement: &MeasurementKind) -> String {
    match measurement {
        MeasurementKind::MaxThroughput => "max_throughput".to_string(),
        MeasurementKind::BatchTiming => "batch_timing".to_string(),
        MeasurementKind::LayoutValidation => "layout_validation".to_string(),
        MeasurementKind::CoAware { target_rate } => format!("co_aware@{target_rate}"),
        MeasurementKind::Unknown(other) => other.clone(),
    }
}

fn wait_strategy_label(scenario: &super::model::ScenarioReport) -> String {
    match &scenario.config.transport.wait_strategy {
        super::model::WaitStrategyKind::BusySpin => "BusySpin".to_string(),
        super::model::WaitStrategyKind::BusySpinWithSpinLoopHint => {
            "BusySpinWithSpinLoopHint".to_string()
        }
        super::model::WaitStrategyKind::Block => "Block".to_string(),
        super::model::WaitStrategyKind::Sleep => "Sleep".to_string(),
        super::model::WaitStrategyKind::Unknown(other) => other.clone(),
    }
}

fn coordination_label(scenario: &super::model::ScenarioReport) -> String {
    match scenario.config.transport.coordination.as_ref() {
        Some(super::model::CoordinationKind::BenchmarkCoordination) => {
            "BenchmarkCoordination".to_string()
        }
        Some(super::model::CoordinationKind::UnifiedCoordination) => {
            "UnifiedCoordination".to_string()
        }
        Some(super::model::CoordinationKind::MmapBuiltin) => "mmap_builtin".to_string(),
        Some(super::model::CoordinationKind::Unknown(other)) => other.clone(),
        None => String::new(),
    }
}

fn discovery_label(scenario: &super::model::ScenarioReport) -> String {
    match scenario.config.transport.discovery.as_ref() {
        Some(super::model::DiscoveryKind::Disabled) => "disabled".to_string(),
        Some(super::model::DiscoveryKind::Enabled { consumers }) => format!("enabled({consumers})"),
        Some(super::model::DiscoveryKind::Unknown(other)) => other.clone(),
        None => String::new(),
    }
}

fn zero_copy_label(scenario: &super::model::ScenarioReport) -> String {
    match scenario.config.transport.zero_copy.as_ref() {
        Some(super::model::ZeroCopyKind::Enabled) => "true".to_string(),
        Some(super::model::ZeroCopyKind::Disabled) => "false".to_string(),
        None => String::new(),
    }
}

fn framing_label(scenario: &super::model::ScenarioReport) -> String {
    match scenario.config.transport.framing.as_ref() {
        Some(super::model::FramingKind::None) => "none".to_string(),
        Some(super::model::FramingKind::Fixed64K) => "fixed_64k".to_string(),
        Some(super::model::FramingKind::Fixed64KBatch) => "fixed_64k_batch".to_string(),
        Some(super::model::FramingKind::RightSized) => "right_sized".to_string(),
        Some(super::model::FramingKind::Unknown(other)) => other.clone(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::layout;
    use crate::infra::output::report::{LayoutTargetMeasurement, ReportBundle};

    #[test]
    fn writes_layout_csv() {
        let report = ReportBundle::from_layout_targets(
            "layout_validation",
            50,
            &[LayoutTargetMeasurement::new(
                &layout::TYPED_SHM_CONSUMER_ATTACH,
                123,
            )],
        );
        let path = std::env::temp_dir().join("perf_bench_report_layout.csv");
        report.write_csv(path.to_str().unwrap()).unwrap();
        let csv = std::fs::read_to_string(&path).unwrap();
        assert!(csv.contains("layout_avg_ns"));
        assert!(csv.contains("typed-shm-consumer-attach"));
        let _ = std::fs::remove_file(path);
    }
}
