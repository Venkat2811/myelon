use super::model::{
    LayoutOutcome, MeasurementKind, ReportBundle, RunMetadata, ScenarioConfig, ScenarioFamily,
    ScenarioIdentity, ScenarioOutcome, ScenarioReport, TransportSpec, WaitStrategyKind,
    WorkloadConfig,
};
use crate::cli::layout::LayoutTargetSpec;

#[derive(Debug, Clone, Copy)]
pub struct LayoutTargetMeasurement {
    pub spec: &'static LayoutTargetSpec,
    pub avg_ns: u64,
    pub pass: bool,
}

impl LayoutTargetMeasurement {
    pub fn new(spec: &'static LayoutTargetSpec, avg_ns: u64) -> Self {
        Self {
            spec,
            avg_ns,
            pass: avg_ns <= spec.budget_ns,
        }
    }
}

impl ReportBundle {
    pub fn from_layout_targets(
        benchmark: &'static str,
        iterations: usize,
        measurements: &[LayoutTargetMeasurement],
    ) -> Self {
        let metadata = RunMetadata::capture();
        Self {
            metadata: metadata.clone(),
            scenarios: measurements
                .iter()
                .map(|measurement| layout_scenario(benchmark, iterations, measurement, &metadata))
                .collect(),
        }
    }
}

fn layout_scenario(
    benchmark: &'static str,
    iterations: usize,
    measurement: &LayoutTargetMeasurement,
    metadata: &RunMetadata,
) -> ScenarioReport {
    ScenarioReport {
        identity: ScenarioIdentity {
            benchmark_id: format!("perf-bench/{benchmark}/{}", measurement.spec.scenario),
            suite: "perf-bench".to_string(),
            benchmark: benchmark.to_string(),
            family: ScenarioFamily::LayoutValidation,
            scenario: measurement.spec.scenario.to_string(),
            backend: measurement.spec.backend.clone(),
            layer: measurement.spec.layer.to_string(),
            codec: None,
        },
        config: ScenarioConfig {
            measurement: MeasurementKind::LayoutValidation,
            transport: TransportSpec {
                wait_strategy: WaitStrategyKind::Unknown("n/a".to_string()),
                coordination: None,
                discovery: None,
                framing: None,
                zero_copy: None,
            },
            workload: WorkloadConfig {
                message_size_bytes: 0,
                payload_bytes: 0,
                buffer_depth: 0,
                num_messages: iterations as u64,
                warmup_messages: 0,
                num_producers: 1,
                num_consumers: 1,
                batch_size: None,
            },
        },
        outcome: ScenarioOutcome::Layout(LayoutOutcome {
            avg_ns: measurement.avg_ns,
            budget_ns: measurement.spec.budget_ns,
            iterations,
            pass: measurement.pass,
        }),
        metadata: metadata.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::layout;
    use crate::infra::output::report::BackendKind;

    #[test]
    fn builds_first_class_layout_bundle() {
        let report = ReportBundle::from_layout_targets(
            "layout_validation",
            50,
            &[LayoutTargetMeasurement::new(
                &layout::TYPED_SHM_CONSUMER_ATTACH,
                123,
            )],
        );
        assert_eq!(report.scenarios.len(), 1);
        assert!(matches!(
            report.scenarios[0].outcome,
            ScenarioOutcome::Layout(_)
        ));
        assert_eq!(report.scenarios[0].identity.layer, "typed");
        assert!(matches!(
            report.scenarios[0].identity.backend,
            BackendKind::Shm
        ));
    }
}
