use crate::report_v2::BackendKind;
use crate::reporting::ReportOutputArgs;

#[derive(Debug, Clone)]
pub struct WaitStrategyScenarioSpec {
    pub backend: BackendKind,
    pub consumers: usize,
    pub wait_strategy: &'static str,
    pub producer_role: &'static str,
    pub consumer_role: &'static str,
    pub throughput_unit: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitStrategyMode {
    Quick,
    Full,
}

#[derive(Debug, Clone)]
pub struct WaitStrategySelection {
    mode: WaitStrategyMode,
    pub output_args: ReportOutputArgs,
}

impl WaitStrategySelection {
    pub fn parse(args: &[String], default_mode: &str) -> Result<Self, String> {
        let mode = match default_mode {
            "quick" => WaitStrategyMode::Quick,
            "full" | "comprehensive" => WaitStrategyMode::Full,
            other => return Err(format!("Unknown mode '{other}', expected quick|full")),
        };
        Ok(Self {
            mode,
            output_args: ReportOutputArgs::from_args(args),
        })
    }

    pub fn mode_name(&self) -> &'static str {
        match self.mode {
            WaitStrategyMode::Quick => "quick",
            WaitStrategyMode::Full => "full",
        }
    }

    pub fn scenario_specs(&self, backend: BackendKind) -> Vec<WaitStrategyScenarioSpec> {
        match self.mode {
            WaitStrategyMode::Quick => vec![scenario_spec(backend, 1, "BusySpin")],
            WaitStrategyMode::Full => [1usize, 2, 4, 6, 8, 10, 12]
                .into_iter()
                .flat_map(|consumers| {
                    let backend = backend.clone();
                    ["BusySpin", "Block", "Sleep", "BusySpinWithSpinLoopHint"]
                        .into_iter()
                        .map(move |wait_strategy| {
                            scenario_spec(backend.clone(), consumers, wait_strategy)
                        })
                })
                .collect(),
        }
    }
}

fn scenario_spec(
    backend: BackendKind,
    consumers: usize,
    wait_strategy: &'static str,
) -> WaitStrategyScenarioSpec {
    let (producer_role, consumer_role, throughput_unit) = match backend {
        BackendKind::Shm => ("shm_wait_producer", "shm_wait_consumer", "ops/s"),
        BackendKind::Mmap => ("mmap_wait_producer", "mmap_wait_consumer", "events/s"),
        _ => ("wait_producer", "wait_consumer", "ops/s"),
    };

    WaitStrategyScenarioSpec {
        backend,
        consumers,
        wait_strategy,
        producer_role,
        consumer_role,
        throughput_unit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quick_mode_only_keeps_busyspin_single_consumer() {
        let selection = WaitStrategySelection::parse(&["bench".to_string()], "quick")
            .expect("parse wait strategy selection");
        let scenarios = selection.scenario_specs(BackendKind::Shm);
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].consumers, 1);
        assert_eq!(scenarios[0].wait_strategy, "BusySpin");
        assert_eq!(scenarios[0].producer_role, "shm_wait_producer");
    }

    #[test]
    fn full_mode_uses_backend_specific_roles() {
        let selection = WaitStrategySelection::parse(&["bench".to_string()], "full")
            .expect("parse wait strategy selection");
        let scenarios = selection.scenario_specs(BackendKind::Mmap);
        let scenario = scenarios
            .into_iter()
            .find(|entry| entry.consumers == 12 && entry.wait_strategy == "Sleep")
            .expect("mmap 12c sleep");
        assert_eq!(scenario.consumer_role, "mmap_wait_consumer");
        assert_eq!(scenario.throughput_unit, "events/s");
    }
}
