use crate::report_v2::BackendKind;
use crate::reporting::ReportOutputArgs;

const BUFFER_DEPTH: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecBenchMode {
    Throughput,
    PhaseTiming,
    Co,
}

#[derive(Debug, Clone)]
pub struct CodecScenarioSpec {
    pub backend: BackendKind,
    pub codec: &'static str,
    pub batch_size: usize,
    pub messages: u64,
    pub consumers: usize,
    pub buffer: usize,
    pub producer_role: &'static str,
    pub consumer_role: &'static str,
}

#[derive(Debug, Clone)]
pub struct CodecSelection {
    mode: CodecBenchMode,
    codec_filter: Option<String>,
    batch_filter: Option<usize>,
    consumers_filter: Option<usize>,
    target_rate: u64,
    pub output_args: ReportOutputArgs,
}

impl CodecSelection {
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let mode = match find_arg_value(args, "--mode")
            .as_deref()
            .unwrap_or("throughput")
        {
            "throughput" => CodecBenchMode::Throughput,
            "phase_timing" => CodecBenchMode::PhaseTiming,
            "co" => CodecBenchMode::Co,
            other => return Err(format!("unsupported mode: {other}")),
        };
        let codec_filter = find_arg_value(args, "--codec").filter(|value| value != "all");
        let batch_filter = find_arg_value(args, "--batch")
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --batch value {value}: {error}"))
            })
            .transpose()?;
        let consumers_filter = find_arg_value(args, "--consumers")
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --consumers value {value}: {error}"))
            })
            .transpose()?;
        let target_rate = find_arg_value(args, "--target-rate")
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --target-rate value {value}: {error}"))
            })
            .transpose()?;

        if mode == CodecBenchMode::Co && target_rate.is_none() {
            return Err("--mode co requires --target-rate".into());
        }
        if mode != CodecBenchMode::Co && target_rate.is_some() {
            return Err("--target-rate requires --mode co".into());
        }

        Ok(Self {
            mode,
            codec_filter,
            batch_filter,
            consumers_filter,
            target_rate: target_rate.unwrap_or(0),
            output_args: ReportOutputArgs::from_args(args),
        })
    }

    pub fn phase_timing(&self) -> bool {
        self.mode == CodecBenchMode::PhaseTiming
    }

    pub fn target_rate(&self) -> u64 {
        self.target_rate
    }

    pub fn mode_description(&self) -> &'static str {
        match self.mode {
            CodecBenchMode::Throughput => "throughput",
            CodecBenchMode::PhaseTiming => "phase_timing (encode/transport/decode)",
            CodecBenchMode::Co => "co_aware",
        }
    }

    pub fn scenario_specs(&self, backend: BackendKind) -> Vec<CodecScenarioSpec> {
        base_specs(backend)
            .into_iter()
            .filter(|spec| self.should_run(spec))
            .collect()
    }

    pub fn should_run(&self, spec: &CodecScenarioSpec) -> bool {
        if self
            .batch_filter
            .is_some_and(|batch_size| batch_size != spec.batch_size)
        {
            return false;
        }
        if self
            .codec_filter
            .as_deref()
            .is_some_and(|codec| codec != spec.codec)
        {
            return false;
        }
        if self
            .consumers_filter
            .is_some_and(|consumers| consumers != spec.consumers)
        {
            return false;
        }
        true
    }
}

fn base_specs(backend: BackendKind) -> Vec<CodecScenarioSpec> {
    let (producer_role, consumer_role) = match backend {
        BackendKind::Shm => ("codec_producer", "codec_consumer"),
        BackendKind::Mmap => ("codec_producer", "codec_consumer"),
        _ => ("codec_producer", "codec_consumer"),
    };

    let mut specs = Vec::new();
    for (batch_size, messages) in [(8usize, 50_000u64), (64, 20_000), (256, 10_000)] {
        for codec in ["bincode", "rkyv", "flatbuf"] {
            for consumers in [1usize, 2, 4, 6, 8, 12] {
                specs.push(CodecScenarioSpec {
                    backend: backend.clone(),
                    codec,
                    batch_size,
                    messages,
                    consumers,
                    buffer: scaled_buffer(consumers),
                    producer_role,
                    consumer_role,
                });
            }
        }
    }
    specs
}

fn scaled_buffer(consumers: usize) -> usize {
    BUFFER_DEPTH
        .max(consumers.next_power_of_two() * 256)
        .next_power_of_two()
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
    fn selection_parses_phase_and_filters() {
        let args = vec![
            "bench".to_string(),
            "--mode".to_string(),
            "phase_timing".to_string(),
            "--codec".to_string(),
            "rkyv".to_string(),
            "--batch".to_string(),
            "64".to_string(),
            "--consumers".to_string(),
            "4".to_string(),
        ];
        let selection = CodecSelection::parse(&args).expect("parse codec selection");
        assert!(selection.phase_timing());
        assert_eq!(selection.target_rate(), 0);

        let scenarios = selection.scenario_specs(BackendKind::Shm);
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].codec, "rkyv");
        assert_eq!(scenarios[0].batch_size, 64);
        assert_eq!(scenarios[0].consumers, 4);
        assert_eq!(scenarios[0].producer_role, "codec_producer");
    }

    #[test]
    fn selection_rejects_invalid_co_flags() {
        let args = vec!["bench".to_string(), "--mode".to_string(), "co".to_string()];
        assert_eq!(
            CodecSelection::parse(&args).unwrap_err(),
            "--mode co requires --target-rate"
        );

        let args = vec![
            "bench".to_string(),
            "--target-rate".to_string(),
            "20000".to_string(),
        ];
        assert_eq!(
            CodecSelection::parse(&args).unwrap_err(),
            "--target-rate requires --mode co"
        );
    }

    #[test]
    fn specs_scale_buffer_from_consumers() {
        let scenarios = CodecSelection::parse(&["bench".to_string()])
            .expect("parse codec selection")
            .scenario_specs(BackendKind::Mmap);
        let spec = scenarios
            .into_iter()
            .find(|entry| {
                entry.codec == "flatbuf" && entry.batch_size == 256 && entry.consumers == 12
            })
            .expect("flatbuf 256 batch 12c");
        assert_eq!(spec.buffer, 4096);
        assert_eq!(spec.consumer_role, "codec_consumer");
    }
}
