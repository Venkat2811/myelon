//! Trait-based top-level bench harness.
//!
//! Bench binaries still own their scenario and reporting logic, but the shared
//! harness now owns child-role dispatch and the top-level execution contract.

use super::output::{ConsumerOutput, ProducerOutput};
use super::runner::{
    collect_child_output, dispatch_child_or_exit, parse_child_metrics, BenchError, BenchRunResult,
    ChildRole,
};
use crate::events::format_throughput;
use crate::latency::LatencyStats;
use crate::reporting::{self, BenchResult};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::Duration;

/// Spawned child processes for one benchmark scenario.
pub struct ScenarioChildren {
    pub producer: Child,
    pub consumers: Vec<Child>,
    pub cleanup_paths: Vec<PathBuf>,
}

impl ScenarioChildren {
    pub fn new(producer: Child, consumers: Vec<Child>) -> Self {
        Self {
            producer,
            consumers,
            cleanup_paths: Vec::new(),
        }
    }

    pub fn with_cleanup_path(mut self, path: PathBuf) -> Self {
        self.cleanup_paths.push(path);
        self
    }
}

/// Shared scenario-level contract for simple multiprocess benchmarks.
pub trait IpcBenchmark {
    fn bench_name(&self) -> &str;
    fn scenario_name(&self) -> String;
    fn backend(&self) -> &str;
    fn layer(&self) -> &str;
    fn transport_metadata(&self) -> reporting::BenchTransportSpec;
    fn codec(&self) -> Option<&str> {
        None
    }
    fn measurement_mode(&self) -> String {
        "max_throughput".to_string()
    }
    fn wait_strategy(&self) -> &str {
        "BusySpin"
    }
    fn message_size_bytes(&self) -> usize;
    fn payload_bytes(&self) -> usize {
        self.message_size_bytes()
    }
    fn buffer_depth(&self) -> usize;
    fn num_messages(&self) -> u64;
    fn warmup_messages(&self) -> u64 {
        0
    }
    fn num_consumers(&self) -> usize;
    fn timeout(&self) -> Duration {
        Duration::from_secs(180)
    }
    fn throughput_unit(&self) -> &str {
        "ops/s"
    }
    fn consumer_summary_label(&self) -> &str {
        if self.num_consumers() == 1 {
            "consumer"
        } else {
            "avg consumer"
        }
    }
    fn producer_label(&self) -> String {
        format!("{} prod", self.scenario_name())
    }
    fn consumer_label(&self, consumer_id: usize) -> String {
        format!("{} cons{consumer_id}", self.scenario_name())
    }
    fn launch(&self, exe: &Path) -> Result<ScenarioChildren, BenchError>;

    fn aggregate_latency(&self, consumers: &[ConsumerOutput]) -> Option<LatencyStats> {
        consumers
            .iter()
            .filter_map(|entry| entry.latency.clone())
            .next_back()
    }

    fn average_consumer_ops(&self, consumers: &[ConsumerOutput]) -> f64 {
        if self.num_consumers() > 0 {
            consumers
                .iter()
                .map(|metrics| metrics.throughput_ops_sec)
                .sum::<f64>()
                / self.num_consumers() as f64
        } else {
            0.0
        }
    }

    fn print_summary_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        latency: Option<&LatencyStats>,
    ) {
        let avg_consumer_ops = self.average_consumer_ops(consumers);
        let latency_suffix = latency
            .map(|stats| format!("  {}", stats.summary()))
            .unwrap_or_default();
        println!(
            "  {:<30} producer: {:>10} {}  {}: {:>10} {}{}",
            self.scenario_name(),
            format_throughput(producer.throughput_ops_sec),
            self.throughput_unit(),
            self.consumer_summary_label(),
            format_throughput(avg_consumer_ops),
            self.throughput_unit(),
            latency_suffix,
        );
    }

    fn build_result_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        latency: Option<LatencyStats>,
    ) -> BenchResult {
        let avg_consumer_ops = self.average_consumer_ops(consumers);
        let mut result = reporting::make_result(reporting::BenchResultSpec {
            bench_name: self.bench_name().to_string(),
            scenario: self.scenario_name(),
            backend: self.backend().to_string(),
            layer: self.layer().to_string(),
            codec: self.codec().map(|value| value.to_string()),
            measurement_mode: self.measurement_mode(),
            wait_strategy: self.wait_strategy().to_string(),
            transport: self.transport_metadata(),
            message_size_bytes: self.message_size_bytes(),
            payload_bytes: self.payload_bytes(),
            buffer_depth: self.buffer_depth(),
            num_messages: self.num_messages(),
            warmup_messages: self.warmup_messages(),
            num_producers: 1,
            num_consumers: self.num_consumers(),
            producer_throughput_ops_sec: producer.throughput_ops_sec,
            consumer_throughput_ops_sec: avg_consumer_ops,
            latency,
        });
        reporting::attach_child_metrics(&mut result, producer, consumers);
        result
    }

    fn run_benchmark(&self) -> Result<BenchResult, BenchError> {
        let exe = std::env::current_exe()?;
        let timeout = super::env_config::bench_timeout_override_secs()
            .map(Duration::from_secs)
            .unwrap_or_else(|| self.timeout());
        std::env::set_var("BENCH_TIMEOUT", timeout.as_secs().to_string());
        let children = self.launch(&exe)?;
        let ScenarioChildren {
            producer,
            consumers,
            cleanup_paths,
        } = children;

        let cleanup = |paths: Vec<PathBuf>| {
            for path in paths {
                let _ = std::fs::remove_dir_all(&path).or_else(|_| std::fs::remove_file(&path));
            }
        };

        let producer_output = collect_child_output(&self.producer_label(), producer, timeout);
        if !producer_output.success {
            for (consumer_id, child) in consumers.into_iter().enumerate() {
                let _ = collect_child_output(
                    &self.consumer_label(consumer_id),
                    child,
                    Duration::from_secs(1),
                );
            }
            cleanup(cleanup_paths);
            return Err(format!(
                "{} producer failed\nstderr:\n{}",
                self.scenario_name(),
                producer_output.stderr
            )
            .into());
        }

        let consumer_outputs: Vec<_> = consumers
            .into_iter()
            .enumerate()
            .map(|(consumer_id, child)| {
                collect_child_output(&self.consumer_label(consumer_id), child, timeout)
            })
            .collect();
        let producer_metrics: ProducerOutput = parse_child_metrics("producer", &producer_output);
        let consumer_metrics: Vec<ConsumerOutput> = consumer_outputs
            .iter()
            .map(|output| parse_child_metrics("consumer", output))
            .collect();

        let latency = self.aggregate_latency(&consumer_metrics);

        cleanup(cleanup_paths);

        self.print_summary_with_metrics(&producer_metrics, &consumer_metrics, latency.as_ref());
        Ok(self.build_result_with_metrics(&producer_metrics, &consumer_metrics, latency))
    }
}

/// Shared contract for a multiprocess bench binary.
pub trait BenchHarness {
    /// Stable bench name for top-level error reporting.
    fn bench_name(&self) -> &'static str;

    /// Child roles handled by this binary.
    fn child_roles(&self) -> &'static [ChildRole] {
        &[]
    }

    /// Orchestrator entrypoint for the bench binary.
    fn run_orchestrator(&self, args: &[String]) -> BenchRunResult;

    /// Run the bench as either a child role or orchestrator.
    fn run(&self) {
        let raw_args: Vec<String> = std::env::args().collect();
        let args = match super::env_config::apply_timeout_arg(&raw_args) {
            Ok(args) => args,
            Err(error) => {
                eprintln!("{} failed: {error}", self.bench_name());
                std::process::exit(1);
            }
        };
        if dispatch_child_or_exit(&args, self.child_roles()) {
            return;
        }
        if let Err(error) = self.run_orchestrator(&args) {
            eprintln!("{} failed: {error}", self.bench_name());
            std::process::exit(1);
        }
    }
}
