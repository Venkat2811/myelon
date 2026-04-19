use super::model::{
    BackendKind, CoordinationKind, DiscoveryKind, FramingKind, MeasurementKind, ReportBundle,
    ScenarioOutcome, WaitStrategyKind, ZeroCopyKind,
};
use crate::events::format_throughput;
use crate::latency::format_ns;
use std::collections::BTreeMap;
use std::iter::repeat;

const TREE_COL_BUF: usize = 2;

#[derive(Debug, Clone)]
struct TreeLeaf {
    benchmark: String,
    backend: String,
    layer: String,
    label: String,
    sort_key: String,
    columns: Vec<String>,
}

#[derive(Debug, Clone)]
struct TreePainter {
    max_name_span: usize,
    column_widths: Vec<usize>,
    depth: usize,
    current_prefix: String,
    out: String,
}

impl TreePainter {
    fn new(max_name_span: usize, column_widths: Vec<usize>) -> Self {
        Self {
            max_name_span,
            column_widths,
            depth: 0,
            current_prefix: String::new(),
            out: String::new(),
        }
    }

    fn start_root(&mut self, title: &str, headings: &[&str]) {
        self.out.push_str(title);
        right_pad_tree_name(&mut self.out, &mut self.max_name_span);
        write_tree_columns(&mut self.out, headings, &mut self.column_widths);
        self.out.push('\n');
    }

    fn start_parent(&mut self, name: &str, is_last: bool) {
        let is_top_level = self.depth == 0;
        if !is_top_level {
            self.out.push_str(&self.current_prefix);
            self.out.push_str(if is_last { "╰─ " } else { "├─ " });
        }
        self.out.push_str(name);
        self.out.push('\n');

        self.depth += 1;
        if !is_top_level {
            self.current_prefix
                .push_str(if is_last { "   " } else { "│  " });
        }
    }

    fn finish_parent(&mut self) {
        self.depth = self.depth.saturating_sub(1);
        if self.depth == 0 {
            return;
        }

        let new_prefix_len = {
            let mut iter = self.current_prefix.chars();
            let _ = iter.by_ref().rev().nth(2);
            iter.as_str().len()
        };
        self.current_prefix.truncate(new_prefix_len);
    }

    fn write_leaf(&mut self, name: &str, is_last: bool, columns: &[String]) {
        self.out.push_str(&self.current_prefix);
        self.out.push_str(if is_last { "╰─ " } else { "├─ " });
        self.out.push_str(name);
        right_pad_tree_name(&mut self.out, &mut self.max_name_span);
        let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        write_tree_columns(&mut self.out, &column_refs, &mut self.column_widths);
        self.out.push('\n');
    }

    fn finish(self) -> String {
        self.out
    }
}

impl ReportBundle {
    pub fn render_tree(&self) -> String {
        if self
            .scenarios
            .iter()
            .all(|scenario| matches!(scenario.outcome, ScenarioOutcome::Layout(_)))
        {
            return render_grouped_tree(
                &self.infer_tree_title(),
                &["avg ns/op", "budget ns", "result"],
                self.scenarios
                    .iter()
                    .map(|scenario| {
                        let ScenarioOutcome::Layout(layout) = &scenario.outcome else {
                            unreachable!("layout-only path");
                        };
                        TreeLeaf {
                            benchmark: scenario.identity.benchmark.clone(),
                            backend: backend_label(&scenario.identity.backend),
                            layer: scenario.identity.layer.clone(),
                            label: scenario.identity.scenario.clone(),
                            sort_key: scenario.identity.scenario.clone(),
                            columns: vec![
                                layout.avg_ns.to_string(),
                                layout.budget_ns.to_string(),
                                if layout.pass {
                                    "PASS".to_string()
                                } else {
                                    "FAIL".to_string()
                                },
                            ],
                        }
                    })
                    .collect(),
            );
        }

        render_grouped_tree(
            &self.infer_tree_title(),
            &[
                "payload",
                "depth",
                "P",
                "C",
                "prod ops/s",
                "cons ops/s",
                "prod BW",
                "cons BW",
                "p50",
                "p99",
                "%raw",
                "hw%",
                "codec%",
                "coord",
                "disc",
                "zc",
                "frame",
                "mode",
                "wait",
            ],
            self.scenarios
                .iter()
                .map(|scenario| {
                    let (
                        payload,
                        depth,
                        producers,
                        consumers,
                        prod_ops,
                        cons_ops,
                        prod_bw,
                        cons_bw,
                        p50,
                        p99,
                        pct_raw,
                        hw_pct,
                        codec_pct,
                    ) = match &scenario.outcome {
                        ScenarioOutcome::Throughput(outcome) => (
                            human_size(scenario.config.workload.payload_bytes),
                            human_depth(scenario.config.workload.buffer_depth),
                            scenario.config.workload.num_producers.to_string(),
                            scenario.config.workload.num_consumers.to_string(),
                            format_throughput(outcome.producer.throughput_ops_sec),
                            format_throughput(outcome.consumers.average_throughput_ops_sec),
                            outcome
                                .producer
                                .data_rate_gbps
                                .map(human_gbps)
                                .unwrap_or_else(|| {
                                    human_data_rate(
                                        outcome.producer.throughput_ops_sec,
                                        scenario.config.workload.message_size_bytes,
                                    )
                                }),
                            outcome
                                .consumers
                                .average_data_rate_gbps
                                .map(human_gbps)
                                .unwrap_or_else(|| {
                                    human_data_rate(
                                        outcome.consumers.average_throughput_ops_sec,
                                        scenario.config.workload.message_size_bytes,
                                    )
                                }),
                            outcome
                                .latency
                                .as_ref()
                                .map(|stats| format_ns(stats.p50_ns))
                                .unwrap_or_else(|| "-".to_string()),
                            outcome
                                .latency
                                .as_ref()
                                .map(|stats| format_ns(stats.p99_ns))
                                .unwrap_or_else(|| "-".to_string()),
                            outcome
                                .derived
                                .pct_of_raw_ring
                                .map(|pct| format!("{pct:.0}%"))
                                .unwrap_or_else(|| "-".to_string()),
                            outcome
                                .derived
                                .hw_efficiency_pct
                                .map(|pct| format!("{pct:.1}%"))
                                .unwrap_or_else(|| "-".to_string()),
                            outcome
                                .phase_timing
                                .as_ref()
                                .map(|timing| format!("{:.0}%", timing.codec_pct()))
                                .unwrap_or_else(|| "-".to_string()),
                        ),
                        ScenarioOutcome::Layout(_) => (
                            "-".to_string(),
                            "-".to_string(),
                            scenario.config.workload.num_producers.to_string(),
                            scenario.config.workload.num_consumers.to_string(),
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                        ),
                    };

                    TreeLeaf {
                        benchmark: scenario.identity.benchmark.clone(),
                        backend: backend_label(&scenario.identity.backend),
                        layer: scenario.identity.layer.clone(),
                        label: compact_scenario_label(&scenario.identity.scenario),
                        sort_key: format!(
                            "{:020}_{:03}_{}",
                            scenario.config.workload.payload_bytes,
                            scenario.config.workload.num_consumers,
                            scenario.identity.scenario
                        ),
                        columns: vec![
                            payload,
                            depth,
                            producers,
                            consumers,
                            prod_ops,
                            cons_ops,
                            prod_bw,
                            cons_bw,
                            p50,
                            p99,
                            pct_raw,
                            hw_pct,
                            codec_pct,
                            coordination_label(scenario.config.transport.coordination.as_ref()),
                            discovery_label(scenario.config.transport.discovery.as_ref()),
                            zero_copy_label(scenario.config.transport.zero_copy.as_ref()),
                            framing_label(scenario.config.transport.framing.as_ref()),
                            measurement_label(&scenario.config.measurement),
                            wait_strategy_label(&scenario.config.transport.wait_strategy),
                        ],
                    }
                })
                .collect(),
        )
    }

    fn infer_tree_title(&self) -> String {
        let mut names = self
            .scenarios
            .iter()
            .map(|scenario| scenario.identity.benchmark.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if names.len() == 1 {
            format!("perf-bench/{}", names.pop_first().unwrap_or("report"))
        } else {
            "perf-bench".to_string()
        }
    }
}

fn render_grouped_tree(title: &str, headings: &[&str], rows: Vec<TreeLeaf>) -> String {
    let mut groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<TreeLeaf>>>> =
        BTreeMap::new();
    let mut max_name_span = title.chars().count();
    let mut column_widths: Vec<usize> = headings.iter().map(|name| name.chars().count()).collect();

    for row in rows {
        max_name_span = max_name_span.max(row.benchmark.chars().count());
        max_name_span = max_name_span.max(3 + row.backend.chars().count());
        max_name_span = max_name_span.max(6 + row.layer.chars().count());
        max_name_span = max_name_span.max(9 + row.label.chars().count());
        for (index, value) in row.columns.iter().enumerate() {
            column_widths[index] = column_widths[index].max(value.chars().count());
        }
        groups
            .entry(row.benchmark.clone())
            .or_default()
            .entry(row.backend.clone())
            .or_default()
            .entry(row.layer.clone())
            .or_default()
            .push(row);
    }

    for backends in groups.values_mut() {
        for layers in backends.values_mut() {
            for rows in layers.values_mut() {
                rows.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));
            }
        }
    }

    let mut painter = TreePainter::new(max_name_span, column_widths);
    painter.start_root(title, headings);

    let benchmarks: Vec<_> = groups.into_iter().collect();
    for (benchmark_index, (benchmark, backends)) in benchmarks.iter().enumerate() {
        let benchmark_is_last = benchmark_index + 1 == benchmarks.len();
        painter.start_parent(benchmark, benchmark_is_last);

        let backend_items: Vec<_> = backends.iter().collect();
        for (backend_index, (backend, layers)) in backend_items.iter().enumerate() {
            let backend_is_last = backend_index + 1 == backend_items.len();
            painter.start_parent(backend, backend_is_last);

            let layer_items: Vec<_> = layers.iter().collect();
            for (layer_index, (layer, rows)) in layer_items.iter().enumerate() {
                let layer_is_last = layer_index + 1 == layer_items.len();
                painter.start_parent(layer, layer_is_last);

                for (row_index, row) in rows.iter().enumerate() {
                    let row_is_last = row_index + 1 == rows.len();
                    painter.write_leaf(&row.label, row_is_last, &row.columns);
                }

                painter.finish_parent();
            }

            painter.finish_parent();
        }

        painter.finish_parent();
    }

    painter.finish()
}

fn compact_scenario_label(scenario: &str) -> String {
    scenario
        .strip_prefix("sweep_")
        .unwrap_or(scenario)
        .replace('_', " ")
}

fn backend_label(backend: &BackendKind) -> String {
    match backend {
        BackendKind::Shm => "shm".to_string(),
        BackendKind::Mmap => "mmap".to_string(),
        BackendKind::Layout => "layout".to_string(),
        BackendKind::Unknown(value) => value.clone(),
    }
}

fn coordination_label(coordination: Option<&CoordinationKind>) -> String {
    match coordination {
        Some(CoordinationKind::BenchmarkCoordination) => "bench".to_string(),
        Some(CoordinationKind::UnifiedCoordination) => "unified".to_string(),
        Some(CoordinationKind::MmapBuiltin) => "mmap".to_string(),
        Some(CoordinationKind::Unknown(value)) => value.clone(),
        None => "-".to_string(),
    }
}

fn discovery_label(discovery: Option<&DiscoveryKind>) -> String {
    match discovery {
        Some(DiscoveryKind::Disabled) => "off".to_string(),
        Some(DiscoveryKind::Enabled { consumers }) => format!("on({consumers})"),
        Some(DiscoveryKind::Unknown(value)) => value.clone(),
        None => "-".to_string(),
    }
}

fn zero_copy_label(zero_copy: Option<&ZeroCopyKind>) -> String {
    match zero_copy {
        Some(ZeroCopyKind::Enabled) => "yes".to_string(),
        Some(ZeroCopyKind::Disabled) => "no".to_string(),
        None => "-".to_string(),
    }
}

fn framing_label(framing: Option<&FramingKind>) -> String {
    match framing {
        Some(FramingKind::None) => "none".to_string(),
        Some(FramingKind::Fixed64K) => "64k".to_string(),
        Some(FramingKind::Fixed64KBatch) => "64k+b".to_string(),
        Some(FramingKind::RightSized) => "right".to_string(),
        Some(FramingKind::Unknown(value)) => value.clone(),
        None => "-".to_string(),
    }
}

fn measurement_label(measurement: &MeasurementKind) -> String {
    match measurement {
        MeasurementKind::MaxThroughput => "max_throughput".to_string(),
        MeasurementKind::CoAware { target_rate } => format!("CO@{target_rate}"),
        MeasurementKind::BatchTiming => "batch_timing".to_string(),
        MeasurementKind::LayoutValidation => "layout".to_string(),
        MeasurementKind::Unknown(value) => value.clone(),
    }
}

fn wait_strategy_label(wait: &WaitStrategyKind) -> String {
    match wait {
        WaitStrategyKind::BusySpin => "BusySpin".to_string(),
        WaitStrategyKind::BusySpinWithSpinLoopHint => "BusySpinWithSpinLoopHint".to_string(),
        WaitStrategyKind::Block => "Block".to_string(),
        WaitStrategyKind::Sleep => "Sleep".to_string(),
        WaitStrategyKind::Unknown(value) => value.clone(),
    }
}

fn human_size(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{}MB", bytes / 1_048_576)
    } else if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{bytes}B")
    }
}

fn human_depth(depth: usize) -> String {
    if depth >= 1_000_000 {
        format!("{}M", depth / 1_000_000)
    } else if depth >= 1000 {
        format!("{}K", depth / 1000)
    } else {
        depth.to_string()
    }
}

fn human_data_rate(ops_sec: f64, payload_bytes: usize) -> String {
    let bytes_per_sec = ops_sec * payload_bytes as f64;
    if bytes_per_sec >= 1e9 {
        format!("{:.1}GB/s", bytes_per_sec / 1e9)
    } else if bytes_per_sec >= 1e6 {
        format!("{:.0}MB/s", bytes_per_sec / 1e6)
    } else if bytes_per_sec >= 1e3 {
        format!("{:.0}KB/s", bytes_per_sec / 1e3)
    } else {
        format!("{:.0}B/s", bytes_per_sec)
    }
}

fn human_gbps(gbps: f64) -> String {
    if gbps >= 1.0 {
        format!("{gbps:.1}GB/s")
    } else {
        format!("{:.0}MB/s", gbps * 1000.0)
    }
}

fn right_pad_tree_name(buf: &mut String, max_name_span: &mut usize) {
    let buf_len = buf.chars().count();
    let pad_len = TREE_COL_BUF + max_name_span.saturating_sub(buf_len);
    buf.extend(repeat(' ').take(pad_len));

    if buf_len > *max_name_span {
        *max_name_span = buf_len;
    }
}

fn write_tree_columns(buf: &mut String, columns: &[&str], widths: &mut [usize]) {
    for (index, value) in columns.iter().enumerate() {
        let is_first = index == 0;
        let is_last = index + 1 == columns.len();
        let value_width = value.chars().count();

        if !is_first {
            let mut sep = " │ ";
            if is_last && value_width == 0 {
                sep = " │";
            }
            buf.push_str(sep);
        }

        buf.push_str(value);

        if !is_last {
            if let Some(rem_width) = widths[index].checked_sub(value_width) {
                buf.extend(repeat(' ').take(rem_width));
            } else {
                widths[index] = value_width;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::harness::output::{ConsumerOutput, ProducerOutput};
    use crate::latency::LatencyRecorder;
    use crate::report_v2::{LayoutTargetMeasurement, ReportBundle, ReportBundleCompat};
    use crate::reporting;
    use crate::scenario_v2::layout;
    use std::time::Duration;

    fn make_test_result(
        bench_name: &str,
        scenario: &str,
        backend: &str,
        layer: &str,
        transport: reporting::BenchTransportSpec,
        msg_bytes: usize,
        buffer_depth: usize,
        num_messages: u64,
        num_consumers: usize,
        prod_ops: f64,
        cons_ops: f64,
        latency: Option<crate::latency::LatencyStats>,
    ) -> reporting::BenchResult {
        reporting::make_result(reporting::BenchResultSpec {
            bench_name: bench_name.to_string(),
            scenario: scenario.to_string(),
            backend: backend.to_string(),
            layer: layer.to_string(),
            codec: None,
            measurement_mode: "max_throughput".to_string(),
            wait_strategy: "BusySpin".to_string(),
            transport,
            message_size_bytes: msg_bytes,
            payload_bytes: msg_bytes,
            buffer_depth,
            num_messages,
            warmup_messages: 0,
            num_producers: 1,
            num_consumers,
            producer_throughput_ops_sec: prod_ops,
            consumer_throughput_ops_sec: cons_ops,
            latency,
        })
    }

    #[test]
    fn renders_throughput_tree_from_report_v2_bundle() {
        let mut recorder = LatencyRecorder::default_range();
        recorder.record(250);
        let latency = recorder.stats().expect("latency");

        let mut result = make_test_result(
            "raw_ring_shm",
            "signal_1p2c_64B",
            "shm",
            "raw_ring",
            reporting::BenchTransportSpec::benchmark_shm(2)
                .with_zero_copy(false)
                .with_framing("none"),
            64,
            65_536,
            1_000_000,
            2,
            10_000_000.0,
            9_500_000.0,
            Some(latency.clone()),
        );
        let producer = ProducerOutput::from_elapsed(1_000_000, Duration::from_millis(100), 64);
        let consumer0 =
            ConsumerOutput::from_elapsed(0, 1_000_000, Duration::from_millis(105), 64, 7)
                .with_latency(latency);
        let consumer1 =
            ConsumerOutput::from_elapsed(1, 1_000_000, Duration::from_millis(106), 64, 11);
        reporting::attach_child_metrics(&mut result, &producer, &[consumer0, consumer1]);

        let mut report = reporting::BenchReport::new();
        report.add(result);
        let tree = report.to_report_v2().render_tree();

        assert!(tree.contains("raw_ring_shm"));
        assert!(tree.contains("signal 1p2c 64B"));
        assert!(tree.contains("shm"));
        assert!(tree.contains("prod BW"));
        assert!(tree.contains("%raw"));
        assert!(tree.contains("hw%"));
    }

    #[test]
    fn renders_layout_tree_from_report_v2_bundle() {
        let report = ReportBundle::from_layout_targets(
            "layout_validation",
            50,
            &[LayoutTargetMeasurement::new(
                &layout::MMAP_RING_ATTACH,
                1234,
            )],
        );
        let tree = report.render_tree();

        assert!(tree.contains("layout_validation"));
        assert!(tree.contains("mmap-ring-attach"));
        assert!(tree.contains("PASS"));
    }
}
