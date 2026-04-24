use super::model::{
    BackendKind, CoordinationKind, DiscoveryKind, FramingKind, MeasurementKind, ReportBundle,
    ScenarioOutcome, WaitStrategyKind, ZeroCopyKind,
};
use crate::events::format_throughput;
use crate::latency::format_ns;
use std::collections::BTreeMap;
use std::iter::repeat_n;

const TREE_COL_BUF: usize = 4;

// Width policy: never wrap or truncate the tree label column. Instead, widen the
// label span to the longest rendered node name and keep parent rows tree-only.
//
// Multi-line rendering: each benchmark result renders as 2 lines:
//   Line 1 (primary):   name + measurement data (throughput, latency, BW)
//   Line 2 (secondary): config context (layer, codec, mode, wait strategy)
// Continuation lines use │ prefix (divan-style) to maintain tree structure.

#[derive(Debug, Clone)]
struct TreeLeaf {
    benchmark: String,
    backend: String,
    layer: String,
    label: String,
    sort_key: String,
    primary_columns: Vec<String>,
    secondary_columns: Vec<String>,
}

#[derive(Debug, Clone)]
enum TreeNode {
    Parent {
        name: String,
        children: Vec<TreeNode>,
    },
    Leaf {
        name: String,
        primary_columns: Vec<String>,
        secondary_columns: Vec<String>,
    },
}

#[derive(Debug, Clone)]
struct TreePainter {
    max_name_span: usize,
    primary_widths: Vec<usize>,
    depth: usize,
    current_prefix: String,
    out: String,
}

impl TreePainter {
    fn new(max_name_span: usize, primary_widths: Vec<usize>) -> Self {
        Self {
            max_name_span,
            primary_widths,
            depth: 0,
            current_prefix: String::new(),
            out: String::new(),
        }
    }

    fn has_primary_columns(&self) -> bool {
        self.primary_widths.iter().any(|width| *width > 0)
    }

    fn start_root(&mut self, title: &str, primary_headings: &[&str], _secondary_headings: &[&str]) {
        self.out.push_str(title);
        if self.has_primary_columns() {
            right_pad_tree_name(&mut self.out, &mut self.max_name_span);
            write_tree_columns(&mut self.out, primary_headings, &mut self.primary_widths);
        }
        self.out.push('\n');
    }

    fn start_parent(&mut self, name: &str, is_last: bool) {
        self.out.push_str(&self.current_prefix);
        self.out.push_str(if is_last { "╰─ " } else { "├─ " });
        self.out.push_str(name);
        self.out.push('\n');

        self.depth += 1;
        self.current_prefix
            .push_str(if is_last { "   " } else { "│  " });
    }

    fn finish_parent(&mut self) {
        if self.depth == 0 {
            return;
        }

        self.depth -= 1;
        let new_prefix_len = {
            let mut iter = self.current_prefix.chars();
            let _ = iter.by_ref().rev().nth(2);
            iter.as_str().len()
        };
        self.current_prefix.truncate(new_prefix_len);
    }

    fn write_leaf(
        &mut self,
        name: &str,
        is_last: bool,
        primary: &[String],
        secondary: &[String],
    ) {
        // Line 1: name + primary measurement data
        self.out.push_str(&self.current_prefix);
        self.out.push_str(if is_last { "╰─ " } else { "├─ " });
        self.out.push_str(name);
        right_pad_tree_name(&mut self.out, &mut self.max_name_span);
        let primary_refs: Vec<&str> = primary.iter().map(String::as_str).collect();
        write_tree_columns(&mut self.out, &primary_refs, &mut self.primary_widths);
        self.out.push('\n');

        // Line 2: compact config info string (not columnar)
        let info_parts: Vec<String> = SECONDARY_HEADINGS
            .iter()
            .zip(secondary.iter())
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        {
            self.out.push_str(&self.current_prefix);
            if !is_last {
                self.out.push('│');
            }
            right_pad_tree_name_blank(&mut self.out, self.max_name_span);
            self.out.push_str(&info_parts.join("  "));
            self.out.push('\n');
        }
    }

    fn finish(self) -> String {
        self.out
    }
}

impl TreeNode {
    fn max_name_span(nodes: &[Self], depth: usize) -> usize {
        const DEPTH_COLS: usize = 3;

        nodes
            .iter()
            .map(|node| match node {
                TreeNode::Parent { name, children } => {
                    let node_span = depth * DEPTH_COLS + name.chars().count();
                    node_span.max(Self::max_name_span(children, depth + 1))
                }
                TreeNode::Leaf { name, .. } => depth * DEPTH_COLS + name.chars().count(),
            })
            .max()
            .unwrap_or_default()
    }
}

impl ReportBundle {
    pub fn render_tree(&self) -> String {
        if self
            .scenarios
            .iter()
            .all(|scenario| matches!(scenario.outcome, ScenarioOutcome::Layout(_)))
        {
            return render_layout_tree(
                &self.infer_tree_title(),
                &["avg ns/op", "budget ns", "result"],
                self.scenarios
                    .iter()
                    .map(|scenario| {
                        let ScenarioOutcome::Layout(layout) = &scenario.outcome else {
                            unreachable!("layout-only path");
                        };
                        LayoutLeaf {
                            benchmark: scenario.identity.benchmark.clone(),
                            backend: backend_label(&scenario.identity.backend),
                            layer: scenario.identity.layer.clone(),
                            label: scenario.identity.scenario.clone(),
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
            self.scenarios
                .iter()
                .map(|scenario| {
                    let (
                        payload,
                        depth,
                        consumers,
                        prod_ops,
                        cons_ops,
                        prod_bw,
                        cons_bw,
                        p50,
                        p99,
                        p999,
                        pct_raw,
                        delta_raw,
                    ) = match &scenario.outcome {
                        ScenarioOutcome::Throughput(outcome) => (
                            human_size(scenario.config.workload.payload_bytes),
                            human_depth(scenario.config.workload.buffer_depth),
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
                                .latency
                                .as_ref()
                                .map(|stats| format_ns(stats.p999_ns))
                                .unwrap_or_else(|| "-".to_string()),
                            outcome
                                .derived
                                .pct_of_raw_ring
                                .map(|pct| format!("{pct:.0}%"))
                                .unwrap_or_else(|| "-".to_string()),
                            outcome
                                .derived
                                .delta_vs_raw_ring_pct
                                .map(|pct| format!("{pct:+.0}%"))
                                .unwrap_or_else(|| "-".to_string()),
                        ),
                        ScenarioOutcome::Layout(_) => (
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
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

                    // Secondary line: config context
                    let codec = scenario
                        .identity
                        .codec
                        .as_ref()
                        .map(|c| format!("{c:?}").to_lowercase())
                        .unwrap_or_else(|| "none".to_string());
                    let access_avg = match &scenario.outcome {
                        ScenarioOutcome::Throughput(outcome) => outcome
                            .derived
                            .access_avg_ns
                            .map(format_avg_ns)
                            .unwrap_or_else(|| "none".to_string()),
                        _ => "none".to_string(),
                    };
                    let access_speedup = match &scenario.outcome {
                        ScenarioOutcome::Throughput(outcome) => outcome
                            .derived
                            .access_vs_decode_speedup
                            .map(|value| format!("{value:.1}x"))
                            .unwrap_or_else(|| "none".to_string()),
                        _ => "none".to_string(),
                    };

                    let batch = scenario
                        .config
                        .workload
                        .batch_size
                        .map(|b| b.to_string())
                        .unwrap_or_else(|| "none".to_string());

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
                        primary_columns: vec![
                            payload, depth, consumers, prod_ops, cons_ops, prod_bw, cons_bw, p50,
                            p99, p999, pct_raw, delta_raw,
                        ],
                        secondary_columns: vec![
                            measurement_label(&scenario.config.measurement),
                            wait_strategy_label(&scenario.config.transport.wait_strategy),
                            codec,
                            batch,
                            zero_copy_label(scenario.config.transport.zero_copy.as_ref()),
                            framing_label(scenario.config.transport.framing.as_ref()),
                            coordination_label(scenario.config.transport.coordination.as_ref()),
                            discovery_label(scenario.config.transport.discovery.as_ref()),
                            access_avg,
                            access_speedup,
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

// --- Primary headings (line 1: measurements) ---
const PRIMARY_HEADINGS: &[&str] = &[
    "payload", "depth", "C", "prod ops/s", "cons ops/s", "prod BW", "cons BW", "p50", "p99",
    "p999", "%raw", "Δraw",
];

// --- Secondary headings (line 2: config context) ---
// Always shown explicitly so every config dimension is visible.
const SECONDARY_HEADINGS: &[&str] = &[
    "mode", "wait", "codec", "batch", "zc-codec", "frag", "coord", "cons_disc", "zc-access",
    "zc-speedup",
];

fn render_grouped_tree(title: &str, rows: Vec<TreeLeaf>) -> String {
    let mut groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<TreeLeaf>>>> =
        BTreeMap::new();
    let mut primary_widths: Vec<usize> = PRIMARY_HEADINGS
        .iter()
        .map(|name| name.chars().count())
        .collect();

    for row in rows {
        for (index, value) in row.primary_columns.iter().enumerate() {
            if index < primary_widths.len() {
                primary_widths[index] = primary_widths[index].max(value.chars().count());
            }
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

    let collapse_benchmark = groups.len() == 1;
    let nodes = build_tree_nodes(groups, collapse_benchmark);
    let max_name_span = title
        .chars()
        .count()
        .max(TreeNode::max_name_span(&nodes, 1));

    let mut painter = TreePainter::new(max_name_span, primary_widths);
    painter.start_root(title, PRIMARY_HEADINGS, SECONDARY_HEADINGS);
    render_tree_nodes(&mut painter, &nodes);

    painter.finish()
}

fn build_tree_nodes(
    groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<TreeLeaf>>>>,
    collapse_benchmark: bool,
) -> Vec<TreeNode> {
    if collapse_benchmark {
        return groups
            .into_iter()
            .next()
            .map(|(_, backends)| build_backend_nodes(backends))
            .unwrap_or_default();
    }

    groups
        .into_iter()
        .map(|(benchmark, backends)| TreeNode::Parent {
            name: benchmark,
            children: build_backend_nodes(backends),
        })
        .collect()
}

fn build_backend_nodes(
    backends: BTreeMap<String, BTreeMap<String, Vec<TreeLeaf>>>,
) -> Vec<TreeNode> {
    backends
        .into_iter()
        .map(|(backend, layers)| TreeNode::Parent {
            name: backend,
            children: build_layer_nodes(layers),
        })
        .collect()
}

fn build_layer_nodes(layers: BTreeMap<String, Vec<TreeLeaf>>) -> Vec<TreeNode> {
    layers
        .into_iter()
        .map(|(layer, rows)| TreeNode::Parent {
            name: layer,
            children: rows
                .into_iter()
                .map(|row| TreeNode::Leaf {
                    name: row.label,
                    primary_columns: row.primary_columns,
                    secondary_columns: row.secondary_columns,
                })
                .collect(),
        })
        .collect()
}

fn render_tree_nodes(painter: &mut TreePainter, nodes: &[TreeNode]) {
    for (index, node) in nodes.iter().enumerate() {
        let is_last = index + 1 == nodes.len();
        match node {
            TreeNode::Parent { name, children } => {
                painter.start_parent(name, is_last);
                render_tree_nodes(painter, children);
                painter.finish_parent();
            }
            TreeNode::Leaf {
                name,
                primary_columns,
                secondary_columns,
            } => painter.write_leaf(name, is_last, primary_columns, secondary_columns),
        }
    }
}

// --- Layout-only tree (simple single-line per leaf) ---

#[derive(Debug, Clone)]
struct LayoutLeaf {
    benchmark: String,
    backend: String,
    layer: String,
    label: String,
    columns: Vec<String>,
}

fn render_layout_tree(title: &str, headings: &[&str], rows: Vec<LayoutLeaf>) -> String {
    let mut groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<LayoutLeaf>>>> =
        BTreeMap::new();
    let mut column_widths: Vec<usize> = headings.iter().map(|name| name.chars().count()).collect();

    for row in rows {
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

    let collapse_benchmark = groups.len() == 1;
    let layout_nodes = build_layout_tree_nodes(groups, collapse_benchmark);
    let max_name_span = title
        .chars()
        .count()
        .max(layout_max_name_span(&layout_nodes, 1));

    let mut painter = LayoutTreePainter::new(max_name_span, column_widths);
    painter.start_root(title, headings);
    render_layout_tree_nodes(&mut painter, &layout_nodes);
    painter.finish()
}

// Simple single-line painter for layout validation results
#[derive(Debug, Clone)]
struct LayoutTreePainter {
    max_name_span: usize,
    column_widths: Vec<usize>,
    depth: usize,
    current_prefix: String,
    out: String,
}

enum LayoutTreeNode {
    Parent {
        name: String,
        children: Vec<LayoutTreeNode>,
    },
    Leaf {
        name: String,
        columns: Vec<String>,
    },
}

impl LayoutTreePainter {
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
        self.out.push_str(&self.current_prefix);
        self.out.push_str(if is_last { "╰─ " } else { "├─ " });
        self.out.push_str(name);
        self.out.push('\n');
        self.depth += 1;
        self.current_prefix
            .push_str(if is_last { "   " } else { "│  " });
    }

    fn finish_parent(&mut self) {
        if self.depth == 0 {
            return;
        }
        self.depth -= 1;
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

fn build_layout_tree_nodes(
    groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<LayoutLeaf>>>>,
    collapse_benchmark: bool,
) -> Vec<LayoutTreeNode> {
    if collapse_benchmark {
        return groups
            .into_iter()
            .next()
            .map(|(_, backends)| {
                backends
                    .into_iter()
                    .map(|(backend, layers)| LayoutTreeNode::Parent {
                        name: backend,
                        children: layers
                            .into_iter()
                            .map(|(layer, rows)| LayoutTreeNode::Parent {
                                name: layer,
                                children: rows
                                    .into_iter()
                                    .map(|row| LayoutTreeNode::Leaf {
                                        name: row.label,
                                        columns: row.columns,
                                    })
                                    .collect(),
                            })
                            .collect(),
                    })
                    .collect()
            })
            .unwrap_or_default();
    }
    groups
        .into_iter()
        .map(|(benchmark, backends)| LayoutTreeNode::Parent {
            name: benchmark,
            children: backends
                .into_iter()
                .map(|(backend, layers)| LayoutTreeNode::Parent {
                    name: backend,
                    children: layers
                        .into_iter()
                        .map(|(layer, rows)| LayoutTreeNode::Parent {
                            name: layer,
                            children: rows
                                .into_iter()
                                .map(|row| LayoutTreeNode::Leaf {
                                    name: row.label,
                                    columns: row.columns,
                                })
                                .collect(),
                        })
                        .collect(),
                })
                .collect(),
        })
        .collect()
}

fn layout_max_name_span(nodes: &[LayoutTreeNode], depth: usize) -> usize {
    const DEPTH_COLS: usize = 3;
    nodes
        .iter()
        .map(|node| match node {
            LayoutTreeNode::Parent { name, children } => {
                let node_span = depth * DEPTH_COLS + name.chars().count();
                node_span.max(layout_max_name_span(children, depth + 1))
            }
            LayoutTreeNode::Leaf { name, .. } => depth * DEPTH_COLS + name.chars().count(),
        })
        .max()
        .unwrap_or_default()
}

fn render_layout_tree_nodes(painter: &mut LayoutTreePainter, nodes: &[LayoutTreeNode]) {
    for (index, node) in nodes.iter().enumerate() {
        let is_last = index + 1 == nodes.len();
        match node {
            LayoutTreeNode::Parent { name, children } => {
                painter.start_parent(name, is_last);
                render_layout_tree_nodes(painter, children);
                painter.finish_parent();
            }
            LayoutTreeNode::Leaf { name, columns } => painter.write_leaf(name, is_last, columns),
        }
    }
}

// --- Shared helpers ---

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
        None => "none".to_string(),
    }
}

fn discovery_label(discovery: Option<&DiscoveryKind>) -> String {
    match discovery {
        Some(DiscoveryKind::Disabled) => "off".to_string(),
        Some(DiscoveryKind::Enabled { consumers }) => format!("on({consumers})"),
        Some(DiscoveryKind::Unknown(value)) => value.clone(),
        None => "none".to_string(),
    }
}

fn zero_copy_label(zero_copy: Option<&ZeroCopyKind>) -> String {
    match zero_copy {
        Some(ZeroCopyKind::Enabled) => "yes".to_string(),
        Some(ZeroCopyKind::Disabled) => "no".to_string(),
        None => "none".to_string(),
    }
}

fn framing_label(framing: Option<&FramingKind>) -> String {
    match framing {
        Some(FramingKind::None) => "none".to_string(),
        Some(FramingKind::Fixed64K) => "64k".to_string(),
        Some(FramingKind::Fixed64KBatch) => "64k+batch".to_string(),
        Some(FramingKind::RightSized) => "right-sized".to_string(),
        Some(FramingKind::Unknown(value)) => value.clone(),
        None => "none".to_string(),
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
        WaitStrategyKind::BusySpinWithSpinLoopHint => "SpinLoopHint".to_string(),
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

fn format_avg_ns(value: f64) -> String {
    if value < 1_000.0 {
        format!("{value:.1}ns")
    } else {
        format_ns(value.round() as u64)
    }
}

fn right_pad_tree_name(buf: &mut String, max_name_span: &mut usize) {
    let buf_len = buf.chars().count();
    let pad_len = TREE_COL_BUF + max_name_span.saturating_sub(buf_len);
    buf.extend(repeat_n(' ', pad_len));

    if buf_len > *max_name_span {
        *max_name_span = buf_len;
    }
}

fn right_pad_tree_name_blank(buf: &mut String, max_name_span: usize) {
    let buf_len = buf.chars().count();
    let pad_len = TREE_COL_BUF + max_name_span.saturating_sub(buf_len);
    buf.extend(repeat_n(' ', pad_len));
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
                buf.extend(repeat_n(' ', rem_width));
            } else {
                widths[index] = value_width;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::report_v2::{LayoutTargetMeasurement, ReportBundle, ReportBundleCompat};
    use crate::reporting;
    use crate::scenario_v2::layout;

    #[expect(
        clippy::too_many_arguments,
        reason = "tree rendering tests build explicit benchmark rows for readability"
    )]
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
        let expected = concat!(
            "perf-bench/layout_validation    avg ns/op │ budget ns │ result\n",
            "╰─ mmap\n",
            "   ╰─ raw_ring\n",
            "      ╰─ mmap-ring-attach    1234      │ 500000    │ PASS\n",
        );
        assert_eq!(tree, expected);
    }

    #[test]
    fn renders_multi_line_tree_with_secondary_row() {
        let result = make_test_result(
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
            None,
        );

        let mut report = reporting::BenchReport::new();
        report.add(result);
        let tree = report.to_report_v2().render_tree();

        // Verify primary line has measurement data
        assert!(tree.contains("10.00M"), "should contain producer throughput");
        assert!(tree.contains("9.50M"), "should contain consumer throughput");

        // Verify secondary line has config context
        assert!(tree.contains("max_throughput"), "should contain mode");
        assert!(tree.contains("BusySpin"), "should contain wait strategy");
        assert!(tree.contains("none"), "should contain framing");

        // Verify tree structure
        assert!(tree.contains("╰─ shm"), "should have backend node");
        assert!(tree.contains("╰─ raw_ring"), "should have layer node");
        assert!(
            tree.contains("signal 1p2c 64B"),
            "should have scenario label"
        );
    }

    #[test]
    fn keeps_long_labels_unwrapped_and_untruncated() {
        let result = make_test_result(
            "raw_ring_shm",
            "signal_extremely_verbose_scenario_name_that_should_not_be_truncated_1p12c_1MB",
            "shm",
            "raw_ring",
            reporting::BenchTransportSpec::benchmark_shm(12)
                .with_zero_copy(false)
                .with_framing("none"),
            1_048_576,
            65_536,
            10_000,
            12,
            1_000.0,
            1_000.0,
            None,
        );

        let mut report = reporting::BenchReport::new();
        report.add(result);
        let tree = report.to_report_v2().render_tree();

        assert!(tree.contains(
            "signal extremely verbose scenario name that should not be truncated 1p12c 1MB"
        ));
        assert!(!tree.contains("..."));
    }
}
