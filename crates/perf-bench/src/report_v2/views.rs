use super::model::{BackendKind, MeasurementKind, ReportBundle, ScenarioOutcome};
use crate::events::format_throughput;
use crate::latency;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonsterSweepBackend {
    Shm,
    Mmap,
}

impl MonsterSweepBackend {
    fn title(self) -> &'static str {
        match self {
            Self::Shm => "SHM",
            Self::Mmap => "MMAP",
        }
    }
}

impl ReportBundle {
    pub fn print_layer_comparison(&self) {
        use tabled::{settings::Style, Table, Tabled};

        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Payload")]
            payload: String,
            #[tabled(rename = "Layer")]
            layer: String,
            #[tabled(rename = "Backend")]
            backend: String,
            #[tabled(rename = "C")]
            consumers: String,
            #[tabled(rename = "Mode")]
            mode: String,
            #[tabled(rename = "Producer\n(ops/s)")]
            prod: String,
            #[tabled(rename = "Consumer\n(ops/s)")]
            cons: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = "% of Raw\nRing")]
            pct: String,
            #[tabled(rename = "Delta\nvs Raw")]
            delta: String,
        }

        let rows: Vec<Row> = self
            .scenarios
            .iter()
            .filter_map(|scenario| {
                let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                    return None;
                };
                let mode = measurement_label(&scenario.config.measurement);
                let baseline = self.raw_baseline(
                    scenario.config.workload.payload_bytes,
                    &scenario.identity.backend,
                    scenario.config.workload.num_consumers,
                    &mode,
                );
                let pct = if scenario.identity.layer == "raw_ring" {
                    "100%".to_string()
                } else if let Some(baseline) = baseline {
                    format!(
                        "{:.0}%",
                        outcome.consumers.average_throughput_ops_sec / baseline * 100.0
                    )
                } else {
                    "-".to_string()
                };
                Some(Row {
                    payload: human_size(scenario.config.workload.payload_bytes),
                    layer: scenario.identity.layer.clone(),
                    backend: backend_label(&scenario.identity.backend),
                    consumers: scenario.config.workload.num_consumers.to_string(),
                    mode: compact_mode(&mode),
                    prod: format_throughput(outcome.producer.throughput_ops_sec),
                    cons: format_throughput(outcome.consumers.average_throughput_ops_sec),
                    p50: outcome
                        .latency
                        .as_ref()
                        .map(|stats| latency::format_ns(stats.p50_ns))
                        .unwrap_or_else(|| "-".to_string()),
                    p99: outcome
                        .latency
                        .as_ref()
                        .map(|stats| latency::format_ns(stats.p99_ns))
                        .unwrap_or_else(|| "-".to_string()),
                    pct,
                    delta: outcome
                        .derived
                        .delta_vs_raw_ring_pct
                        .map(|value| format!("{value:+.0}%"))
                        .unwrap_or_else(|| "-".to_string()),
                })
            })
            .collect();

        println!("\n{}", Table::new(rows).with(Style::modern()));
    }

    pub fn print_nofrag_matrix(&self) {
        use tabled::{settings::Style, Table, Tabled};

        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Payload")]
            size: String,
            #[tabled(rename = "Layer")]
            layer: String,
            #[tabled(rename = "Backend")]
            backend: String,
            #[tabled(rename = "Mode")]
            mode: String,
            #[tabled(rename = "Slot")]
            slot: String,
            #[tabled(rename = "Depth")]
            depth: String,
            #[tabled(rename = "Ring")]
            ring_size: String,
            #[tabled(rename = "P")]
            producers: String,
            #[tabled(rename = "C")]
            consumers: String,
            #[tabled(rename = "Producer\n(ops/s)")]
            prod: String,
            #[tabled(rename = "Consumer\n(ops/s)")]
            cons: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = "% of\nRaw")]
            pct: String,
            #[tabled(rename = "Delta\nvs Raw")]
            delta: String,
        }

        let rows: Vec<Row> = self
            .scenarios
            .iter()
            .filter_map(|scenario| {
                let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                    return None;
                };
                let mode = measurement_label(&scenario.config.measurement);
                let baseline = self.raw_baseline(
                    scenario.config.workload.payload_bytes,
                    &scenario.identity.backend,
                    scenario.config.workload.num_consumers,
                    &mode,
                );
                let pct = if scenario.identity.layer == "raw_ring" {
                    "100%".to_string()
                } else if let Some(baseline) = baseline {
                    format!(
                        "{:.0}%",
                        outcome.consumers.average_throughput_ops_sec / baseline * 100.0
                    )
                } else {
                    "-".to_string()
                };
                Some(Row {
                    size: human_size(scenario.config.workload.payload_bytes),
                    layer: scenario.identity.layer.clone(),
                    backend: backend_label(&scenario.identity.backend),
                    mode: compact_mode(&mode),
                    slot: human_size(scenario.config.workload.message_size_bytes),
                    depth: scenario.config.workload.buffer_depth.to_string(),
                    ring_size: ring_size(
                        scenario.config.workload.message_size_bytes,
                        scenario.config.workload.buffer_depth,
                    ),
                    producers: scenario.config.workload.num_producers.to_string(),
                    consumers: scenario.config.workload.num_consumers.to_string(),
                    prod: format_throughput(outcome.producer.throughput_ops_sec),
                    cons: format_throughput(outcome.consumers.average_throughput_ops_sec),
                    p50: outcome
                        .latency
                        .as_ref()
                        .map(|stats| latency::format_ns(stats.p50_ns))
                        .unwrap_or_else(|| "-".to_string()),
                    p99: outcome
                        .latency
                        .as_ref()
                        .map(|stats| latency::format_ns(stats.p99_ns))
                        .unwrap_or_else(|| "-".to_string()),
                    pct,
                    delta: outcome
                        .derived
                        .delta_vs_raw_ring_pct
                        .map(|value| format!("{value:+.0}%"))
                        .unwrap_or_else(|| "-".to_string()),
                })
            })
            .collect();

        println!("\n{}", Table::new(rows).with(Style::modern()));
    }

    pub fn print_monster_sweep_report(&self, backend: MonsterSweepBackend) {
        use tabled::{settings::Style, Table, Tabled};

        const HW_BW_GBS: f64 = 300.0;

        println!();
        println!("{}", "=".repeat(100));
        println!(
            "        DISRUPTOR-MP MONSTER SWEEP -- {} BACKEND",
            backend.title()
        );
        println!("{}", "=".repeat(100));
        println!();
        println!("System: {} | {}", self.metadata.cpu, self.metadata.platform);
        println!("Memory: 96GB unified | {} GB/s bandwidth", HW_BW_GBS as u64);
        if let Some(ref commit) = self.metadata.git_commit {
            println!("Git:    {} (perf_bench)", commit);
        }
        println!("Time:   {}", self.metadata.timestamp);
        println!("Config: Full payload fill (producer) + checksum (consumer)");
        println!();

        #[derive(Tabled)]
        struct ThroughputRow {
            #[tabled(rename = "Payload")]
            payload: String,
            #[tabled(rename = "Total\nMemory")]
            total_mem: String,
            #[tabled(rename = "Events")]
            events: String,
            #[tabled(rename = "Producer\n(ops/s)")]
            prod: String,
            #[tabled(rename = "Consumer\n(ops/s)")]
            cons: String,
            #[tabled(rename = "Data Rate\n(MB/s)")]
            data_rate: String,
            #[tabled(rename = "Bandwidth\n(GB/s)")]
            bw: String,
            #[tabled(rename = "% of HW\nLimit")]
            pct: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = " ")]
            status: String,
        }

        let throughput_rows: Vec<ThroughputRow> = self
            .scenarios
            .iter()
            .filter_map(|scenario| {
                let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                    return None;
                };
                if scenario.config.workload.num_consumers != 1
                    || !matches!(scenario.config.measurement, MeasurementKind::MaxThroughput)
                {
                    return None;
                }
                let size = scenario.config.workload.message_size_bytes;
                let bw = outcome.consumers.average_throughput_ops_sec * size as f64 / 1e9;
                let pct = bw / HW_BW_GBS * 100.0;
                let is_signal = scenario.identity.scenario.contains("SIG");
                Some(ThroughputRow {
                    payload: if is_signal {
                        "signal".to_string()
                    } else {
                        human_size(size)
                    },
                    total_mem: monster_sweep_ring_label(
                        size,
                        scenario.config.workload.buffer_depth,
                    ),
                    events: human_events(scenario.config.workload.num_messages),
                    prod: format_throughput(outcome.producer.throughput_ops_sec),
                    cons: format_throughput(outcome.consumers.average_throughput_ops_sec),
                    data_rate: format!(
                        "{:.0}",
                        outcome.consumers.average_throughput_ops_sec * size as f64 / 1e6
                    ),
                    bw: format!("{:.1}", bw),
                    pct: if is_signal {
                        "seq-ctr".into()
                    } else {
                        format!("{pct:.1}%")
                    },
                    p50: outcome
                        .latency
                        .as_ref()
                        .map(|latency| crate::latency::format_ns(latency.p50_ns))
                        .unwrap_or("-".into()),
                    p99: outcome
                        .latency
                        .as_ref()
                        .map(|latency| crate::latency::format_ns(latency.p99_ns))
                        .unwrap_or("-".into()),
                    status: if is_signal || pct > 10.0 {
                        "✓".to_string()
                    } else if pct > 1.0 {
                        "△".to_string()
                    } else {
                        "✗".to_string()
                    },
                })
            })
            .collect();

        if !throughput_rows.is_empty() {
            println!("{}", "-".repeat(100));
            println!("  THROUGHPUT SWEEP (1p1c)");
            println!("{}", "-".repeat(100));
            println!("{}", Table::new(throughput_rows).with(Style::modern()));
            println!("Legend: ✓ = >10% BW efficiency | △ = >1% | ✗ = <1%");
            println!();
        }

        #[derive(Tabled)]
        struct ScalingRow {
            #[tabled(rename = "Payload")]
            payload: String,
            #[tabled(rename = "1p1c")]
            c1: String,
            #[tabled(rename = "1p2c")]
            c2: String,
            #[tabled(rename = "1p4c")]
            c4: String,
            #[tabled(rename = "1p6c")]
            c6: String,
            #[tabled(rename = "1p8c")]
            c8: String,
            #[tabled(rename = "1p10c")]
            c10: String,
            #[tabled(rename = "1p12c")]
            c12: String,
        }

        let scaling_sizes: Vec<usize> = self
            .scenarios
            .iter()
            .filter(|scenario| {
                matches!(scenario.config.measurement, MeasurementKind::MaxThroughput)
                    && !scenario.identity.scenario.contains("SIG")
            })
            .map(|scenario| scenario.config.workload.message_size_bytes)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        let mut scaling_rows = Vec::new();
        for size in &scaling_sizes {
            let find = |consumers: usize| -> Option<f64> {
                self.scenarios.iter().find_map(|scenario| {
                    let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                        return None;
                    };
                    (scenario.config.workload.message_size_bytes == *size
                        && scenario.config.workload.num_consumers == consumers
                        && matches!(scenario.config.measurement, MeasurementKind::MaxThroughput))
                    .then_some(outcome.consumers.average_throughput_ops_sec)
                })
            };

            let c1 = find(1);
            let has_multi = [2, 4, 6, 8, 10, 12]
                .iter()
                .any(|&consumers| find(consumers).is_some());
            if has_multi {
                let format_consumer = |consumers: usize| -> String {
                    find(consumers)
                        .map(|value| monster_sweep_scaling_cell(value, c1))
                        .unwrap_or("-".into())
                };
                scaling_rows.push(ScalingRow {
                    payload: human_size(*size),
                    c1: c1.map(format_throughput).unwrap_or("-".into()),
                    c2: format_consumer(2),
                    c4: format_consumer(4),
                    c6: format_consumer(6),
                    c8: format_consumer(8),
                    c10: format_consumer(10),
                    c12: format_consumer(12),
                });
            }
        }

        if !scaling_rows.is_empty() {
            println!("{}", "-".repeat(100));
            println!("  CONSUMER SCALING");
            println!("{}", "-".repeat(100));
            println!("{}", Table::new(scaling_rows).with(Style::modern()));
            println!();
        }

        if let Some(matrix) = self.build_co_matrix(false) {
            println!("{}", "-".repeat(100));
            println!("  COORDINATED OMISSION LATENCY MATRIX");
            println!("{}", "-".repeat(100));
            println!("{matrix}");
            println!("Legend: ✓ P99 CO < 1us (Excellent) | △ P99 CO < 10ms (Good) | ✗ P99 CO >= 10ms (Saturated)");
            println!("All latencies are Coordinated Omission corrected, showing true user-experienced delays");
            println!();
        }
    }

    pub fn write_monster_sweep_markdown(
        &self,
        path: &str,
        backend: MonsterSweepBackend,
    ) -> std::io::Result<()> {
        use tabled::{settings::Style, Table, Tabled};

        const HW_BW_GBS: f64 = 300.0;

        let mut md = String::new();
        md.push_str(&format!(
            "# Disruptor-MP Monster Sweep — {} Backend\n\n",
            backend.title()
        ));
        md.push_str(&format!("- **CPU**: {}\n", self.metadata.cpu));
        md.push_str(&format!("- **Platform**: {}\n", self.metadata.platform));
        md.push_str("- **Memory**: 96GB unified, 300 GB/s bandwidth\n");
        if let Some(ref commit) = self.metadata.git_commit {
            md.push_str(&format!("- **Git**: `{}`\n", commit));
        }
        md.push_str(&format!("- **Timestamp**: {}\n", self.metadata.timestamp));
        md.push_str("- **Config**: Full payload fill (producer) + checksum (consumer)\n\n");

        #[derive(Tabled)]
        struct ThroughputRow {
            #[tabled(rename = "Payload")]
            payload: String,
            #[tabled(rename = "Total Memory")]
            total_mem: String,
            #[tabled(rename = "Events")]
            events: String,
            #[tabled(rename = "Producer (ops/s)")]
            prod: String,
            #[tabled(rename = "Consumer (ops/s)")]
            cons: String,
            #[tabled(rename = "Data Rate (MB/s)")]
            data_rate: String,
            #[tabled(rename = "BW (GB/s)")]
            bw: String,
            #[tabled(rename = "% HW Limit")]
            pct: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = " ")]
            status: String,
        }

        let throughput_rows: Vec<ThroughputRow> = self
            .scenarios
            .iter()
            .filter_map(|scenario| {
                let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                    return None;
                };
                if scenario.config.workload.num_consumers != 1
                    || !matches!(scenario.config.measurement, MeasurementKind::MaxThroughput)
                {
                    return None;
                }
                let size = scenario.config.workload.message_size_bytes;
                let bw = outcome.consumers.average_throughput_ops_sec * size as f64 / 1e9;
                let pct = bw / HW_BW_GBS * 100.0;
                let is_signal = scenario.identity.scenario.contains("SIG");
                Some(ThroughputRow {
                    payload: if is_signal {
                        "signal".into()
                    } else {
                        human_size(size)
                    },
                    total_mem: monster_sweep_ring_label(
                        size,
                        scenario.config.workload.buffer_depth,
                    ),
                    events: human_events(scenario.config.workload.num_messages),
                    prod: format_throughput(outcome.producer.throughput_ops_sec),
                    cons: format_throughput(outcome.consumers.average_throughput_ops_sec),
                    data_rate: format!(
                        "{:.0}",
                        outcome.consumers.average_throughput_ops_sec * size as f64 / 1e6
                    ),
                    bw: format!("{:.1}", bw),
                    pct: if is_signal {
                        "seq-ctr".into()
                    } else {
                        format!("{pct:.1}%")
                    },
                    p50: outcome
                        .latency
                        .as_ref()
                        .map(|latency| crate::latency::format_ns(latency.p50_ns))
                        .unwrap_or("-".into()),
                    p99: outcome
                        .latency
                        .as_ref()
                        .map(|latency| crate::latency::format_ns(latency.p99_ns))
                        .unwrap_or("-".into()),
                    status: if is_signal || pct > 10.0 {
                        "✓".into()
                    } else if pct > 1.0 {
                        "△".into()
                    } else {
                        "✗".into()
                    },
                })
            })
            .collect();

        md.push_str("## Throughput Sweep (1p1c)\n\n");
        md.push_str(
            &Table::new(throughput_rows)
                .with(Style::markdown())
                .to_string(),
        );
        md.push_str("\n\n");

        if let Some(matrix) = self.build_co_matrix(true) {
            md.push_str("## Coordinated Omission Latency Matrix\n\n");
            md.push_str(&matrix);
            md.push('\n');
        }

        std::fs::write(path, md)
    }

    fn build_co_matrix(&self, use_markdown: bool) -> Option<String> {
        use tabled::builder::Builder;
        use tabled::settings::{object::Cell, Alignment, Span, Style};

        let co_results: Vec<_> = self
            .scenarios
            .iter()
            .filter_map(|scenario| {
                let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                    return None;
                };
                matches!(scenario.config.measurement, MeasurementKind::CoAware { .. })
                    .then_some((scenario, outcome))
            })
            .filter(|(_, outcome)| outcome.latency.is_some())
            .collect();

        if co_results.is_empty() {
            return None;
        }

        let rates: Vec<u64> = co_results
            .iter()
            .filter_map(|(scenario, _)| match scenario.config.measurement {
                MeasurementKind::CoAware { target_rate } => Some(target_rate),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let sizes: Vec<usize> = co_results
            .iter()
            .map(|(scenario, _)| scenario.config.workload.message_size_bytes)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        let fmt_rate = |rate: u64| -> String {
            if rate >= 1_000_000 {
                format!("{}M/s", rate / 1_000_000)
            } else {
                format!("{}K/s", rate / 1000)
            }
        };

        let mut builder = Builder::default();
        let mut header0 = vec!["Size".to_string()];
        for rate in &rates {
            header0.push(fmt_rate(*rate));
            header0.push(String::new());
            header0.push(String::new());
            header0.push(String::new());
        }
        builder.push_record(header0);

        let mut header1 = vec![String::new()];
        for _ in &rates {
            header1.push("P50".into());
            header1.push("P90".into());
            header1.push("P99".into());
            header1.push("P99.9".into());
        }
        builder.push_record(header1);

        for size in &sizes {
            let mut row = vec![human_size(*size)];
            for rate in &rates {
                let entry = co_results.iter().find(|(scenario, _)| {
                    scenario.config.workload.message_size_bytes == *size
                        && matches!(
                            scenario.config.measurement,
                            MeasurementKind::CoAware { target_rate } if target_rate == *rate
                        )
                });
                if let Some((_, outcome)) = entry {
                    let stats = outcome.latency.as_ref().expect("co latency");
                    let indicator = if stats.p99_ns < 1_000 {
                        "✓"
                    } else if stats.p99_ns < 10_000_000 {
                        "△"
                    } else {
                        "✗"
                    };
                    row.push(latency::format_ns(stats.p50_ns));
                    row.push(latency::format_ns(stats.p90_ns));
                    row.push(format!("{}{}", latency::format_ns(stats.p99_ns), indicator));
                    row.push(latency::format_ns(stats.p999_ns));
                } else {
                    row.extend(["-".into(), "-".into(), "-".into(), "-".into()]);
                }
            }
            builder.push_record(row);
        }

        let mut table = builder.build();
        for (index, _) in rates.iter().enumerate() {
            let col = 1 + index * 4;
            table.modify(Cell::new(0, col), Span::column(4));
            table.modify(Cell::new(0, col), Alignment::center());
        }

        if use_markdown {
            table.with(Style::markdown());
        } else {
            table.with(Style::modern());
        }
        Some(table.to_string())
    }

    fn raw_baseline(
        &self,
        payload_bytes: usize,
        backend: &BackendKind,
        consumers: usize,
        mode: &str,
    ) -> Option<f64> {
        self.scenarios.iter().find_map(|scenario| {
            let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                return None;
            };
            (scenario.identity.layer == "raw_ring"
                && scenario.config.workload.payload_bytes == payload_bytes
                && &scenario.identity.backend == backend
                && scenario.config.workload.num_consumers == consumers
                && measurement_label(&scenario.config.measurement) == mode)
                .then_some(outcome.consumers.average_throughput_ops_sec)
        })
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

fn compact_mode(mode: &str) -> String {
    mode.strip_prefix("co_aware@")
        .map(|rate| format!("CO@{}", rate))
        .unwrap_or_else(|| mode.to_string())
}

fn backend_label(backend: &BackendKind) -> String {
    match backend {
        BackendKind::Shm => "shm".to_string(),
        BackendKind::Mmap => "mmap".to_string(),
        BackendKind::Layout => "layout".to_string(),
        BackendKind::Unknown(other) => other.clone(),
    }
}

fn human_size(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{}MB", bytes / 1_048_576)
    } else if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{}B", bytes)
    }
}

fn ring_size(slot_size: usize, depth: usize) -> String {
    let total = slot_size * depth;
    if total >= 1024 * 1024 * 1024 {
        format!("{:.1}GB", total as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if total >= 1024 * 1024 {
        format!("{}MB", total / (1024 * 1024))
    } else {
        format!("{}KB", total / 1024)
    }
}

fn human_events(events: u64) -> String {
    if events >= 1_000_000 {
        format!("{:.1}M", events as f64 / 1_000_000.0)
    } else if events >= 1_000 {
        format!("{:.0}K", events as f64 / 1_000.0)
    } else {
        events.to_string()
    }
}

fn monster_sweep_ring_label(size: usize, depth: usize) -> String {
    let total = size.saturating_mul(depth);
    if total >= 1024 * 1024 * 1024 {
        format!("{:.1}GB", total as f64 / (1024.0 * 1024.0 * 1024.0))
    } else {
        format!("{:.0}MB", total as f64 / (1024.0 * 1024.0))
    }
}

fn monster_sweep_scaling_cell(value: f64, baseline: Option<f64>) -> String {
    match baseline {
        Some(baseline) if baseline > 0.0 => {
            format!(
                "{} ({:.0}%)",
                format_throughput(value),
                value / baseline * 100.0
            )
        }
        _ => format_throughput(value),
    }
}
