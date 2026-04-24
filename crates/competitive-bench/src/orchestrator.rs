use crate::adapter::{parity_adapters, AdapterId, AdapterOrigin};
use crate::parity::config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    ThroughputQuick,
    FixedRateQuick { rate: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalCommand {
    pub adapter: AdapterId,
    pub bench_name: &'static str,
    pub output_file: String,
    pub mode: ExecutionMode,
    pub message_size: usize,
    pub command: String,
}

pub fn internal_quick_commands(out_dir: &str) -> Vec<InternalCommand> {
    let cfg = config();
    let internal = parity_adapters()
        .iter()
        .filter(|spec| spec.origin == AdapterOrigin::Internal)
        .collect::<Vec<_>>();

    let mut commands = Vec::new();
    for adapter in internal {
        let bench_name = bench_name(adapter.id);
        let json_flag = json_flag(adapter.id);
        for size in &cfg.sizes_small {
            let output_file = format!("{out_dir}/{}_{}.json", adapter.output_prefix, size);
            let command = format!(
                "cargo bench -p perf-bench --profile competitive --manifest-path ../../Cargo.toml --bench {bench_name} -- --{json_flag} --no-compare -s {size} -n {} -w {} > {output_file}",
                cfg.num_messages, cfg.warmup
            );
            commands.push(InternalCommand {
                adapter: adapter.id,
                bench_name,
                output_file,
                mode: ExecutionMode::ThroughputQuick,
                message_size: *size,
                command,
            });
        }
    }
    commands
}

pub fn internal_fixed_rate_commands(out_dir: &str) -> Vec<InternalCommand> {
    let cfg = config();
    let internal = parity_adapters()
        .iter()
        .filter(|spec| spec.origin == AdapterOrigin::Internal)
        .collect::<Vec<_>>();

    let mut commands = Vec::new();
    for adapter in internal {
        let bench_name = bench_name(adapter.id);
        let json_flag = json_flag(adapter.id);
        for size in &cfg.sizes_small {
            for rate in &cfg.rates {
                let output_file =
                    format!("{out_dir}/{}_{}_{}.json", adapter.output_prefix, size, rate);
                let command = format!(
                    "cargo bench -p perf-bench --profile competitive --manifest-path ../../Cargo.toml --bench {bench_name} -- --{json_flag} --no-compare -s {size} -n {} -w {} --target-rate {rate} > {output_file}",
                    cfg.num_messages, cfg.warmup
                );
                commands.push(InternalCommand {
                    adapter: adapter.id,
                    bench_name,
                    output_file,
                    mode: ExecutionMode::FixedRateQuick { rate: *rate },
                    message_size: *size,
                    command,
                });
            }
        }
    }
    commands
}

fn bench_name(adapter: AdapterId) -> &'static str {
    match adapter {
        AdapterId::DisruptorShm => "pingpong_shm",
        AdapterId::DisruptorMmap => "pingpong_mmap",
        AdapterId::MyelonRawShm => "pingpong_raw_myelon_shm",
        AdapterId::MyelonRawMmap => "pingpong_raw_myelon_mmap",
        _ => panic!("non-internal adapter is not backed by a perf-bench bench"),
    }
}

fn json_flag(adapter: AdapterId) -> &'static str {
    match adapter {
        AdapterId::DisruptorShm | AdapterId::DisruptorMmap => "json-canonical",
        AdapterId::MyelonRawShm | AdapterId::MyelonRawMmap => "json",
        _ => panic!("non-internal adapter does not have a json flag contract"),
    }
}

#[cfg(test)]
mod tests {
    use super::{internal_fixed_rate_commands, internal_quick_commands, ExecutionMode};
    use crate::adapter::AdapterId;

    #[test]
    fn internal_quick_plan_is_strict_subset_of_world_domination_surface() {
        let commands = internal_quick_commands("output/results");
        assert_eq!(commands.len(), 20);
        assert!(commands
            .iter()
            .all(|cmd| matches!(cmd.mode, ExecutionMode::ThroughputQuick)));
        assert!(commands.iter().all(|cmd| matches!(
            cmd.adapter,
            AdapterId::DisruptorShm
                | AdapterId::DisruptorMmap
                | AdapterId::MyelonRawShm
                | AdapterId::MyelonRawMmap
        )));
        assert!(commands.iter().all(|cmd| matches!(
            cmd.bench_name,
            "pingpong_shm"
                | "pingpong_mmap"
                | "pingpong_raw_myelon_shm"
                | "pingpong_raw_myelon_mmap"
        )));
        assert!(commands.iter().all(|cmd| !cmd.command.contains("/tmp/")));
        assert!(commands
            .iter()
            .all(|cmd| !cmd.command.contains("myelon_layers")));
        assert!(commands.iter().all(|cmd| !cmd.command.contains("framed")));
        assert!(commands.iter().all(|cmd| !cmd.command.contains("codec")));
    }

    #[test]
    fn internal_fixed_rate_plan_is_strict_subset_of_world_domination_surface() {
        let commands = internal_fixed_rate_commands("output/results");
        assert_eq!(commands.len(), 100);
        assert!(commands
            .iter()
            .all(|cmd| matches!(cmd.mode, ExecutionMode::FixedRateQuick { .. })));
        assert!(commands.iter().all(|cmd| matches!(
            cmd.adapter,
            AdapterId::DisruptorShm
                | AdapterId::DisruptorMmap
                | AdapterId::MyelonRawShm
                | AdapterId::MyelonRawMmap
        )));
        assert!(commands
            .iter()
            .all(|cmd| cmd.command.contains("--target-rate")));
        assert!(commands
            .iter()
            .all(|cmd| !cmd.command.contains("batch-timing")));
        assert!(commands
            .iter()
            .all(|cmd| !cmd.command.contains("typed_zero_copy")));
        assert!(commands.iter().all(|cmd| !cmd.command.contains("sweep")));
    }
}
