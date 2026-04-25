use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "competitive-bench-runner",
    about = "Unified orchestrator for competitive-bench parity suite"
)]
pub struct RunnerCli {
    /// Tier: quick, smoke, simple-smoke, extensive, headon-smoke, headon-full, headon-extensive
    #[arg(long, default_value = "quick")]
    pub tier: String,

    /// Comma-separated adapter filter. "all" runs everything, "internal" runs disruptor+myelon,
    /// or specify individual adapters like "disruptor-shm,rusteron"
    #[arg(long, default_value = "all")]
    pub adapters: String,

    /// Output directory for JSON results
    #[arg(long, default_value = "output/results")]
    pub outdir: PathBuf,

    /// Override sizes (comma-separated bytes). If unset, uses tier defaults.
    #[arg(long)]
    pub sizes: Option<String>,

    /// Override num-messages for all sizes
    #[arg(long)]
    pub msgs: Option<u64>,

    /// Override warmup for all sizes
    #[arg(long)]
    pub warmup: Option<u64>,

    /// Run max-throughput mode
    #[arg(long, default_value_t = true)]
    pub throughput: bool,

    /// Run fixed-rate CO mode
    #[arg(long)]
    pub fixed_rate: bool,

    /// Run broadcast topology
    #[arg(long)]
    pub broadcast: bool,

    /// Timeout per adapter invocation in seconds (0 = no timeout)
    #[arg(long, default_value_t = 300)]
    pub timeout: u64,

    /// Dry-run: print plan without executing
    #[arg(long)]
    pub dry_run: bool,

    /// Workspace manifest path (auto-detected if not set)
    #[arg(long)]
    pub manifest_path: Option<PathBuf>,

    /// Profile for cargo builds (competitive or release)
    #[arg(long, default_value = "competitive")]
    pub profile: String,
}

impl RunnerCli {
    pub fn parse_sizes(&self) -> Option<Vec<usize>> {
        self.sizes.as_ref().map(|s| {
            s.split(',')
                .map(|v| v.trim().parse::<usize>().expect("valid size"))
                .collect()
        })
    }

    pub fn parse_adapters(&self) -> Vec<String> {
        self.adapters
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }

    pub fn resolve_manifest_path(&self) -> PathBuf {
        if let Some(path) = &self.manifest_path {
            return path.clone();
        }
        // Auto-detect: we're in crates/competitive-bench, workspace is ../../Cargo.toml
        let mut path = std::env::current_dir().unwrap_or_default();
        // Walk up until we find Cargo.toml with [workspace]
        for _ in 0..5 {
            let candidate = path.join("Cargo.toml");
            if candidate.exists() {
                if let Ok(content) = std::fs::read_to_string(&candidate) {
                    if content.contains("[workspace]") {
                        return candidate;
                    }
                }
            }
            path = path.join("..");
        }
        PathBuf::from("../../Cargo.toml")
    }
}
