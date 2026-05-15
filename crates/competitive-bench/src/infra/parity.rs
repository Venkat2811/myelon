use std::collections::HashMap;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParityConfig {
    pub core_sizes: Vec<usize>,
    pub large_sizes: Vec<usize>,
    pub sizes_small: Vec<usize>,
    pub broker_sizes: Vec<usize>,
    pub extensive_sizes: Vec<usize>,
    pub sizes_extensive: Vec<usize>,
    pub large_size_threshold: usize,
    pub huge_size_threshold: usize,
    pub massive_size_threshold: usize,
    pub giant_size_threshold: usize,
    pub num_messages: u64,
    pub warmup: u64,
    pub rates: Vec<u64>,
    pub large_num_messages: u64,
    pub large_warmup: u64,
    pub large_rates: Vec<u64>,
    pub huge_num_messages: u64,
    pub huge_warmup: u64,
    pub huge_rates: Vec<u64>,
    pub massive_num_messages: u64,
    pub massive_warmup: u64,
    pub massive_rates: Vec<u64>,
    pub giant_num_messages: u64,
    pub giant_warmup: u64,
    pub giant_rates: Vec<u64>,
    pub headon_sizes: Vec<usize>,
    pub headon_sizes_extensive: Vec<usize>,
    pub headon_rate_smoke: u64,
    pub large_headon_rate_smoke: u64,
    pub huge_headon_rate_smoke: u64,
    pub massive_headon_rate_smoke: u64,
    pub giant_headon_rate_smoke: u64,
    pub co_warmup: u64,
    pub co_num_messages: u64,
    pub outdir_expr: String,
    pub headon_dir_expr: String,
    pub ompi_timeout_expr: String,
    pub zmq_timeout_expr: String,
    pub cargo_timeout_expr: String,
}

static CONFIG: OnceLock<ParityConfig> = OnceLock::new();

pub fn config() -> &'static ParityConfig {
    CONFIG.get_or_init(|| parse_config(include_str!("../../config/parity.mk")))
}

fn parse_config(src: &str) -> ParityConfig {
    let values: HashMap<String, String> = src
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (lhs, rhs) = line.split_once(":=").or_else(|| line.split_once("?="))?;
            Some((lhs.trim().to_string(), rhs.trim().to_string()))
        })
        .collect();

    fn resolve_value(
        values: &HashMap<String, String>,
        key: &str,
        visiting: &mut Vec<String>,
    ) -> String {
        if visiting.iter().any(|entry| entry == key) {
            panic!("cyclic parity config reference involving {key}");
        }
        let Some(mut value) = values.get(key).cloned() else {
            return format!("$({key})");
        };
        visiting.push(key.to_string());
        let mut search_from = 0usize;
        loop {
            let Some(start_rel) = value[search_from..].find("$(") else {
                break;
            };
            let start = search_from + start_rel;
            let rest = &value[start + 2..];
            let Some(end_rel) = rest.find(')') else { break };
            let end = start + 2 + end_rel;
            let nested_key = &value[start + 2..end];
            if values.contains_key(nested_key) {
                let nested_value = resolve_value(values, nested_key, visiting);
                value.replace_range(start..=end, &nested_value);
                search_from = start + nested_value.len();
            } else {
                search_from = end + 1;
            }
        }
        visiting.pop();
        value
    }

    let get = |key: &str| -> String { resolve_value(&values, key, &mut Vec::new()) };

    ParityConfig {
        core_sizes: parse_usize_list(&get("CORE_SIZES")),
        large_sizes: parse_usize_list(&get("LARGE_SIZES")),
        sizes_small: parse_usize_list(&get("SIZES_SMALL")),
        broker_sizes: parse_usize_list(&get("BROKER_SIZES")),
        extensive_sizes: parse_usize_list(&get("EXTENSIVE_SIZES")),
        sizes_extensive: parse_usize_list(&get("SIZES_EXTENSIVE")),
        large_size_threshold: get("LARGE_SIZE_THRESHOLD")
            .parse()
            .expect("LARGE_SIZE_THRESHOLD"),
        huge_size_threshold: get("HUGE_SIZE_THRESHOLD")
            .parse()
            .expect("HUGE_SIZE_THRESHOLD"),
        massive_size_threshold: get("MASSIVE_SIZE_THRESHOLD")
            .parse()
            .expect("MASSIVE_SIZE_THRESHOLD"),
        giant_size_threshold: get("GIANT_SIZE_THRESHOLD")
            .parse()
            .expect("GIANT_SIZE_THRESHOLD"),
        num_messages: get("NUM_MESSAGES").parse().expect("NUM_MESSAGES"),
        warmup: get("WARMUP").parse().expect("WARMUP"),
        rates: parse_u64_list(&get("RATES")),
        large_num_messages: get("LARGE_NUM_MESSAGES")
            .parse()
            .expect("LARGE_NUM_MESSAGES"),
        large_warmup: get("LARGE_WARMUP").parse().expect("LARGE_WARMUP"),
        large_rates: parse_u64_list(&get("LARGE_RATES")),
        huge_num_messages: get("HUGE_NUM_MESSAGES").parse().expect("HUGE_NUM_MESSAGES"),
        huge_warmup: get("HUGE_WARMUP").parse().expect("HUGE_WARMUP"),
        huge_rates: parse_u64_list(&get("HUGE_RATES")),
        massive_num_messages: get("MASSIVE_NUM_MESSAGES")
            .parse()
            .expect("MASSIVE_NUM_MESSAGES"),
        massive_warmup: get("MASSIVE_WARMUP").parse().expect("MASSIVE_WARMUP"),
        massive_rates: parse_u64_list(&get("MASSIVE_RATES")),
        giant_num_messages: get("GIANT_NUM_MESSAGES")
            .parse()
            .expect("GIANT_NUM_MESSAGES"),
        giant_warmup: get("GIANT_WARMUP").parse().expect("GIANT_WARMUP"),
        giant_rates: parse_u64_list(&get("GIANT_RATES")),
        headon_sizes: parse_usize_list(&get("HEADON_SIZES")),
        headon_sizes_extensive: parse_usize_list(&get("HEADON_SIZES_EXTENSIVE")),
        headon_rate_smoke: get("HEADON_RATE_SMOKE").parse().expect("HEADON_RATE_SMOKE"),
        large_headon_rate_smoke: get("LARGE_HEADON_RATE_SMOKE")
            .parse()
            .expect("LARGE_HEADON_RATE_SMOKE"),
        huge_headon_rate_smoke: get("HUGE_HEADON_RATE_SMOKE")
            .parse()
            .expect("HUGE_HEADON_RATE_SMOKE"),
        massive_headon_rate_smoke: get("MASSIVE_HEADON_RATE_SMOKE")
            .parse()
            .expect("MASSIVE_HEADON_RATE_SMOKE"),
        giant_headon_rate_smoke: get("GIANT_HEADON_RATE_SMOKE")
            .parse()
            .expect("GIANT_HEADON_RATE_SMOKE"),
        co_warmup: get("CO_WARMUP").parse().expect("CO_WARMUP"),
        co_num_messages: get("CO_NUM_MESSAGES").parse().expect("CO_NUM_MESSAGES"),
        outdir_expr: get("OUTDIR"),
        headon_dir_expr: get("HEADON_DIR"),
        ompi_timeout_expr: get("OMPI_TIMEOUT_SEC"),
        zmq_timeout_expr: get("ZMQ_TIMEOUT_SEC"),
        cargo_timeout_expr: get("CARGO_TIMEOUT_SEC"),
    }
}

// ============================================================
// Size tuning — replaces the Makefile SIZE_TUNING macro.
//
// Given a message size, returns the appropriate (num_messages,
// warmup, rates, headon_rate_smoke, stack_kb) for that tier.
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeTuning {
    pub num_messages: u64,
    pub warmup: u64,
    pub rates: Vec<u64>,
    pub headon_rate_smoke: u64,
    pub stack_kb: usize,
}

/// Returns tuning parameters for a given message size.
/// Mirrors the `SIZE_TUNING` macro from the Makefile exactly.
pub fn tune_for_size(cfg: &ParityConfig, size: usize) -> SizeTuning {
    if size >= cfg.giant_size_threshold {
        SizeTuning {
            num_messages: cfg.giant_num_messages,
            warmup: cfg.giant_warmup,
            rates: cfg.giant_rates.clone(),
            headon_rate_smoke: cfg.giant_headon_rate_smoke,
            stack_kb: 524288,
        }
    } else if size >= cfg.massive_size_threshold {
        SizeTuning {
            num_messages: cfg.massive_num_messages,
            warmup: cfg.massive_warmup,
            rates: cfg.massive_rates.clone(),
            headon_rate_smoke: cfg.massive_headon_rate_smoke,
            stack_kb: 262144,
        }
    } else if size >= cfg.huge_size_threshold {
        SizeTuning {
            num_messages: cfg.huge_num_messages,
            warmup: cfg.huge_warmup,
            rates: cfg.huge_rates.clone(),
            headon_rate_smoke: cfg.huge_headon_rate_smoke,
            stack_kb: 131072,
        }
    } else if size >= cfg.large_size_threshold {
        SizeTuning {
            num_messages: cfg.large_num_messages,
            warmup: cfg.large_warmup,
            rates: cfg.large_rates.clone(),
            headon_rate_smoke: cfg.large_headon_rate_smoke,
            stack_kb: 65536,
        }
    } else {
        SizeTuning {
            num_messages: cfg.num_messages,
            warmup: cfg.warmup,
            rates: cfg.rates.clone(),
            headon_rate_smoke: cfg.headon_rate_smoke,
            stack_kb: 16384,
        }
    }
}

/// Sizes for a given tier name.
pub fn sizes_for_tier(cfg: &ParityConfig, tier: &str) -> Vec<usize> {
    match tier {
        "quick" | "smoke" => cfg.sizes_small.clone(),
        "simple-smoke" => vec![32, 64, 128, 1024, 2048, 4096],
        "extensive" => cfg.sizes_extensive.clone(),
        "headon-smoke" | "headon-full" => cfg.headon_sizes.clone(),
        "headon-extensive" => cfg.headon_sizes_extensive.clone(),
        "broker" => cfg.broker_sizes.clone(),
        _ => cfg.sizes_small.clone(),
    }
}

fn parse_usize_list(value: &str) -> Vec<usize> {
    value
        .split_whitespace()
        .map(|item| item.parse().expect("usize list entry"))
        .collect()
}

fn parse_u64_list(value: &str) -> Vec<u64> {
    value
        .split_whitespace()
        .map(|item| item.parse().expect("u64 list entry"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::config;

    #[test]
    fn parity_sizes_preserve_world_domination_core_and_default_large_extension() {
        let cfg = config();
        assert_eq!(cfg.core_sizes, vec![64, 512, 1024, 2048]);
        assert_eq!(cfg.large_sizes, vec![2 * 1024 * 1024]);
        assert_eq!(cfg.sizes_small, vec![64, 512, 1024, 2048, 2 * 1024 * 1024]);
        assert_eq!(cfg.broker_sizes, vec![64, 1024]);
        assert_eq!(cfg.headon_sizes, vec![64, 512, 1024, 2048, 2 * 1024 * 1024]);
        assert_eq!(cfg.large_size_threshold, 512 * 1024);
    }

    #[test]
    fn parity_sizes_add_non_default_extensive_ladder() {
        let cfg = config();
        assert_eq!(
            cfg.extensive_sizes,
            vec![
                16 * 1024,
                32 * 1024,
                64 * 1024,
                128 * 1024,
                512 * 1024,
                1024 * 1024,
                8 * 1024 * 1024,
                16 * 1024 * 1024,
                32 * 1024 * 1024,
                64 * 1024 * 1024,
            ]
        );
        assert_eq!(
            cfg.sizes_extensive,
            vec![
                64,
                512,
                1024,
                2048,
                16 * 1024,
                32 * 1024,
                64 * 1024,
                128 * 1024,
                512 * 1024,
                1024 * 1024,
                2 * 1024 * 1024,
                8 * 1024 * 1024,
                16 * 1024 * 1024,
                32 * 1024 * 1024,
                64 * 1024 * 1024,
            ]
        );
        assert_eq!(cfg.headon_sizes_extensive, cfg.sizes_extensive);
    }

    #[test]
    fn parity_rates_match_world_domination_core_contract_and_extensive_profiles() {
        let cfg = config();
        assert_eq!(
            cfg.rates,
            vec![200_000, 400_000, 600_000, 800_000, 1_000_000]
        );
        assert_eq!(cfg.headon_rate_smoke, 400_000);
        assert_eq!(cfg.large_rates, vec![200]);
        assert_eq!(cfg.large_headon_rate_smoke, 200);
        assert_eq!(cfg.huge_rates, vec![20]);
        assert_eq!(cfg.huge_headon_rate_smoke, 20);
        assert_eq!(cfg.massive_rates, vec![10]);
        assert_eq!(cfg.massive_headon_rate_smoke, 10);
        assert_eq!(cfg.giant_rates, vec![5]);
        assert_eq!(cfg.giant_headon_rate_smoke, 5);
    }

    #[test]
    fn parity_counts_preserve_core_contract_and_tier_extensive_profiles() {
        let cfg = config();
        assert_eq!(cfg.num_messages, 100_000);
        assert_eq!(cfg.warmup, 10_000);
        assert_eq!(cfg.large_num_messages, 1_000);
        assert_eq!(cfg.large_warmup, 100);
        assert_eq!(cfg.huge_num_messages, 200);
        assert_eq!(cfg.huge_warmup, 20);
        assert_eq!(cfg.massive_num_messages, 100);
        assert_eq!(cfg.massive_warmup, 10);
        assert_eq!(cfg.giant_num_messages, 50);
        assert_eq!(cfg.giant_warmup, 5);
        assert_eq!(cfg.co_warmup, 500);
        assert_eq!(cfg.co_num_messages, 5_000);
        assert_eq!(cfg.large_size_threshold, 512 * 1024);
        assert_eq!(cfg.huge_size_threshold, 8 * 1024 * 1024);
        assert_eq!(cfg.massive_size_threshold, 16 * 1024 * 1024);
        assert_eq!(cfg.giant_size_threshold, 32 * 1024 * 1024);
    }

    #[test]
    fn parity_output_paths_are_durable_and_repo_local() {
        let cfg = config();
        assert_eq!(cfg.outdir_expr, "$(CURDIR)/output/results");
        assert_eq!(cfg.headon_dir_expr, "$(CURDIR)/output/headon");
        assert_eq!(cfg.zmq_timeout_expr, "120");
        assert_eq!(cfg.ompi_timeout_expr, "");
        assert_eq!(cfg.cargo_timeout_expr, "");
    }

    #[test]
    fn tune_for_size_matches_makefile_size_tuning_macro() {
        use super::tune_for_size;
        let cfg = config();

        // Default tier (< 512KB)
        let t = tune_for_size(cfg, 64);
        assert_eq!(t.num_messages, 100_000);
        assert_eq!(t.warmup, 10_000);
        assert_eq!(t.rates, vec![200_000, 400_000, 600_000, 800_000, 1_000_000]);
        assert_eq!(t.headon_rate_smoke, 400_000);
        assert_eq!(t.stack_kb, 16384);

        // Large tier (>= 512KB)
        let t = tune_for_size(cfg, 524_288);
        assert_eq!(t.num_messages, 1_000);
        assert_eq!(t.warmup, 100);
        assert_eq!(t.rates, vec![200]);
        assert_eq!(t.stack_kb, 65536);

        // Huge tier (>= 8MB)
        let t = tune_for_size(cfg, 8_388_608);
        assert_eq!(t.num_messages, 200);
        assert_eq!(t.warmup, 20);
        assert_eq!(t.stack_kb, 131072);

        // Massive tier (>= 16MB)
        let t = tune_for_size(cfg, 16_777_216);
        assert_eq!(t.num_messages, 100);
        assert_eq!(t.warmup, 10);
        assert_eq!(t.stack_kb, 262144);

        // Giant tier (>= 32MB)
        let t = tune_for_size(cfg, 33_554_432);
        assert_eq!(t.num_messages, 50);
        assert_eq!(t.warmup, 5);
        assert_eq!(t.rates, vec![5]);
        assert_eq!(t.stack_kb, 524288);

        // Boundary: just below large threshold → default
        let t = tune_for_size(cfg, 524_287);
        assert_eq!(t.num_messages, 100_000);

        // Boundary: exactly at large threshold → large
        let t = tune_for_size(cfg, 524_288);
        assert_eq!(t.num_messages, 1_000);
    }

    #[test]
    fn sizes_for_tier_returns_correct_lists() {
        use super::sizes_for_tier;
        let cfg = config();

        assert_eq!(sizes_for_tier(cfg, "quick"), cfg.sizes_small);
        assert_eq!(sizes_for_tier(cfg, "extensive"), cfg.sizes_extensive);
        assert_eq!(sizes_for_tier(cfg, "broker"), cfg.broker_sizes);
        assert_eq!(
            sizes_for_tier(cfg, "simple-smoke"),
            vec![32, 64, 128, 1024, 2048, 4096]
        );
    }
}
