use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParityConfig {
    pub sizes_small: Vec<usize>,
    pub num_messages: u64,
    pub warmup: u64,
    pub rates: Vec<u64>,
    pub headon_sizes: Vec<usize>,
    pub headon_rate_smoke: u64,
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
    CONFIG.get_or_init(|| parse_config(include_str!("../config/parity.mk")))
}

fn parse_config(src: &str) -> ParityConfig {
    let get = |key: &str| -> String {
        src.lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .find_map(|line| {
                let (lhs, rhs) = line.split_once(":=").or_else(|| line.split_once("?="))?;
                (lhs.trim() == key).then(|| rhs.trim().to_string())
            })
            .unwrap_or_else(|| panic!("missing parity key {key}"))
    };

    ParityConfig {
        sizes_small: parse_usize_list(&get("SIZES_SMALL")),
        num_messages: get("NUM_MESSAGES").parse().expect("NUM_MESSAGES"),
        warmup: get("WARMUP").parse().expect("WARMUP"),
        rates: parse_u64_list(&get("RATES")),
        headon_sizes: parse_usize_list(
            &get("HEADON_SIZES").replace("$(SIZES_SMALL)", &get("SIZES_SMALL")),
        ),
        headon_rate_smoke: get("HEADON_RATE_SMOKE").parse().expect("HEADON_RATE_SMOKE"),
        co_warmup: get("CO_WARMUP").parse().expect("CO_WARMUP"),
        co_num_messages: get("CO_NUM_MESSAGES").parse().expect("CO_NUM_MESSAGES"),
        outdir_expr: get("OUTDIR"),
        headon_dir_expr: get("HEADON_DIR"),
        ompi_timeout_expr: get("OMPI_TIMEOUT_SEC"),
        zmq_timeout_expr: get("ZMQ_TIMEOUT_SEC"),
        cargo_timeout_expr: get("CARGO_TIMEOUT_SEC"),
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
    fn parity_sizes_match_world_domination_contract() {
        assert_eq!(config().sizes_small, vec![64, 512, 1024, 2048]);
        assert_eq!(config().headon_sizes, vec![64, 512, 1024, 2048]);
    }

    #[test]
    fn parity_rates_match_world_domination_contract() {
        assert_eq!(
            config().rates,
            vec![200_000, 400_000, 600_000, 800_000, 1_000_000]
        );
        assert_eq!(config().headon_rate_smoke, 400_000);
    }

    #[test]
    fn parity_counts_match_world_domination_contract() {
        let cfg = config();
        assert_eq!(cfg.num_messages, 100_000);
        assert_eq!(cfg.warmup, 10_000);
        assert_eq!(cfg.co_warmup, 500);
        assert_eq!(cfg.co_num_messages, 5_000);
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
}
