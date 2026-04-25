use crate::infra::output::reporting::ReportOutputArgs;

#[derive(Debug, Clone, Copy)]
pub struct PayloadSweepSpec {
    pub tag: &'static str,
    pub payload_bytes: usize,
    pub events: u64,
    pub buffer_depth: usize,
    pub batch_size: usize,
    pub raw_slot_bytes: usize,
    pub codec_slot_bytes: usize,
}

pub const TARGET_CONSUMERS: [usize; 6] = [1, 2, 4, 6, 8, 12];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepBackend {
    Shm,
    Mmap,
}

impl SweepBackend {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Shm => "shm",
            Self::Mmap => "mmap",
        }
    }

    pub fn display_label(self) -> &'static str {
        match self {
            Self::Shm => "SHM",
            Self::Mmap => "mmap",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BasicSweepSelection {
    size_filter: Option<String>,
    layer_filter: Option<String>,
    consumers_filter: Option<usize>,
    pub output_args: ReportOutputArgs,
}

impl BasicSweepSelection {
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let size_filter = find_flag_value(args, "--size").filter(|value| value != "all");
        let layer_filter = find_flag_value(args, "--layer").filter(|value| value != "all");
        let consumers_filter = find_flag_value(args, "--consumers")
            .filter(|value| value != "all")
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --consumers value {value}: {error}"))
            })
            .transpose()?;

        Ok(Self {
            size_filter,
            layer_filter,
            consumers_filter,
            output_args: ReportOutputArgs::from_args(args),
        })
    }

    pub fn matches_size(&self, tag: &str) -> bool {
        self.size_filter.as_deref().is_none_or(|size| size == tag)
    }

    pub fn matches_layer(&self, layer: &str) -> bool {
        self.layer_filter
            .as_deref()
            .is_none_or(|layer_filter| layer_filter == layer)
    }

    pub fn matches_consumers(&self, consumers: usize) -> bool {
        self.consumers_filter
            .is_none_or(|consumer_filter| consumer_filter == consumers)
    }
}

const PAYLOAD_SWEEP_SPECS: [PayloadSweepSpec; 4] = [
    PayloadSweepSpec {
        tag: "1KB",
        payload_bytes: 1024,
        events: 200_000,
        buffer_depth: 16_384,
        batch_size: 2,
        raw_slot_bytes: 1024,
        codec_slot_bytes: 2048,
    },
    PayloadSweepSpec {
        tag: "4KB",
        payload_bytes: 4096,
        events: 100_000,
        buffer_depth: 16_384,
        batch_size: 8,
        raw_slot_bytes: 4096,
        codec_slot_bytes: 8192,
    },
    PayloadSweepSpec {
        tag: "16KB",
        payload_bytes: 16_384,
        events: 50_000,
        buffer_depth: 16_384,
        batch_size: 28,
        raw_slot_bytes: 16_384,
        codec_slot_bytes: 32_768,
    },
    PayloadSweepSpec {
        tag: "64KB",
        payload_bytes: 65_536,
        events: 20_000,
        buffer_depth: 16_384,
        batch_size: 110,
        raw_slot_bytes: 65_536,
        codec_slot_bytes: 131_072,
    },
];

pub fn payload_sweep_specs() -> &'static [PayloadSweepSpec] {
    &PAYLOAD_SWEEP_SPECS
}

pub fn default_co_target_rate(size_tag: &str) -> u64 {
    match size_tag {
        "1KB" => 20_000,
        "4KB" => 10_000,
        "16KB" => 5_000,
        "64KB" => 1_000,
        _ => 0,
    }
}

fn find_flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_selection_matches_filters() {
        let args = vec![
            "bench".to_string(),
            "--size".to_string(),
            "16KB".to_string(),
            "--layer".to_string(),
            "raw_ring".to_string(),
            "--consumers".to_string(),
            "4".to_string(),
        ];
        let selection = BasicSweepSelection::parse(&args).expect("parse sweep selection");
        assert!(selection.matches_size("16KB"));
        assert!(!selection.matches_size("64KB"));
        assert!(selection.matches_layer("raw_ring"));
        assert!(!selection.matches_layer("framed"));
        assert!(selection.matches_consumers(4));
        assert!(!selection.matches_consumers(2));
    }

    #[test]
    fn payload_sweep_specs_cover_expected_ladder() {
        let tags: Vec<&str> = payload_sweep_specs().iter().map(|spec| spec.tag).collect();
        assert_eq!(tags, vec!["1KB", "4KB", "16KB", "64KB"]);
        assert_eq!(default_co_target_rate("1KB"), 20_000);
        assert_eq!(default_co_target_rate("64KB"), 1_000);
    }
}
