use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::ReportOutputArgs;
use clap::Parser;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingPongMode {
    Throughput,
    CoAware,
}

impl PingPongMode {
    pub fn measurement_label(self, target_rate: u64) -> String {
        match self {
            Self::Throughput => "max_throughput".to_string(),
            Self::CoAware => format!("co_aware@{target_rate}"),
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::Throughput => "throughput",
            Self::CoAware => "co_aware",
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct FramedPingPongArgs {
    #[arg(long, hide = true)]
    pub bench: bool,

    #[arg(long, default_value = "all")]
    pub payload: String,

    #[arg(short = 'n', long)]
    pub num_messages: Option<u64>,

    #[arg(short = 'w', long)]
    pub warmup: Option<u64>,

    #[arg(short = 'b', long)]
    pub buffer_size: Option<usize>,

    #[arg(long, default_value = "busyspin")]
    pub wait_strategy: String,

    #[arg(long, default_value = "throughput")]
    pub mode: String,

    #[arg(long)]
    pub target_rate: Option<u64>,

    #[arg(long)]
    pub json: bool,

    #[arg(long)]
    pub tree: bool,

    #[arg(long)]
    pub json_out: Option<String>,

    #[arg(long)]
    pub csv_out: Option<String>,

    #[arg(long = "md-out")]
    pub markdown_out: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FramedPingPongScenarioSpec {
    pub backend: BackendKind,
    pub payload_label: &'static str,
    pub payload_tag: &'static str,
    pub payload_bytes: usize,
    pub messages: u64,
    pub warmup: u64,
    pub buffer_depth: usize,
    pub wait_strategy: String,
    pub target_rate: u64,
    pub producer_role: &'static str,
    pub consumer_role: &'static str,
}

#[derive(Debug, Clone)]
pub struct FramedPingPongSelection {
    pub mode: PingPongMode,
    payload_filter: Option<String>,
    pub output_args: ReportOutputArgs,
    pub wait_strategy: String,
    pub num_messages_override: Option<u64>,
    pub warmup_override: Option<u64>,
    pub buffer_override: Option<usize>,
    pub target_rate: u64,
}

impl FramedPingPongSelection {
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let parsed = FramedPingPongArgs::parse_from(args);
        let mode = parse_mode(&parsed.mode, parsed.target_rate)?;
        validate_wait_strategy(&parsed.wait_strategy)?;
        Ok(Self {
            mode,
            payload_filter: normalized_filter(&parsed.payload),
            output_args: ReportOutputArgs {
                json_mode: parsed.json,
                quick_mode: false,
                tree_mode: parsed.tree,
                json_out: parsed.json_out,
                csv_out: parsed.csv_out,
                markdown_out: parsed.markdown_out,
            },
            wait_strategy: parsed.wait_strategy,
            num_messages_override: parsed.num_messages,
            warmup_override: parsed.warmup,
            buffer_override: parsed.buffer_size,
            target_rate: parsed.target_rate.unwrap_or(0),
        })
    }

    pub fn scenario_specs(&self, backend: BackendKind) -> Vec<FramedPingPongScenarioSpec> {
        framed_specs(backend)
            .into_iter()
            .filter(|spec| self.matches_payload(spec))
            .map(|spec| FramedPingPongScenarioSpec {
                backend: spec.backend,
                payload_label: spec.payload_label,
                payload_tag: spec.payload_tag,
                payload_bytes: spec.payload_bytes,
                messages: self.num_messages_override.unwrap_or(spec.messages),
                warmup: self.warmup_override.unwrap_or(spec.warmup),
                buffer_depth: self.buffer_override.unwrap_or(spec.buffer_depth),
                wait_strategy: self.wait_strategy.clone(),
                target_rate: self.target_rate,
                producer_role: spec.producer_role,
                consumer_role: spec.consumer_role,
            })
            .collect()
    }

    fn matches_payload(&self, spec: &FramedPingPongScenarioSpec) -> bool {
        self.payload_filter.as_deref().is_none_or(|payload| {
            payload.eq_ignore_ascii_case(spec.payload_tag)
                || payload.eq_ignore_ascii_case(spec.payload_label)
        })
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct CodecPingPongArgs {
    #[arg(long, hide = true)]
    pub bench: bool,

    #[arg(long, default_value = "all")]
    pub codec: String,

    #[arg(long)]
    pub batch: Option<usize>,

    #[arg(short = 'n', long)]
    pub num_messages: Option<u64>,

    #[arg(short = 'w', long)]
    pub warmup: Option<u64>,

    #[arg(short = 'b', long)]
    pub buffer_size: Option<usize>,

    #[arg(long, default_value = "busyspin")]
    pub wait_strategy: String,

    #[arg(long, default_value = "throughput")]
    pub mode: String,

    #[arg(long)]
    pub target_rate: Option<u64>,

    #[arg(long)]
    pub json: bool,

    #[arg(long)]
    pub tree: bool,

    #[arg(long)]
    pub json_out: Option<String>,

    #[arg(long)]
    pub csv_out: Option<String>,

    #[arg(long = "md-out")]
    pub markdown_out: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CodecPingPongScenarioSpec {
    pub backend: BackendKind,
    pub codec: &'static str,
    pub batch_size: usize,
    pub messages: u64,
    pub warmup: u64,
    pub buffer_depth: usize,
    pub wait_strategy: String,
    pub target_rate: u64,
    pub producer_role: &'static str,
    pub consumer_role: &'static str,
}

#[derive(Debug, Clone)]
pub struct CodecPingPongSelection {
    pub mode: PingPongMode,
    codec_filter: Option<String>,
    batch_filter: Option<usize>,
    pub output_args: ReportOutputArgs,
    pub wait_strategy: String,
    pub num_messages_override: Option<u64>,
    pub warmup_override: Option<u64>,
    pub buffer_override: Option<usize>,
    pub target_rate: u64,
}

impl CodecPingPongSelection {
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let parsed = CodecPingPongArgs::parse_from(args);
        let mode = parse_mode(&parsed.mode, parsed.target_rate)?;
        validate_wait_strategy(&parsed.wait_strategy)?;
        Ok(Self {
            mode,
            codec_filter: normalized_filter(&parsed.codec),
            batch_filter: parsed.batch,
            output_args: ReportOutputArgs {
                json_mode: parsed.json,
                quick_mode: false,
                tree_mode: parsed.tree,
                json_out: parsed.json_out,
                csv_out: parsed.csv_out,
                markdown_out: parsed.markdown_out,
            },
            wait_strategy: parsed.wait_strategy,
            num_messages_override: parsed.num_messages,
            warmup_override: parsed.warmup,
            buffer_override: parsed.buffer_size,
            target_rate: parsed.target_rate.unwrap_or(0),
        })
    }

    pub fn scenario_specs(&self, backend: BackendKind) -> Vec<CodecPingPongScenarioSpec> {
        codec_specs(backend)
            .into_iter()
            .filter(|spec| self.matches(spec))
            .map(|spec| CodecPingPongScenarioSpec {
                backend: spec.backend,
                codec: spec.codec,
                batch_size: spec.batch_size,
                messages: self.num_messages_override.unwrap_or(spec.messages),
                warmup: self.warmup_override.unwrap_or(spec.warmup),
                buffer_depth: self.buffer_override.unwrap_or(spec.buffer_depth),
                wait_strategy: self.wait_strategy.clone(),
                target_rate: self.target_rate,
                producer_role: spec.producer_role,
                consumer_role: spec.consumer_role,
            })
            .collect()
    }

    pub fn scenario_specs_zero_copy(&self, backend: BackendKind) -> Vec<CodecPingPongScenarioSpec> {
        codec_specs_zero_copy(backend)
            .into_iter()
            .filter(|spec| self.matches(spec))
            .map(|spec| CodecPingPongScenarioSpec {
                backend: spec.backend,
                codec: spec.codec,
                batch_size: spec.batch_size,
                messages: self.num_messages_override.unwrap_or(spec.messages),
                warmup: self.warmup_override.unwrap_or(spec.warmup),
                buffer_depth: self.buffer_override.unwrap_or(spec.buffer_depth),
                wait_strategy: self.wait_strategy.clone(),
                target_rate: self.target_rate,
                producer_role: spec.producer_role,
                consumer_role: spec.consumer_role,
            })
            .collect()
    }

    pub fn validate_zero_copy_codec_filter(&self) -> Result<(), String> {
        if self
            .codec_filter
            .as_deref()
            .is_some_and(|codec| !codec_supports_zero_copy(codec))
        {
            return Err("zero-copy ping-pong only supports codecs: rkyv, flatbuf".into());
        }
        Ok(())
    }

    fn matches(&self, spec: &CodecPingPongScenarioSpec) -> bool {
        if self
            .codec_filter
            .as_deref()
            .is_some_and(|codec| !codec.eq_ignore_ascii_case(spec.codec))
        {
            return false;
        }
        if self
            .batch_filter
            .is_some_and(|batch_size| batch_size != spec.batch_size)
        {
            return false;
        }
        true
    }
}

fn normalized_filter(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("all") {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_mode(mode: &str, target_rate: Option<u64>) -> Result<PingPongMode, String> {
    match mode {
        "throughput" => {
            if target_rate.is_some() {
                return Err("--target-rate requires --mode co".into());
            }
            Ok(PingPongMode::Throughput)
        }
        "co" => target_rate
            .map(|_| PingPongMode::CoAware)
            .ok_or_else(|| "--mode co requires --target-rate".to_string()),
        other => Err(format!("unsupported --mode value: {other}")),
    }
}

fn validate_wait_strategy(wait_strategy: &str) -> Result<(), String> {
    match wait_strategy.to_ascii_lowercase().as_str() {
        "busyspin" | "block" => Ok(()),
        other => Err(format!("unsupported --wait-strategy value: {other}")),
    }
}

fn default_buffer_depth(payload_bytes: usize) -> usize {
    match payload_bytes {
        0..=1024 => 4096,
        1025..=16384 => 2048,
        16385..=65536 => 1024,
        _ => 512,
    }
}

fn framed_specs(backend: BackendKind) -> Vec<FramedPingPongScenarioSpec> {
    let (producer_role, consumer_role) = match backend {
        BackendKind::Shm => ("pingpong_framed_shm_echo", "pingpong_framed_shm_initiator"),
        BackendKind::Mmap => (
            "pingpong_framed_mmap_echo",
            "pingpong_framed_mmap_initiator",
        ),
        _ => unreachable!("framed ping-pong only supports shm and mmap"),
    };

    [
        ("64B", "64B", 64usize, 200_000u64, 20_000u64),
        ("512B", "512B", 512usize, 200_000u64, 20_000u64),
        ("1KB", "1KB", 1_024usize, 150_000u64, 15_000u64),
        ("4KB", "4KB", 4 * 1024usize, 100_000u64, 10_000u64),
        ("32KB", "32KB", 32 * 1024usize, 50_000u64, 5_000u64),
        ("64KB", "64KB", 64 * 1024usize, 30_000u64, 3_000u64),
        ("128KB", "128KB", 128 * 1024usize, 15_000u64, 1_500u64),
    ]
    .into_iter()
    .map(
        |(payload_label, payload_tag, payload_bytes, messages, warmup)| {
            FramedPingPongScenarioSpec {
                backend: backend.clone(),
                payload_label,
                payload_tag,
                payload_bytes,
                messages,
                warmup,
                buffer_depth: default_buffer_depth(payload_bytes),
                wait_strategy: "busyspin".to_string(),
                target_rate: 0,
                producer_role,
                consumer_role,
            }
        },
    )
    .collect()
}

fn codec_specs(backend: BackendKind) -> Vec<CodecPingPongScenarioSpec> {
    let (producer_role, consumer_role) = match backend {
        BackendKind::Shm => ("pingpong_codec_shm_echo", "pingpong_codec_shm_initiator"),
        BackendKind::Mmap => ("pingpong_codec_mmap_echo", "pingpong_codec_mmap_initiator"),
        _ => unreachable!("codec ping-pong only supports shm and mmap"),
    };

    [
        (1usize, 150_000u64, 15_000u64),
        (8, 75_000, 7_500),
        (64, 20_000, 2_000),
        (256, 5_000, 500),
    ]
    .into_iter()
    .flat_map(|(batch_size, messages, warmup)| {
        let backend = backend.clone();
        ["bincode", "rkyv", "flatbuf"]
            .into_iter()
            .map(move |codec| {
                let approx_payload_bytes = batch_size.saturating_mul(640).max(1);
                CodecPingPongScenarioSpec {
                    backend: backend.clone(),
                    codec,
                    batch_size,
                    messages,
                    warmup,
                    buffer_depth: default_buffer_depth(approx_payload_bytes),
                    wait_strategy: "busyspin".to_string(),
                    target_rate: 0,
                    producer_role,
                    consumer_role,
                }
            })
    })
    .collect()
}

fn codec_specs_zero_copy(backend: BackendKind) -> Vec<CodecPingPongScenarioSpec> {
    let (producer_role, consumer_role) = match backend {
        BackendKind::Shm => (
            "pingpong_typed_zero_copy_shm_echo",
            "pingpong_typed_zero_copy_shm_initiator",
        ),
        BackendKind::Mmap => (
            "pingpong_typed_zero_copy_mmap_echo",
            "pingpong_typed_zero_copy_mmap_initiator",
        ),
        _ => unreachable!("zero-copy codec ping-pong only supports shm and mmap"),
    };

    codec_specs(backend)
        .into_iter()
        .filter(|spec| codec_supports_zero_copy(spec.codec))
        .map(|mut spec| {
            spec.producer_role = producer_role;
            spec.consumer_role = consumer_role;
            spec
        })
        .collect()
}

fn codec_supports_zero_copy(codec: &str) -> bool {
    matches!(codec, "rkyv" | "flatbuf")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zero_copy_codecs(specs: &[CodecPingPongScenarioSpec]) -> Vec<&'static str> {
        specs.iter().map(|spec| spec.codec).collect()
    }

    #[test]
    fn zero_copy_codec_specs_only_include_zero_copy_capable_codecs() {
        let args = vec!["bench".to_string()];
        let selection = CodecPingPongSelection::parse(&args).expect("parse");

        let shm = selection.scenario_specs_zero_copy(BackendKind::Shm);
        let mmap = selection.scenario_specs_zero_copy(BackendKind::Mmap);

        assert!(!shm.is_empty());
        assert!(!mmap.is_empty());
        assert!(zero_copy_codecs(&shm)
            .iter()
            .all(|codec| { matches!(*codec, "rkyv" | "flatbuf") }));
        assert!(zero_copy_codecs(&mmap)
            .iter()
            .all(|codec| { matches!(*codec, "rkyv" | "flatbuf") }));
    }

    #[test]
    fn zero_copy_codec_selection_rejects_bincode_filter() {
        let args = vec![
            "bench".to_string(),
            "--codec".to_string(),
            "bincode".to_string(),
        ];
        let selection = CodecPingPongSelection::parse(&args).expect("parse");

        assert_eq!(
            selection.validate_zero_copy_codec_filter(),
            Err("zero-copy ping-pong only supports codecs: rkyv, flatbuf".into())
        );
        assert!(selection
            .scenario_specs_zero_copy(BackendKind::Shm)
            .is_empty());
    }

    #[test]
    fn zero_copy_codec_selection_preserves_batch_filter() {
        let args = vec![
            "bench".to_string(),
            "--codec".to_string(),
            "rkyv".to_string(),
            "--batch".to_string(),
            "8".to_string(),
        ];
        let selection = CodecPingPongSelection::parse(&args).expect("parse");
        let specs = selection.scenario_specs_zero_copy(BackendKind::Shm);

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].codec, "rkyv");
        assert_eq!(specs[0].batch_size, 8);
    }
}
