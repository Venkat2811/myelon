use crate::report_v2::BackendKind;
use crate::reporting::ReportOutputArgs;

#[derive(Debug, Clone)]
pub struct FramedScenarioSpec {
    pub backend: BackendKind,
    pub payload_label: &'static str,
    pub payload_tag: &'static str,
    pub payload_bytes: usize,
    pub messages: u64,
    pub base_buffer: usize,
    pub consumers: usize,
    pub producer_role: &'static str,
    pub consumer_role: &'static str,
}

impl FramedScenarioSpec {
    pub fn selector(&self) -> String {
        format!("{}_{}c", self.payload_tag, self.consumers)
    }

    pub fn buffer_depth(&self) -> usize {
        scaled_buffer(self.base_buffer, self.consumers)
    }
}

#[derive(Debug, Clone)]
pub struct FramedSelection {
    payload_filter: Option<String>,
    pub output_args: ReportOutputArgs,
}

impl FramedSelection {
    pub fn parse(args: &[String]) -> Self {
        Self {
            payload_filter: find_arg_value(args, "--payload").filter(|value| value != "all"),
            output_args: ReportOutputArgs::from_args(args),
        }
    }

    pub fn scenario_specs(&self, backend: BackendKind) -> Vec<FramedScenarioSpec> {
        base_specs(backend)
            .into_iter()
            .filter(|spec| self.should_run(spec))
            .collect()
    }

    fn should_run(&self, spec: &FramedScenarioSpec) -> bool {
        self.payload_filter
            .as_deref()
            .is_none_or(|payload| payload == spec.payload_tag || payload == spec.selector())
    }
}

fn base_specs(backend: BackendKind) -> Vec<FramedScenarioSpec> {
    let (producer_role, consumer_role) = match backend {
        BackendKind::Shm => ("framed_producer", "framed_consumer"),
        BackendKind::Mmap => ("framed_mmap_producer", "framed_mmap_consumer"),
        _ => ("framed_producer", "framed_consumer"),
    };

    let mut scenarios = Vec::new();
    for (payload_label, payload_tag, payload_bytes, messages, base_buffer) in [
        ("1KB", "1K", 1_024usize, 100_000u64, 1024usize),
        ("32KB", "32K", 32 * 1024, 50_000, 1024),
        ("64KB", "64K", 65_524, 50_000, 1024),
        ("128KB-frag", "128K", 128 * 1024, 10_000, 2048),
    ] {
        for consumers in [1usize, 2, 4, 6, 8, 12] {
            scenarios.push(FramedScenarioSpec {
                backend: backend.clone(),
                payload_label,
                payload_tag,
                payload_bytes,
                messages,
                base_buffer,
                consumers,
                producer_role,
                consumer_role,
            });
        }
    }

    scenarios.push(FramedScenarioSpec {
        backend,
        payload_label: "32KB",
        payload_tag: "32K",
        payload_bytes: 32 * 1024,
        messages: 50_000,
        base_buffer: 1024,
        consumers: 3,
        producer_role,
        consumer_role,
    });

    scenarios
}

fn scaled_buffer(base_buffer: usize, consumers: usize) -> usize {
    base_buffer
        .max(consumers.next_power_of_two() * 256)
        .next_power_of_two()
}

fn find_arg_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_matches_payload_tag_and_selector() {
        let spec = FramedScenarioSpec {
            backend: BackendKind::Shm,
            payload_label: "64KB",
            payload_tag: "64K",
            payload_bytes: 65_524,
            messages: 50_000,
            base_buffer: 1024,
            consumers: 2,
            producer_role: "framed_producer",
            consumer_role: "framed_consumer",
        };

        let tag = FramedSelection::parse(&[
            "bench".to_string(),
            "--payload".to_string(),
            "64K".to_string(),
        ]);
        assert!(tag.should_run(&spec));

        let selector = FramedSelection::parse(&[
            "bench".to_string(),
            "--payload".to_string(),
            "64K_2c".to_string(),
        ]);
        assert!(selector.should_run(&spec));

        let other = FramedSelection::parse(&[
            "bench".to_string(),
            "--payload".to_string(),
            "1K".to_string(),
        ]);
        assert!(!other.should_run(&spec));
    }

    #[test]
    fn scenarios_carry_backend_specific_roles() {
        let scenarios =
            FramedSelection::parse(&["bench".to_string()]).scenario_specs(BackendKind::Mmap);
        let spec = scenarios
            .into_iter()
            .find(|entry| entry.payload_tag == "32K" && entry.consumers == 3)
            .expect("legacy 32K_3c anchor");
        assert_eq!(spec.producer_role, "framed_mmap_producer");
        assert_eq!(spec.consumer_role, "framed_mmap_consumer");
    }
}
