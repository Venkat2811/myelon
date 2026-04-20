use crate::report_v2::BackendKind;

#[derive(Debug, Clone)]
pub struct LayoutTargetSpec {
    pub scenario: &'static str,
    pub backend: BackendKind,
    pub layer: &'static str,
    pub budget_ns: u64,
}

const fn target(
    scenario: &'static str,
    backend: BackendKind,
    layer: &'static str,
    budget_ns: u64,
) -> LayoutTargetSpec {
    LayoutTargetSpec {
        scenario,
        backend,
        layer,
        budget_ns,
    }
}

pub static SHM_RING_PRODUCER_CREATE: LayoutTargetSpec = target(
    "shm-ring-producer-create",
    BackendKind::Shm,
    "raw_ring",
    2_000_000_000,
);
pub static SHM_RING_ATTACH: LayoutTargetSpec = target(
    "shm-ring-attach",
    BackendKind::Shm,
    "raw_ring",
    500_000,
);
pub static SHM_CURSOR_ATTACH: LayoutTargetSpec =
    target("shm-cursor-attach", BackendKind::Shm, "cursor", 500_000);
pub static MMAP_RING_PRODUCER_CREATE: LayoutTargetSpec = target(
    "mmap-ring-producer-create",
    BackendKind::Mmap,
    "raw_ring",
    5_000_000,
);
pub static MMAP_RING_ATTACH: LayoutTargetSpec =
    target("mmap-ring-attach", BackendKind::Mmap, "raw_ring", 500_000);
pub static MMAP_CURSOR_ATTACH: LayoutTargetSpec =
    target("mmap-cursor-attach", BackendKind::Mmap, "cursor", 500_000);
pub static FRAMED_SHM_PRODUCER_CREATE: LayoutTargetSpec = target(
    "framed-shm-producer-create",
    BackendKind::Shm,
    "framed",
    2_000_000_000,
);
pub static FRAMED_SHM_CONSUMER_ATTACH: LayoutTargetSpec = target(
    "framed-shm-consumer-attach",
    BackendKind::Shm,
    "framed",
    500_000,
);
pub static FRAMED_MMAP_PRODUCER_CREATE: LayoutTargetSpec = target(
    "framed-mmap-producer-create",
    BackendKind::Mmap,
    "framed",
    10_000_000,
);
pub static FRAMED_MMAP_CONSUMER_ATTACH: LayoutTargetSpec = target(
    "framed-mmap-consumer-attach",
    BackendKind::Mmap,
    "framed",
    500_000,
);
pub static TYPED_SHM_PRODUCER_CREATE: LayoutTargetSpec = target(
    "typed-shm-producer-create",
    BackendKind::Shm,
    "typed",
    2_000_000_000,
);
pub static TYPED_SHM_CONSUMER_ATTACH: LayoutTargetSpec = target(
    "typed-shm-consumer-attach",
    BackendKind::Shm,
    "typed",
    500_000,
);
pub static TYPED_MMAP_PRODUCER_CREATE: LayoutTargetSpec = target(
    "typed-mmap-producer-create",
    BackendKind::Mmap,
    "typed",
    10_000_000,
);
pub static TYPED_MMAP_CONSUMER_ATTACH: LayoutTargetSpec = target(
    "typed-mmap-consumer-attach",
    BackendKind::Mmap,
    "typed",
    500_000,
);

pub static ALL_TARGETS: [&LayoutTargetSpec; 14] = [
    &SHM_RING_PRODUCER_CREATE,
    &SHM_RING_ATTACH,
    &SHM_CURSOR_ATTACH,
    &MMAP_RING_PRODUCER_CREATE,
    &MMAP_RING_ATTACH,
    &MMAP_CURSOR_ATTACH,
    &FRAMED_SHM_PRODUCER_CREATE,
    &FRAMED_SHM_CONSUMER_ATTACH,
    &FRAMED_MMAP_PRODUCER_CREATE,
    &FRAMED_MMAP_CONSUMER_ATTACH,
    &TYPED_SHM_PRODUCER_CREATE,
    &TYPED_SHM_CONSUMER_ATTACH,
    &TYPED_MMAP_PRODUCER_CREATE,
    &TYPED_MMAP_CONSUMER_ATTACH,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_targets_cover_all_expected_pairs() {
        assert_eq!(ALL_TARGETS.len(), 14);
        assert!(ALL_TARGETS
            .iter()
            .any(|target| target.scenario == "shm-ring-producer-create"
                && target.layer == "raw_ring"));
        assert!(ALL_TARGETS.iter().any(
            |target| target.scenario == "typed-mmap-consumer-attach" && target.layer == "typed"
        ));
    }
}
