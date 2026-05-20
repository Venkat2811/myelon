use super::common::SweepBackend;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FramedSweepLayer {
    Framed,
    FramedBatch,
    FramedRight,
}

impl FramedSweepLayer {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Framed => "framed",
            Self::FramedBatch => "framed_batch",
            Self::FramedRight => "framed_right",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FramedSweepRoleKey {
    Frame64,
    Frame64Batch,
    Frame2K,
    Frame8K,
    Frame32K,
    Frame128K,
}

#[derive(Debug, Clone, Copy)]
pub struct FramedSweepSizeSpec {
    pub layer: FramedSweepLayer,
    pub tag: &'static str,
    pub payload_bytes: usize,
    pub events: u64,
    pub base_buffer: usize,
    pub role_key: FramedSweepRoleKey,
}

const FRAMED_SWEEP_SPECS: [FramedSweepSizeSpec; 20] = [
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::Framed,
        tag: "1KB",
        payload_bytes: 1_024,
        events: 200_000,
        base_buffer: 16_384,
        role_key: FramedSweepRoleKey::Frame64,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::Framed,
        tag: "4KB",
        payload_bytes: 4_096,
        events: 100_000,
        base_buffer: 16_384,
        role_key: FramedSweepRoleKey::Frame64,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::Framed,
        tag: "16KB",
        payload_bytes: 16_384,
        events: 50_000,
        base_buffer: 16_384,
        role_key: FramedSweepRoleKey::Frame64,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::Framed,
        tag: "64KB",
        payload_bytes: 65_536,
        events: 20_000,
        base_buffer: 8_192,
        role_key: FramedSweepRoleKey::Frame64,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::Framed,
        tag: "128KB",
        payload_bytes: 128 * 1024,
        events: 10_000,
        base_buffer: 4_096,
        role_key: FramedSweepRoleKey::Frame64,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::Framed,
        tag: "256KB",
        payload_bytes: 256 * 1024,
        events: 5_000,
        base_buffer: 2_048,
        role_key: FramedSweepRoleKey::Frame64,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::Framed,
        tag: "512KB",
        payload_bytes: 512 * 1024,
        events: 2_000,
        base_buffer: 2_048,
        role_key: FramedSweepRoleKey::Frame64,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::Framed,
        tag: "1MB",
        payload_bytes: 1_024 * 1_024,
        events: 1_000,
        base_buffer: 1_024,
        role_key: FramedSweepRoleKey::Frame64,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedBatch,
        tag: "1KB",
        payload_bytes: 1_024,
        events: 200_000,
        base_buffer: 16_384,
        role_key: FramedSweepRoleKey::Frame64Batch,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedBatch,
        tag: "4KB",
        payload_bytes: 4_096,
        events: 100_000,
        base_buffer: 16_384,
        role_key: FramedSweepRoleKey::Frame64Batch,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedBatch,
        tag: "16KB",
        payload_bytes: 16_384,
        events: 50_000,
        base_buffer: 16_384,
        role_key: FramedSweepRoleKey::Frame64Batch,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedBatch,
        tag: "64KB",
        payload_bytes: 65_536,
        events: 20_000,
        base_buffer: 8_192,
        role_key: FramedSweepRoleKey::Frame64Batch,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedBatch,
        tag: "128KB",
        payload_bytes: 128 * 1024,
        events: 10_000,
        base_buffer: 4_096,
        role_key: FramedSweepRoleKey::Frame64Batch,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedBatch,
        tag: "256KB",
        payload_bytes: 256 * 1024,
        events: 5_000,
        base_buffer: 2_048,
        role_key: FramedSweepRoleKey::Frame64Batch,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedBatch,
        tag: "512KB",
        payload_bytes: 512 * 1024,
        events: 2_000,
        base_buffer: 2_048,
        role_key: FramedSweepRoleKey::Frame64Batch,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedBatch,
        tag: "1MB",
        payload_bytes: 1_024 * 1_024,
        events: 1_000,
        base_buffer: 1_024,
        role_key: FramedSweepRoleKey::Frame64Batch,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedRight,
        tag: "1KB",
        payload_bytes: 1_024,
        events: 200_000,
        base_buffer: 16_384,
        role_key: FramedSweepRoleKey::Frame2K,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedRight,
        tag: "4KB",
        payload_bytes: 4_096,
        events: 100_000,
        base_buffer: 16_384,
        role_key: FramedSweepRoleKey::Frame8K,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedRight,
        tag: "16KB",
        payload_bytes: 16_384,
        events: 50_000,
        base_buffer: 8_192,
        role_key: FramedSweepRoleKey::Frame32K,
    },
    FramedSweepSizeSpec {
        layer: FramedSweepLayer::FramedRight,
        tag: "64KB",
        payload_bytes: 65_536,
        events: 20_000,
        base_buffer: 4_096,
        role_key: FramedSweepRoleKey::Frame128K,
    },
];

pub fn framed_sweep_specs() -> &'static [FramedSweepSizeSpec] {
    &FRAMED_SWEEP_SPECS
}

pub fn framed_sweep_default_co_target_rate(size_tag: &str) -> u64 {
    match size_tag {
        "1KB" => 20_000,
        "4KB" => 10_000,
        "16KB" => 5_000,
        "64KB" => 1_000,
        "128KB" => 500,
        "256KB" => 250,
        "512KB" => 100,
        "1MB" => 50,
        _ => 0,
    }
}

pub fn framed_sweep_roles(
    backend: SweepBackend,
    role_key: FramedSweepRoleKey,
) -> (&'static str, &'static str) {
    match (backend, role_key) {
        (SweepBackend::Shm, FramedSweepRoleKey::Frame64) => {
            ("shm_frame64_prod", "shm_frame64_cons")
        }
        (SweepBackend::Shm, FramedSweepRoleKey::Frame64Batch) => {
            ("shm_frame64_prod", "shm_frame64_batch_cons")
        }
        (SweepBackend::Shm, FramedSweepRoleKey::Frame2K) => {
            ("shm_frame2k_prod", "shm_frame2k_cons")
        }
        (SweepBackend::Shm, FramedSweepRoleKey::Frame8K) => {
            ("shm_frame8k_prod", "shm_frame8k_cons")
        }
        (SweepBackend::Shm, FramedSweepRoleKey::Frame32K) => {
            ("shm_frame32k_prod", "shm_frame32k_cons")
        }
        (SweepBackend::Shm, FramedSweepRoleKey::Frame128K) => {
            ("shm_frame128k_prod", "shm_frame128k_cons")
        }
        (SweepBackend::Mmap, FramedSweepRoleKey::Frame64) => {
            ("mmap_frame64_prod", "mmap_frame64_cons")
        }
        (SweepBackend::Mmap, FramedSweepRoleKey::Frame64Batch) => {
            ("mmap_frame64_prod", "mmap_frame64_batch_cons")
        }
        (SweepBackend::Mmap, FramedSweepRoleKey::Frame2K) => {
            ("mmap_frame2k_prod", "mmap_frame2k_cons")
        }
        (SweepBackend::Mmap, FramedSweepRoleKey::Frame8K) => {
            ("mmap_frame8k_prod", "mmap_frame8k_cons")
        }
        (SweepBackend::Mmap, FramedSweepRoleKey::Frame32K) => {
            ("mmap_frame32k_prod", "mmap_frame32k_cons")
        }
        (SweepBackend::Mmap, FramedSweepRoleKey::Frame128K) => {
            ("mmap_frame128k_prod", "mmap_frame128k_cons")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framed_sweep_specs_cover_full_and_right_sized_ladders() {
        let specs = framed_sweep_specs();
        assert_eq!(specs.len(), 20);
        assert!(specs
            .iter()
            .any(|spec| spec.layer == FramedSweepLayer::Framed && spec.tag == "1MB"));
        assert!(specs
            .iter()
            .any(|spec| spec.layer == FramedSweepLayer::FramedBatch && spec.tag == "1MB"));
        assert!(specs
            .iter()
            .any(|spec| spec.layer == FramedSweepLayer::FramedRight && spec.tag == "64KB"));
        assert_eq!(framed_sweep_default_co_target_rate("128KB"), 500);
        assert_eq!(
            framed_sweep_roles(SweepBackend::Mmap, FramedSweepRoleKey::Frame32K),
            ("mmap_frame32k_prod", "mmap_frame32k_cons")
        );
        assert_eq!(
            framed_sweep_roles(SweepBackend::Shm, FramedSweepRoleKey::Frame64Batch),
            ("shm_frame64_prod", "shm_frame64_batch_cons")
        );
    }
}
