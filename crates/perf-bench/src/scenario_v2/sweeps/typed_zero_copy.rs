use super::common::SweepBackend;
use crate::codec_payloads::{encoded_len, make_payloads};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedZeroCopyCodec {
    Rkyv,
    Flatbuf,
}

impl TypedZeroCopyCodec {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Rkyv => "rkyv",
            Self::Flatbuf => "flatbuf",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TypedZeroCopyTargetSpec {
    pub tag: &'static str,
    pub target_payload_bytes: usize,
    pub events: u64,
    pub base_buffer: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct TypedZeroCopySweepSpec {
    pub codec: TypedZeroCopyCodec,
    pub tag: &'static str,
    pub payload_bytes: usize,
    pub events: u64,
    pub base_buffer: usize,
    pub batch_size: usize,
}

const TYPED_ZERO_COPY_TARGETS: [TypedZeroCopyTargetSpec; 8] = [
    TypedZeroCopyTargetSpec {
        tag: "1KB",
        target_payload_bytes: 1_024,
        events: 200_000,
        base_buffer: 16_384,
    },
    TypedZeroCopyTargetSpec {
        tag: "4KB",
        target_payload_bytes: 4_096,
        events: 100_000,
        base_buffer: 16_384,
    },
    TypedZeroCopyTargetSpec {
        tag: "16KB",
        target_payload_bytes: 16_384,
        events: 50_000,
        base_buffer: 16_384,
    },
    TypedZeroCopyTargetSpec {
        tag: "64KB",
        target_payload_bytes: 65_536,
        events: 20_000,
        base_buffer: 8_192,
    },
    TypedZeroCopyTargetSpec {
        tag: "128KB",
        target_payload_bytes: 128 * 1024,
        events: 10_000,
        base_buffer: 4_096,
    },
    TypedZeroCopyTargetSpec {
        tag: "256KB",
        target_payload_bytes: 256 * 1024,
        events: 5_000,
        base_buffer: 2_048,
    },
    TypedZeroCopyTargetSpec {
        tag: "512KB",
        target_payload_bytes: 512 * 1024,
        events: 2_000,
        base_buffer: 2_048,
    },
    TypedZeroCopyTargetSpec {
        tag: "1MB",
        target_payload_bytes: 1_024 * 1_024,
        events: 1_000,
        base_buffer: 1_024,
    },
];

pub fn typed_zero_copy_targets() -> &'static [TypedZeroCopyTargetSpec] {
    &TYPED_ZERO_COPY_TARGETS
}

pub fn typed_zero_copy_sweep_specs(codec: TypedZeroCopyCodec) -> Vec<TypedZeroCopySweepSpec> {
    typed_zero_copy_targets()
        .iter()
        .map(|target| {
            let (batch_size, payload_bytes) =
                calibrate_typed_zero_copy_batch(codec, target.target_payload_bytes);
            TypedZeroCopySweepSpec {
                codec,
                tag: target.tag,
                payload_bytes,
                events: target.events,
                base_buffer: target.base_buffer,
                batch_size,
            }
        })
        .collect()
}

pub fn typed_zero_copy_roles(backend: SweepBackend) -> (&'static str, &'static str) {
    match backend {
        SweepBackend::Shm => ("typed_zc_shm_prod", "typed_zc_shm_cons"),
        SweepBackend::Mmap => ("typed_zc_mmap_prod", "typed_zc_mmap_cons"),
    }
}

fn calibrate_typed_zero_copy_batch(
    codec: TypedZeroCopyCodec,
    target_payload_bytes: usize,
) -> (usize, usize) {
    let codec_slug = codec.slug();
    let mut hi = 1usize;
    let mut hi_payload_bytes = encoded_len(codec_slug, &make_payloads(hi));
    while hi_payload_bytes < target_payload_bytes {
        hi *= 2;
        hi_payload_bytes = encoded_len(codec_slug, &make_payloads(hi));
    }

    let mut lo = (hi / 2).max(1);
    let mut best = (hi, hi_payload_bytes);

    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let payload_bytes = encoded_len(codec_slug, &make_payloads(mid));
        if payload_bytes >= target_payload_bytes {
            best = (mid, payload_bytes);
            if mid == 0 {
                break;
            }
            hi = mid.saturating_sub(1);
        } else {
            lo = mid + 1;
        }
    }

    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_zero_copy_targets_cover_full_ladder() {
        let tags: Vec<&str> = typed_zero_copy_targets()
            .iter()
            .map(|spec| spec.tag)
            .collect();
        assert_eq!(
            tags,
            vec!["1KB", "4KB", "16KB", "64KB", "128KB", "256KB", "512KB", "1MB"]
        );
    }

    #[test]
    fn typed_zero_copy_specs_are_calibrated_and_monotonic() {
        for codec in [TypedZeroCopyCodec::Rkyv, TypedZeroCopyCodec::Flatbuf] {
            let specs = typed_zero_copy_sweep_specs(codec);
            assert_eq!(specs.len(), 8);
            assert_eq!(specs.last().expect("1MB spec").tag, "1MB");

            let mut prev_payload_bytes = 0usize;
            let mut prev_batch_size = 0usize;
            for spec in specs {
                let target = typed_zero_copy_targets()
                    .iter()
                    .find(|target| target.tag == spec.tag)
                    .expect("target for spec");
                assert!(spec.payload_bytes >= target.target_payload_bytes);
                assert!(spec.payload_bytes >= prev_payload_bytes);
                assert!(spec.batch_size >= prev_batch_size);
                prev_payload_bytes = spec.payload_bytes;
                prev_batch_size = spec.batch_size;
            }
        }
    }

    #[test]
    fn typed_zero_copy_roles_cover_both_backends() {
        assert_eq!(
            typed_zero_copy_roles(SweepBackend::Shm),
            ("typed_zc_shm_prod", "typed_zc_shm_cons")
        );
        assert_eq!(
            typed_zero_copy_roles(SweepBackend::Mmap),
            ("typed_zc_mmap_prod", "typed_zc_mmap_cons")
        );
    }
}
