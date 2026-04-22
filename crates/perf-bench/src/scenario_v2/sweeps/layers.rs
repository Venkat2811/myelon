use super::common::SweepBackend;

#[derive(Debug, Clone, Copy)]
pub struct NofragVariantSpec {
    pub layer: &'static str,
    pub backend: SweepBackend,
    pub prod_role: &'static str,
    pub cons_role: &'static str,
    pub summary_label: &'static str,
    pub uses_codec_slot: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum MyelonLayerVariantKind {
    Raw,
    RawMyelon,
    Framed,
    LayerScenario {
        codec_env: Option<&'static str>,
        include_payload_size: bool,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct MyelonLayerVariantSpec {
    pub layer: &'static str,
    pub codec: Option<&'static str>,
    pub summary_label: &'static str,
    pub segment_prefix: &'static str,
    pub prod_role: &'static str,
    pub cons_role: &'static str,
    pub buffer_override: Option<usize>,
    pub kind: MyelonLayerVariantKind,
}

pub fn myelon_raw_roles(size_tag: &str) -> (&'static str, &'static str) {
    match size_tag {
        "1KB" => ("raw_prod_1k", "raw_cons_1k"),
        "4KB" => ("raw_prod_4k", "raw_cons_4k"),
        "16KB" => ("raw_prod_16k", "raw_cons_16k"),
        "64KB" => ("raw_prod_64k", "raw_cons_64k"),
        _ => ("raw_prod_1k", "raw_cons_1k"),
    }
}

pub fn myelon_curated_raw_roles(size_tag: &str) -> (&'static str, &'static str) {
    match size_tag {
        "1KB" => ("my_raw_prod_1k", "my_raw_cons_1k"),
        "4KB" => ("my_raw_prod_4k", "my_raw_cons_4k"),
        "16KB" => ("my_raw_prod_16k", "my_raw_cons_16k"),
        "64KB" => ("my_raw_prod_64k", "my_raw_cons_64k"),
        _ => ("my_raw_prod_1k", "my_raw_cons_1k"),
    }
}

pub fn myelon_right_sized_framed_roles(
    size_tag: &str,
) -> Option<(&'static str, &'static str, usize)> {
    match size_tag {
        "1KB" => Some(("rs_framed_prod_2k", "rs_framed_cons_2k", 65_536usize)),
        "4KB" => Some(("rs_framed_prod_8k", "rs_framed_cons_8k", 32_768)),
        "16KB" => Some(("rs_framed_prod_32k", "rs_framed_cons_32k", 16_384)),
        "64KB" => None,
        _ => None,
    }
}

pub fn myelon_rkyv_nofrag_roles(size_tag: &str) -> (&'static str, &'static str) {
    match size_tag {
        "1KB" => ("rkyv_nf_prod_2k", "rkyv_nf_cons_2k"),
        "4KB" => ("rkyv_nf_prod_8k", "rkyv_nf_cons_8k"),
        "16KB" => ("rkyv_nf_prod_32k", "rkyv_nf_cons_32k"),
        "64KB" => ("rkyv_nf_prod_128k", "rkyv_nf_cons_128k"),
        _ => ("rkyv_nf_prod_8k", "rkyv_nf_cons_8k"),
    }
}

pub fn nofrag_raw_roles(backend: SweepBackend, size_tag: &str) -> (&'static str, &'static str) {
    match (backend, size_tag) {
        (SweepBackend::Shm, "1KB") => ("raw_shm_prod_1k", "raw_shm_cons_1k"),
        (SweepBackend::Shm, "4KB") => ("raw_shm_prod_4k", "raw_shm_cons_4k"),
        (SweepBackend::Shm, "16KB") => ("raw_shm_prod_16k", "raw_shm_cons_16k"),
        (SweepBackend::Shm, "64KB") => ("raw_shm_prod_64k", "raw_shm_cons_64k"),
        (SweepBackend::Mmap, "1KB") => ("raw_mmap_prod_1k", "raw_mmap_cons_1k"),
        (SweepBackend::Mmap, "4KB") => ("raw_mmap_prod_4k", "raw_mmap_cons_4k"),
        (SweepBackend::Mmap, "16KB") => ("raw_mmap_prod_16k", "raw_mmap_cons_16k"),
        (SweepBackend::Mmap, "64KB") => ("raw_mmap_prod_64k", "raw_mmap_cons_64k"),
        (SweepBackend::Shm, _) => ("raw_shm_prod_1k", "raw_shm_cons_1k"),
        (SweepBackend::Mmap, _) => ("raw_mmap_prod_1k", "raw_mmap_cons_1k"),
    }
}

pub fn nofrag_rkyv_roles(backend: SweepBackend, size_tag: &str) -> (&'static str, &'static str) {
    match (backend, size_tag) {
        (SweepBackend::Shm, "1KB") => ("rkyv_shm_prod_2k", "rkyv_shm_cons_2k"),
        (SweepBackend::Shm, "4KB") => ("rkyv_shm_prod_8k", "rkyv_shm_cons_8k"),
        (SweepBackend::Shm, "16KB") => ("rkyv_shm_prod_32k", "rkyv_shm_cons_32k"),
        (SweepBackend::Shm, "64KB") => ("rkyv_shm_prod_128k", "rkyv_shm_cons_128k"),
        (SweepBackend::Mmap, "1KB") => ("rkyv_mmap_prod_2k", "rkyv_mmap_cons_2k"),
        (SweepBackend::Mmap, "4KB") => ("rkyv_mmap_prod_8k", "rkyv_mmap_cons_8k"),
        (SweepBackend::Mmap, "16KB") => ("rkyv_mmap_prod_32k", "rkyv_mmap_cons_32k"),
        (SweepBackend::Mmap, "64KB") => ("rkyv_mmap_prod_128k", "rkyv_mmap_cons_128k"),
        (SweepBackend::Shm, _) => ("rkyv_shm_prod_2k", "rkyv_shm_cons_2k"),
        (SweepBackend::Mmap, _) => ("rkyv_mmap_prod_2k", "rkyv_mmap_cons_2k"),
    }
}

pub fn nofrag_flatbuf_roles(backend: SweepBackend, size_tag: &str) -> (&'static str, &'static str) {
    match (backend, size_tag) {
        (SweepBackend::Shm, "1KB") => ("fb_shm_prod_2k", "fb_shm_cons_2k"),
        (SweepBackend::Shm, "4KB") => ("fb_shm_prod_8k", "fb_shm_cons_8k"),
        (SweepBackend::Shm, "16KB") => ("fb_shm_prod_32k", "fb_shm_cons_32k"),
        (SweepBackend::Shm, "64KB") => ("fb_shm_prod_128k", "fb_shm_cons_128k"),
        (SweepBackend::Mmap, "1KB") => ("fb_mmap_prod_2k", "fb_mmap_cons_2k"),
        (SweepBackend::Mmap, "4KB") => ("fb_mmap_prod_8k", "fb_mmap_cons_8k"),
        (SweepBackend::Mmap, "16KB") => ("fb_mmap_prod_32k", "fb_mmap_cons_32k"),
        (SweepBackend::Mmap, "64KB") => ("fb_mmap_prod_128k", "fb_mmap_cons_128k"),
        (SweepBackend::Shm, _) => ("fb_shm_prod_2k", "fb_shm_cons_2k"),
        (SweepBackend::Mmap, _) => ("fb_mmap_prod_2k", "fb_mmap_cons_2k"),
    }
}

pub fn nofrag_variant_specs(size_tag: &str) -> [NofragVariantSpec; 6] {
    let (raw_shm_prod, raw_shm_cons) = nofrag_raw_roles(SweepBackend::Shm, size_tag);
    let (raw_mmap_prod, raw_mmap_cons) = nofrag_raw_roles(SweepBackend::Mmap, size_tag);
    let (rkyv_shm_prod, rkyv_shm_cons) = nofrag_rkyv_roles(SweepBackend::Shm, size_tag);
    let (flatbuf_shm_prod, flatbuf_shm_cons) = nofrag_flatbuf_roles(SweepBackend::Shm, size_tag);
    let (rkyv_mmap_prod, rkyv_mmap_cons) = nofrag_rkyv_roles(SweepBackend::Mmap, size_tag);
    let (flatbuf_mmap_prod, flatbuf_mmap_cons) = nofrag_flatbuf_roles(SweepBackend::Mmap, size_tag);

    [
        NofragVariantSpec {
            layer: "raw_ring",
            backend: SweepBackend::Shm,
            prod_role: raw_shm_prod,
            cons_role: raw_shm_cons,
            summary_label: "raw_ring",
            uses_codec_slot: false,
        },
        NofragVariantSpec {
            layer: "raw_ring",
            backend: SweepBackend::Mmap,
            prod_role: raw_mmap_prod,
            cons_role: raw_mmap_cons,
            summary_label: "raw_ring",
            uses_codec_slot: false,
        },
        NofragVariantSpec {
            layer: "rkyv_nofrag",
            backend: SweepBackend::Shm,
            prod_role: rkyv_shm_prod,
            cons_role: rkyv_shm_cons,
            summary_label: "rkyv_nf",
            uses_codec_slot: true,
        },
        NofragVariantSpec {
            layer: "flatbuf_nf",
            backend: SweepBackend::Shm,
            prod_role: flatbuf_shm_prod,
            cons_role: flatbuf_shm_cons,
            summary_label: "flatbuf_nf",
            uses_codec_slot: true,
        },
        NofragVariantSpec {
            layer: "rkyv_nofrag",
            backend: SweepBackend::Mmap,
            prod_role: rkyv_mmap_prod,
            cons_role: rkyv_mmap_cons,
            summary_label: "rkyv_nf",
            uses_codec_slot: true,
        },
        NofragVariantSpec {
            layer: "flatbuf_nf",
            backend: SweepBackend::Mmap,
            prod_role: flatbuf_mmap_prod,
            cons_role: flatbuf_mmap_cons,
            summary_label: "flatbuf_nf",
            uses_codec_slot: true,
        },
    ]
}

pub fn myelon_layer_variant_specs(size_tag: &str) -> Vec<MyelonLayerVariantSpec> {
    let raw_roles = myelon_raw_roles(size_tag);
    let raw_myelon_roles = myelon_curated_raw_roles(size_tag);
    let rkyv_roles = myelon_rkyv_nofrag_roles(size_tag);
    let mut specs = vec![
        MyelonLayerVariantSpec {
            layer: "raw_ring",
            codec: None,
            summary_label: "raw_ring:",
            segment_prefix: "ml_raw",
            prod_role: raw_roles.0,
            cons_role: raw_roles.1,
            buffer_override: None,
            kind: MyelonLayerVariantKind::Raw,
        },
        MyelonLayerVariantSpec {
            layer: "raw_myelon",
            codec: None,
            summary_label: "raw_myelon:",
            segment_prefix: "ml_mraw",
            prod_role: raw_myelon_roles.0,
            cons_role: raw_myelon_roles.1,
            buffer_override: None,
            kind: MyelonLayerVariantKind::RawMyelon,
        },
        MyelonLayerVariantSpec {
            layer: "framed",
            codec: None,
            summary_label: "framed:",
            segment_prefix: "ml_frm",
            prod_role: "framed_prod",
            cons_role: "framed_cons",
            buffer_override: None,
            kind: MyelonLayerVariantKind::Framed,
        },
        MyelonLayerVariantSpec {
            layer: "framed_batch",
            codec: None,
            summary_label: "framed_batch:",
            segment_prefix: "ml_frmb",
            prod_role: "framed_prod",
            cons_role: "framed_batch_cons",
            buffer_override: None,
            kind: MyelonLayerVariantKind::Framed,
        },
        MyelonLayerVariantSpec {
            layer: "rkyv_nofrag",
            codec: Some("rkyv"),
            summary_label: "rkyv_nofrag:",
            segment_prefix: "ml_rkyv_nf",
            prod_role: rkyv_roles.0,
            cons_role: rkyv_roles.1,
            buffer_override: None,
            kind: MyelonLayerVariantKind::LayerScenario {
                codec_env: None,
                include_payload_size: false,
            },
        },
        MyelonLayerVariantSpec {
            layer: "typed_zero_copy",
            codec: Some("rkyv"),
            summary_label: "typed_zero_copy:",
            segment_prefix: "ml_typed_zc",
            prod_role: "typed_zc_prod",
            cons_role: "typed_zc_cons",
            buffer_override: None,
            kind: MyelonLayerVariantKind::LayerScenario {
                codec_env: Some("rkyv"),
                include_payload_size: true,
            },
        },
        MyelonLayerVariantSpec {
            layer: "typed_zero_copy_flatbuf",
            codec: Some("flatbuf"),
            summary_label: "typed_zero_copy_flatbuf:",
            segment_prefix: "ml_typed_zc_fb",
            prod_role: "typed_zc_prod",
            cons_role: "typed_zc_cons",
            buffer_override: None,
            kind: MyelonLayerVariantKind::LayerScenario {
                codec_env: Some("flatbuf"),
                include_payload_size: true,
            },
        },
    ];

    if let Some((prod_role, cons_role, buffer_override)) = myelon_right_sized_framed_roles(size_tag)
    {
        specs.insert(
            3,
            MyelonLayerVariantSpec {
                layer: "framed_right",
                codec: None,
                summary_label: "framed_right:",
                segment_prefix: "ml_rsf",
                prod_role,
                cons_role,
                buffer_override: Some(buffer_override),
                kind: MyelonLayerVariantKind::Framed,
            },
        );
    }

    specs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_tables_resolve_expected_pairs() {
        assert_eq!(myelon_raw_roles("16KB"), ("raw_prod_16k", "raw_cons_16k"));
        assert_eq!(
            myelon_right_sized_framed_roles("4KB"),
            Some(("rs_framed_prod_8k", "rs_framed_cons_8k", 32_768))
        );
        assert_eq!(
            nofrag_rkyv_roles(SweepBackend::Mmap, "64KB"),
            ("rkyv_mmap_prod_128k", "rkyv_mmap_cons_128k")
        );
        assert_eq!(
            nofrag_flatbuf_roles(SweepBackend::Shm, "1KB"),
            ("fb_shm_prod_2k", "fb_shm_cons_2k")
        );
    }

    #[test]
    fn nofrag_variant_specs_cover_all_layer_backend_pairs() {
        let specs = nofrag_variant_specs("16KB");
        assert_eq!(specs.len(), 6);
        assert!(specs
            .iter()
            .any(|spec| spec.layer == "raw_ring" && spec.backend == SweepBackend::Shm));
        assert!(specs
            .iter()
            .any(|spec| spec.layer == "raw_ring" && spec.backend == SweepBackend::Mmap));
        assert!(specs
            .iter()
            .any(|spec| spec.layer == "rkyv_nofrag" && spec.backend == SweepBackend::Mmap));
        assert!(specs
            .iter()
            .any(|spec| spec.layer == "flatbuf_nf" && spec.backend == SweepBackend::Shm));
        assert!(specs
            .iter()
            .find(|spec| spec.layer == "raw_ring" && spec.backend == SweepBackend::Shm)
            .is_some_and(|spec| !spec.uses_codec_slot));
        assert!(specs
            .iter()
            .find(|spec| spec.layer == "rkyv_nofrag" && spec.backend == SweepBackend::Mmap)
            .is_some_and(|spec| spec.uses_codec_slot));
    }

    #[test]
    fn myelon_layer_variant_specs_cover_expected_layers() {
        let specs_16k = myelon_layer_variant_specs("16KB");
        assert_eq!(specs_16k.len(), 8);
        assert!(specs_16k.iter().any(|spec| spec.layer == "raw_ring"));
        assert!(specs_16k.iter().any(|spec| spec.layer == "raw_myelon"));
        assert!(specs_16k.iter().any(|spec| spec.layer == "framed"));
        assert!(specs_16k.iter().any(|spec| spec.layer == "framed_batch"));
        assert!(specs_16k.iter().any(|spec| spec.layer == "framed_right"));
        assert!(specs_16k.iter().any(|spec| spec.layer == "rkyv_nofrag"));
        assert!(specs_16k
            .iter()
            .any(|spec| spec.layer == "typed_zero_copy_flatbuf"));

        let specs_64k = myelon_layer_variant_specs("64KB");
        assert_eq!(specs_64k.len(), 7);
        assert!(specs_64k.iter().any(|spec| spec.layer == "raw_myelon"));
        assert!(!specs_64k.iter().any(|spec| spec.layer == "framed_right"));
    }
}
