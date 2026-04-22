use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterOrigin {
    Internal,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildMode {
    Cargo,
    Make,
    System,
    Planned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterId {
    DisruptorShm,
    DisruptorMmap,
    MyelonRawShm,
    MyelonRawMmap,
    ShmIpcRs,
    BoostMq,
    Ompi,
    Rusteron,
    Crossbar,
    ZeroMq,
    ZeroMqIpc,
    ZeroMqIpcAbs,
    ZeroMqTcp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdapterSpec {
    pub id: AdapterId,
    pub display_name: &'static str,
    pub output_prefix: &'static str,
    pub origin: AdapterOrigin,
    pub build_mode: BuildMode,
    pub supports_throughput: bool,
    pub supports_fixed_rate: bool,
    pub supports_headon: bool,
}

static ADAPTERS: OnceLock<Vec<AdapterSpec>> = OnceLock::new();

pub fn parity_adapters() -> &'static [AdapterSpec] {
    ADAPTERS
        .get_or_init(|| {
            vec![
                AdapterSpec {
                    id: AdapterId::DisruptorShm,
                    display_name: "disruptor-shm",
                    output_prefix: "disruptor",
                    origin: AdapterOrigin::Internal,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: true,
                },
                AdapterSpec {
                    id: AdapterId::DisruptorMmap,
                    display_name: "disruptor-mmap",
                    output_prefix: "disruptor_mmap",
                    origin: AdapterOrigin::Internal,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::MyelonRawShm,
                    display_name: "myelon-raw-shm",
                    output_prefix: "myelon_raw",
                    origin: AdapterOrigin::Internal,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::MyelonRawMmap,
                    display_name: "myelon-raw-mmap",
                    output_prefix: "myelon_raw_mmap",
                    origin: AdapterOrigin::Internal,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::ShmIpcRs,
                    display_name: "shmipc-rs",
                    output_prefix: "shmipc",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::BoostMq,
                    display_name: "boost-message-queue",
                    output_prefix: "boost",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Make,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::Ompi,
                    display_name: "ompi-vader-self",
                    output_prefix: "ompi",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Make,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::Rusteron,
                    display_name: "rusteron-aeron-ipc",
                    output_prefix: "rusteron",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: true,
                },
                AdapterSpec {
                    id: AdapterId::Crossbar,
                    display_name: "crossbar-channel",
                    output_prefix: "crossbar",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::ZeroMq,
                    display_name: "zeromq-default",
                    output_prefix: "zmq",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::ZeroMqIpc,
                    display_name: "zeromq-ipc",
                    output_prefix: "zmqipc",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::ZeroMqIpcAbs,
                    display_name: "zeromq-ipc-abs",
                    output_prefix: "zmqabs",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
                AdapterSpec {
                    id: AdapterId::ZeroMqTcp,
                    display_name: "zeromq-tcp",
                    output_prefix: "zmqtcp",
                    origin: AdapterOrigin::External,
                    build_mode: BuildMode::Cargo,
                    supports_throughput: true,
                    supports_fixed_rate: true,
                    supports_headon: false,
                },
            ]
        })
        .as_slice()
}

pub fn headon_adapters() -> Vec<AdapterSpec> {
    parity_adapters()
        .iter()
        .copied()
        .filter(|spec| spec.supports_headon)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{headon_adapters, parity_adapters, AdapterId, AdapterOrigin, BuildMode};

    #[test]
    fn parity_inventory_matches_world_domination_surface() {
        let ids = parity_adapters()
            .iter()
            .map(|spec| spec.id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                AdapterId::DisruptorShm,
                AdapterId::DisruptorMmap,
                AdapterId::MyelonRawShm,
                AdapterId::MyelonRawMmap,
                AdapterId::ShmIpcRs,
                AdapterId::BoostMq,
                AdapterId::Ompi,
                AdapterId::Rusteron,
                AdapterId::Crossbar,
                AdapterId::ZeroMq,
                AdapterId::ZeroMqIpc,
                AdapterId::ZeroMqIpcAbs,
                AdapterId::ZeroMqTcp,
            ]
        );
    }

    #[test]
    fn internal_scope_stays_narrow() {
        let internal = parity_adapters()
            .iter()
            .filter(|spec| spec.origin == AdapterOrigin::Internal)
            .collect::<Vec<_>>();
        assert_eq!(internal.len(), 4);
        assert_eq!(internal[0].id, AdapterId::DisruptorShm);
        assert_eq!(internal[1].id, AdapterId::DisruptorMmap);
        assert_eq!(internal[2].id, AdapterId::MyelonRawShm);
        assert_eq!(internal[3].id, AdapterId::MyelonRawMmap);
        assert!(internal
            .iter()
            .all(|spec| spec.build_mode == BuildMode::Cargo));
    }

    #[test]
    fn crossbar_is_wired_as_cargo_external_peer() {
        let crossbar = parity_adapters()
            .iter()
            .find(|spec| spec.id == AdapterId::Crossbar)
            .expect("crossbar adapter missing");
        assert_eq!(crossbar.origin, AdapterOrigin::External);
        assert_eq!(crossbar.build_mode, BuildMode::Cargo);
        assert!(crossbar.supports_throughput);
        assert!(crossbar.supports_fixed_rate);
        assert!(!crossbar.supports_headon);
    }

    #[test]
    fn rust_external_peers_are_no_longer_planned() {
        let ids = [
            AdapterId::ShmIpcRs,
            AdapterId::BoostMq,
            AdapterId::Ompi,
            AdapterId::Rusteron,
            AdapterId::ZeroMq,
            AdapterId::ZeroMqIpc,
            AdapterId::ZeroMqIpcAbs,
            AdapterId::ZeroMqTcp,
        ];
        for id in ids {
            let spec = parity_adapters()
                .iter()
                .find(|spec| spec.id == id)
                .expect("missing external peer");
            let expected = match id {
                AdapterId::BoostMq | AdapterId::Ompi => BuildMode::Make,
                _ => BuildMode::Cargo,
            };
            assert_eq!(spec.build_mode, expected);
        }
    }

    #[test]
    fn headon_scope_matches_contract() {
        let headon = headon_adapters();
        assert_eq!(headon.len(), 2);
        assert_eq!(headon[0].id, AdapterId::DisruptorShm);
        assert_eq!(headon[1].id, AdapterId::Rusteron);
    }
}
