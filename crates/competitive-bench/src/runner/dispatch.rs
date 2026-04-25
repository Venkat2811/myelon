use crate::infra::adapter::AdapterId;
use std::time::Duration;

/// How to execute a benchmark for a given adapter.
#[derive(Debug, Clone)]
pub enum ExecutionStrategy {
    /// Internal: invoke perf-bench-pingpong binary directly (single process, no server/client).
    InternalPingpong {
        layer: &'static str,
        backend: &'static str,
    },

    /// External server+client: spawn server, wait, run client, kill server.
    ExternalPingpong {
        binary_name: &'static str,
        /// If set, binary is at this path relative to the competitive-bench crate dir
        /// instead of in target/{profile}/
        relative_to_crate: bool,
        extra_server_args: Vec<String>,
        extra_client_args: Vec<String>,
        startup_delay: Duration,
        needs_aeron_env: bool,
        cleanup_shm_base: bool,
    },

    /// Open MPI: special mpirun invocation (no server/client split).
    Mpi {
        binary_relative: &'static str,
        extra_args: Vec<&'static str>,
    },

    /// Internal broadcast: invoke internal_broadcast binary with --adapter flag.
    InternalBroadcast { adapter_flag: &'static str },

    /// Crossbar broadcast: invoke crossbar_broadcast binary.
    CrossbarBroadcast,
}

/// Map an adapter ID to its execution strategy.
pub fn strategy_for(id: AdapterId) -> ExecutionStrategy {
    match id {
        AdapterId::DisruptorShm => ExecutionStrategy::InternalPingpong {
            layer: "raw_ring",
            backend: "shm",
        },
        AdapterId::DisruptorMmap => ExecutionStrategy::InternalPingpong {
            layer: "raw_ring",
            backend: "mmap",
        },
        AdapterId::MyelonRawShm => ExecutionStrategy::InternalPingpong {
            layer: "raw_myelon",
            backend: "shm",
        },
        AdapterId::MyelonRawMmap => ExecutionStrategy::InternalPingpong {
            layer: "raw_myelon",
            backend: "mmap",
        },
        AdapterId::Crossbar => ExecutionStrategy::ExternalPingpong {
            binary_name: "crossbar_pingpong",
            relative_to_crate: false,
            extra_server_args: vec![],
            extra_client_args: vec![],
            startup_delay: Duration::from_millis(200),
            needs_aeron_env: false,
            cleanup_shm_base: false,
        },
        AdapterId::ShmIpcRs => ExecutionStrategy::ExternalPingpong {
            binary_name: "shmipc_pingpong",
            relative_to_crate: false,
            extra_server_args: vec![],
            extra_client_args: vec![],
            startup_delay: Duration::from_millis(500),
            needs_aeron_env: false,
            cleanup_shm_base: false,
        },
        AdapterId::Rusteron => ExecutionStrategy::ExternalPingpong {
            binary_name: "rusteron_pingpong",
            relative_to_crate: false,
            extra_server_args: vec![],
            extra_client_args: vec![],
            startup_delay: Duration::from_millis(500),
            needs_aeron_env: true,
            cleanup_shm_base: true,
        },
        AdapterId::Iceoryx2 => ExecutionStrategy::ExternalPingpong {
            binary_name: "iceoryx2_pingpong",
            relative_to_crate: false,
            extra_server_args: vec![],
            extra_client_args: vec![],
            startup_delay: Duration::from_millis(300),
            needs_aeron_env: false,
            cleanup_shm_base: false,
        },
        AdapterId::ZeroMq | AdapterId::ZeroMqIpc => ExecutionStrategy::ExternalPingpong {
            binary_name: "zmq_pingpong",
            relative_to_crate: false,
            extra_server_args: vec!["--transport".into(), "ipc".into()],
            extra_client_args: vec!["--transport".into(), "ipc".into()],
            startup_delay: Duration::from_millis(200),
            needs_aeron_env: false,
            cleanup_shm_base: false,
        },
        AdapterId::ZeroMqIpcAbs => ExecutionStrategy::ExternalPingpong {
            binary_name: "zmq_pingpong",
            relative_to_crate: false,
            extra_server_args: vec!["--transport".into(), "ipc-abs".into()],
            extra_client_args: vec!["--transport".into(), "ipc-abs".into()],
            startup_delay: Duration::from_millis(200),
            needs_aeron_env: false,
            cleanup_shm_base: false,
        },
        AdapterId::ZeroMqTcp => ExecutionStrategy::ExternalPingpong {
            binary_name: "zmq_pingpong",
            relative_to_crate: false,
            extra_server_args: vec!["--transport".into(), "tcp".into()],
            extra_client_args: vec!["--transport".into(), "tcp".into()],
            startup_delay: Duration::from_millis(200),
            needs_aeron_env: false,
            cleanup_shm_base: false,
        },
        AdapterId::BoostMq => ExecutionStrategy::ExternalPingpong {
            binary_name: "third_party/boost_pingpong/boost_pingpong",
            relative_to_crate: true,
            extra_server_args: vec![],
            extra_client_args: vec![],
            startup_delay: Duration::from_millis(200),
            needs_aeron_env: false,
            cleanup_shm_base: false,
        },
        AdapterId::Ompi => ExecutionStrategy::Mpi {
            binary_relative: "third_party/ompi_pingpong/pingpong",
            extra_args: vec![
                "--oversubscribe",
                "-n",
                "2",
                "--mca",
                "pml",
                "ob1",
                "--mca",
                "btl",
                "vader,self",
            ],
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::adapter::parity_adapters;

    #[test]
    fn every_adapter_has_a_strategy() {
        for spec in parity_adapters() {
            let _ = strategy_for(spec.id);
        }
    }
}
