use super::common::SweepBackend;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonsterSweepRoleKey {
    Signal,
    Ev64B,
    Ev512B,
    Ev1K,
    Ev4K,
    Ev16K,
    Ev64K,
    Ev256K,
    Ev1M,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterSweepScenarioSpec {
    pub label: String,
    pub tag: String,
    pub size_bytes: usize,
    pub events: u64,
    pub buffer: usize,
    pub consumers: usize,
    pub target_rate: u64,
    pub role_key: MonsterSweepRoleKey,
}

pub fn monster_sweep_roles(role_key: MonsterSweepRoleKey) -> (&'static str, &'static str) {
    match role_key {
        MonsterSweepRoleKey::Signal => ("sig_prod", "sig_cons"),
        MonsterSweepRoleKey::Ev64B => ("prod_64b", "cons_64b"),
        MonsterSweepRoleKey::Ev512B => ("prod_512b", "cons_512b"),
        MonsterSweepRoleKey::Ev1K => ("prod_1k", "cons_1k"),
        MonsterSweepRoleKey::Ev4K => ("prod_4k", "cons_4k"),
        MonsterSweepRoleKey::Ev16K => ("prod_16k", "cons_16k"),
        MonsterSweepRoleKey::Ev64K => ("prod_64k", "cons_64k"),
        MonsterSweepRoleKey::Ev256K => ("prod_256k", "cons_256k"),
        MonsterSweepRoleKey::Ev1M => ("prod_1m", "cons_1m"),
    }
}

fn monster_sweep_size_label(bytes: usize) -> &'static str {
    match bytes {
        64 => "64B",
        512 => "512B",
        1024 => "1KB",
        4096 => "4KB",
        16384 => "16KB",
        65536 => "64KB",
        262144 => "256KB",
        1048576 => "1MB",
        _ => "?",
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "catalog rows are constructed from explicit benchmark dimensions"
)]
fn push_monster_sweep_scenario(
    specs: &mut Vec<MonsterSweepScenarioSpec>,
    label: impl Into<String>,
    tag: impl Into<String>,
    size_bytes: usize,
    events: u64,
    buffer: usize,
    consumers: usize,
    target_rate: u64,
    role_key: MonsterSweepRoleKey,
) {
    specs.push(MonsterSweepScenarioSpec {
        label: label.into(),
        tag: tag.into(),
        size_bytes,
        events,
        buffer,
        consumers,
        target_rate,
        role_key,
    });
}

pub fn monster_sweep_scenarios(
    backend: SweepBackend,
    run_throughput: bool,
    run_co: bool,
) -> Vec<MonsterSweepScenarioSpec> {
    let mut specs = Vec::new();

    if run_throughput {
        push_monster_sweep_scenario(
            &mut specs,
            "signal",
            "SIG",
            64,
            10_000_000,
            65_536,
            1,
            0,
            MonsterSweepRoleKey::Signal,
        );

        for (label, size, events, buffer, role_key, tag) in [
            (
                "64B",
                64usize,
                10_000_000u64,
                65_536usize,
                MonsterSweepRoleKey::Ev64B,
                "64B",
            ),
            (
                "512B",
                512,
                5_000_000,
                131_072,
                MonsterSweepRoleKey::Ev512B,
                "512B",
            ),
            (
                "1KB",
                1_024,
                2_000_000,
                131_072,
                MonsterSweepRoleKey::Ev1K,
                "1K",
            ),
            (
                "4KB",
                4_096,
                1_000_000,
                65_536,
                MonsterSweepRoleKey::Ev4K,
                "4K",
            ),
            (
                "16KB",
                16_384,
                500_000,
                32_768,
                MonsterSweepRoleKey::Ev16K,
                "16K",
            ),
            (
                "64KB",
                65_536,
                200_000,
                16_384,
                MonsterSweepRoleKey::Ev64K,
                "64K",
            ),
            (
                "256KB",
                262_144,
                100_000,
                8_192,
                MonsterSweepRoleKey::Ev256K,
                "256K",
            ),
            (
                "1MB",
                1_048_576,
                50_000,
                4_096,
                MonsterSweepRoleKey::Ev1M,
                "1M",
            ),
        ] {
            push_monster_sweep_scenario(
                &mut specs, label, tag, size, events, buffer, 1, 0, role_key,
            );
        }

        match backend {
            SweepBackend::Shm => {
                for consumers in [2usize, 4, 6, 8, 10, 12] {
                    for (size, events, buffer, role_key, base_tag) in [
                        (
                            1_024usize,
                            200_000u64,
                            131_072usize,
                            MonsterSweepRoleKey::Ev1K,
                            "1K",
                        ),
                        (4_096, 100_000, 65_536, MonsterSweepRoleKey::Ev4K, "4K"),
                    ] {
                        push_monster_sweep_scenario(
                            &mut specs,
                            format!("{}x{}c", monster_sweep_size_label(size), consumers),
                            format!("{base_tag}_{consumers}c"),
                            size,
                            events,
                            buffer,
                            consumers,
                            0,
                            role_key,
                        );
                    }
                }
            }
            SweepBackend::Mmap => {
                for consumers in [2usize, 4, 6, 8, 12] {
                    push_monster_sweep_scenario(
                        &mut specs,
                        format!("signalx{consumers}c"),
                        format!("SIG_{consumers}c"),
                        64,
                        1_000_000,
                        65_536,
                        consumers,
                        0,
                        MonsterSweepRoleKey::Signal,
                    );
                }

                for consumers in [2usize, 4, 6, 8, 12] {
                    for (size, events, buffer, role_key, base_tag) in [
                        (
                            64usize,
                            1_000_000u64,
                            65_536usize,
                            MonsterSweepRoleKey::Ev64B,
                            "64B",
                        ),
                        (512, 500_000, 131_072, MonsterSweepRoleKey::Ev512B, "512B"),
                        (
                            1_024usize,
                            200_000u64,
                            131_072usize,
                            MonsterSweepRoleKey::Ev1K,
                            "1K",
                        ),
                        (4_096, 100_000, 65_536, MonsterSweepRoleKey::Ev4K, "4K"),
                        (16_384, 50_000, 32_768, MonsterSweepRoleKey::Ev16K, "16K"),
                        (65_536, 20_000, 16_384, MonsterSweepRoleKey::Ev64K, "64K"),
                        (262_144, 10_000, 8_192, MonsterSweepRoleKey::Ev256K, "256K"),
                        (1_048_576, 5_000, 4_096, MonsterSweepRoleKey::Ev1M, "1M"),
                    ] {
                        push_monster_sweep_scenario(
                            &mut specs,
                            format!("{}x{}c", monster_sweep_size_label(size), consumers),
                            format!("{base_tag}_{consumers}c"),
                            size,
                            events,
                            buffer,
                            consumers,
                            0,
                            role_key,
                        );
                    }
                }
            }
        }
    }

    if run_co {
        match backend {
            SweepBackend::Shm => {
                for (label, size, buffer, rate, role_key, tag) in [
                    (
                        "1KB@100K",
                        1_024usize,
                        131_072usize,
                        100_000u64,
                        MonsterSweepRoleKey::Ev1K,
                        "1K_CO",
                    ),
                    (
                        "1KB@500K",
                        1_024,
                        131_072,
                        500_000,
                        MonsterSweepRoleKey::Ev1K,
                        "1K_CO",
                    ),
                    (
                        "1KB@1M",
                        1_024,
                        131_072,
                        1_000_000,
                        MonsterSweepRoleKey::Ev1K,
                        "1K_CO",
                    ),
                    (
                        "4KB@100K",
                        4_096,
                        65_536,
                        100_000,
                        MonsterSweepRoleKey::Ev4K,
                        "4K_CO",
                    ),
                    (
                        "4KB@500K",
                        4_096,
                        65_536,
                        500_000,
                        MonsterSweepRoleKey::Ev4K,
                        "4K_CO",
                    ),
                    (
                        "16KB@100K",
                        16_384,
                        32_768,
                        100_000,
                        MonsterSweepRoleKey::Ev16K,
                        "16K_CO",
                    ),
                    (
                        "64KB@50K",
                        65_536,
                        16_384,
                        50_000,
                        MonsterSweepRoleKey::Ev64K,
                        "64K_CO",
                    ),
                    (
                        "64KB@100K",
                        65_536,
                        16_384,
                        100_000,
                        MonsterSweepRoleKey::Ev64K,
                        "64K_CO",
                    ),
                    (
                        "256KB@10K",
                        262_144,
                        8_192,
                        10_000,
                        MonsterSweepRoleKey::Ev256K,
                        "256K_CO",
                    ),
                    (
                        "256KB@50K",
                        262_144,
                        8_192,
                        50_000,
                        MonsterSweepRoleKey::Ev256K,
                        "256K_CO",
                    ),
                    (
                        "1MB@10K",
                        1_048_576,
                        4_096,
                        10_000,
                        MonsterSweepRoleKey::Ev1M,
                        "1M_CO",
                    ),
                    (
                        "1MB@30K",
                        1_048_576,
                        4_096,
                        30_000,
                        MonsterSweepRoleKey::Ev1M,
                        "1M_CO",
                    ),
                ] {
                    push_monster_sweep_scenario(
                        &mut specs, label, tag, size, 100_000, buffer, 1, rate, role_key,
                    );
                }
            }
            SweepBackend::Mmap => {
                for (label, size, buffer, rate, role_key, tag) in [
                    (
                        "1KB@100K",
                        1_024usize,
                        131_072usize,
                        100_000u64,
                        MonsterSweepRoleKey::Ev1K,
                        "1K_CO",
                    ),
                    (
                        "1KB@500K",
                        1_024,
                        131_072,
                        500_000,
                        MonsterSweepRoleKey::Ev1K,
                        "1K_CO",
                    ),
                    (
                        "4KB@100K",
                        4_096,
                        65_536,
                        100_000,
                        MonsterSweepRoleKey::Ev4K,
                        "4K_CO",
                    ),
                    (
                        "64KB@50K",
                        65_536,
                        16_384,
                        50_000,
                        MonsterSweepRoleKey::Ev64K,
                        "64K_CO",
                    ),
                    (
                        "64KB@100K",
                        65_536,
                        16_384,
                        100_000,
                        MonsterSweepRoleKey::Ev64K,
                        "64K_CO",
                    ),
                    (
                        "256KB@10K",
                        262_144,
                        8_192,
                        10_000,
                        MonsterSweepRoleKey::Ev256K,
                        "256K_CO",
                    ),
                    (
                        "1MB@10K",
                        1_048_576,
                        4_096,
                        10_000,
                        MonsterSweepRoleKey::Ev1M,
                        "1M_CO",
                    ),
                ] {
                    push_monster_sweep_scenario(
                        &mut specs, label, tag, size, 100_000, buffer, 1, rate, role_key,
                    );
                }

                for consumers in [2usize, 4, 6, 8, 12] {
                    for (size, buffer, rate, role_key, base_tag) in [
                        (
                            1_024usize,
                            131_072usize,
                            100_000u64,
                            MonsterSweepRoleKey::Ev1K,
                            "1K_CO",
                        ),
                        (1_024, 131_072, 500_000, MonsterSweepRoleKey::Ev1K, "1K_CO"),
                        (4_096, 65_536, 100_000, MonsterSweepRoleKey::Ev4K, "4K_CO"),
                        (65_536, 16_384, 50_000, MonsterSweepRoleKey::Ev64K, "64K_CO"),
                        (
                            65_536,
                            16_384,
                            100_000,
                            MonsterSweepRoleKey::Ev64K,
                            "64K_CO",
                        ),
                        (
                            262_144,
                            8_192,
                            10_000,
                            MonsterSweepRoleKey::Ev256K,
                            "256K_CO",
                        ),
                        (1_048_576, 4_096, 10_000, MonsterSweepRoleKey::Ev1M, "1M_CO"),
                    ] {
                        push_monster_sweep_scenario(
                            &mut specs,
                            format!(
                                "{}@{}Kx{}c",
                                monster_sweep_size_label(size),
                                rate / 1000,
                                consumers
                            ),
                            format!("{base_tag}_{consumers}c"),
                            size,
                            100_000,
                            buffer,
                            consumers,
                            rate,
                            role_key,
                        );
                    }
                }
            }
        }
    }

    specs
}

pub fn monster_sweep_should_run(
    size_filter: &str,
    quick_mode: bool,
    backend: SweepBackend,
    spec: &MonsterSweepScenarioSpec,
) -> bool {
    match size_filter {
        "all" => {
            if !quick_mode {
                return true;
            }

            match backend {
                SweepBackend::Shm => {
                    let is_quick_tput =
                        matches!(spec.tag.as_str(), "SIG" | "64B" | "1K" | "64K" | "1M");
                    let is_quick_multi =
                        spec.tag.starts_with("1K_") && matches!(spec.consumers, 2 | 6 | 12);
                    let is_quick_co = (spec.tag == "1K_CO" && spec.target_rate == 500_000)
                        || (spec.tag == "64K_CO" && spec.target_rate == 50_000);
                    (is_quick_tput && spec.consumers == 1) || is_quick_multi || is_quick_co
                }
                SweepBackend::Mmap => {
                    matches!(
                        spec.tag.as_str(),
                        "SIG" | "64B" | "1K" | "64K" | "1M" | "1K_CO" | "64K_CO"
                    ) && (spec.target_rate == 0
                        || spec.target_rate == 500_000
                        || spec.target_rate == 50_000)
                }
            }
        }
        tag => spec.tag == tag,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monster_sweep_scenarios_cover_backend_specific_matrices() {
        let shm = monster_sweep_scenarios(SweepBackend::Shm, true, true);
        assert!(shm.iter().any(|spec| spec.tag == "1K_10c"));
        assert!(!shm.iter().any(|spec| spec.tag == "SIG_2c"));
        assert!(shm
            .iter()
            .any(|spec| spec.tag == "1M_CO" && spec.target_rate == 30_000));

        let mmap = monster_sweep_scenarios(SweepBackend::Mmap, true, true);
        assert!(mmap.iter().any(|spec| spec.tag == "SIG_12c"));
        assert!(mmap
            .iter()
            .any(|spec| spec.tag == "1M_CO_12c" && spec.target_rate == 10_000));
        assert!(!mmap.iter().any(|spec| spec.tag == "1K_10c"));
    }

    #[test]
    fn monster_sweep_quick_filter_matches_existing_rules() {
        let shm = monster_sweep_scenarios(SweepBackend::Shm, true, true);
        let shm_multi = shm
            .iter()
            .find(|spec| spec.tag == "1K_6c")
            .expect("1K_6c shm scenario");
        assert!(monster_sweep_should_run(
            "all",
            true,
            SweepBackend::Shm,
            shm_multi
        ));
        let shm_non_quick = shm
            .iter()
            .find(|spec| spec.tag == "4K_6c")
            .expect("4K_6c shm scenario");
        assert!(!monster_sweep_should_run(
            "all",
            true,
            SweepBackend::Shm,
            shm_non_quick
        ));

        let mmap = monster_sweep_scenarios(SweepBackend::Mmap, true, true);
        let mmap_sig = mmap
            .iter()
            .find(|spec| spec.tag == "SIG")
            .expect("SIG mmap scenario");
        assert!(monster_sweep_should_run(
            "all",
            true,
            SweepBackend::Mmap,
            mmap_sig
        ));
        let mmap_multi = mmap
            .iter()
            .find(|spec| spec.tag == "1K_2c")
            .expect("1K_2c mmap scenario");
        assert!(!monster_sweep_should_run(
            "all",
            true,
            SweepBackend::Mmap,
            mmap_multi
        ));
    }

    #[test]
    fn monster_sweep_roles_match_expected_children() {
        assert_eq!(
            monster_sweep_roles(MonsterSweepRoleKey::Signal),
            ("sig_prod", "sig_cons")
        );
        assert_eq!(
            monster_sweep_roles(MonsterSweepRoleKey::Ev64K),
            ("prod_64k", "cons_64k")
        );
    }
}
