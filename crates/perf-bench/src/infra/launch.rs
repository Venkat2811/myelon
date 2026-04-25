//! Shared multiprocess launch helpers for bench scenarios.

use super::bench::ScenarioChildren;
use super::child_runner::BenchError;
use super::naming::{unique_mmap_root, unique_mmap_segment, unique_shm_segment};
use super::process::spawn_child;
use std::path::Path;

pub struct MultiConsumerSpawn<'a> {
    pub producer_role: &'a str,
    pub consumer_role: &'a str,
    pub consumers: usize,
    pub consumer_id_env: &'static str,
    pub base_envs: Vec<(&'static str, String)>,
}

pub fn launch_shm_group(
    exe: &Path,
    segment_prefix: &str,
    segment_env_key: &'static str,
    mut spawn: MultiConsumerSpawn<'_>,
) -> Result<ScenarioChildren, BenchError> {
    let segment = unique_shm_segment(segment_prefix);
    spawn.base_envs.push((segment_env_key, segment));

    let producer = spawn_child(exe, spawn.producer_role, &spawn.base_envs);
    let consumers = (0..spawn.consumers)
        .map(|consumer_id| {
            let mut consumer_envs = spawn.base_envs.clone();
            consumer_envs.push((spawn.consumer_id_env, consumer_id.to_string()));
            spawn_child(exe, spawn.consumer_role, &consumer_envs)
        })
        .collect();

    Ok(ScenarioChildren::new(producer, consumers))
}

pub fn launch_mmap_group(
    exe: &Path,
    root_prefix: &str,
    segment_prefix: &str,
    root_env_key: &'static str,
    segment_env_key: &'static str,
    mut spawn: MultiConsumerSpawn<'_>,
) -> Result<ScenarioChildren, BenchError> {
    let root = unique_mmap_root(root_prefix);
    let segment = unique_mmap_segment(segment_prefix);
    spawn
        .base_envs
        .push((root_env_key, root.display().to_string()));
    spawn.base_envs.push((segment_env_key, segment));

    let producer = spawn_child(exe, spawn.producer_role, &spawn.base_envs);
    let consumers = (0..spawn.consumers)
        .map(|consumer_id| {
            let mut consumer_envs = spawn.base_envs.clone();
            consumer_envs.push((spawn.consumer_id_env, consumer_id.to_string()));
            spawn_child(exe, spawn.consumer_role, &consumer_envs)
        })
        .collect();

    Ok(ScenarioChildren::new(producer, consumers).with_cleanup_path(root))
}
