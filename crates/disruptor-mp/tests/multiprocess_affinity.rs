#![cfg(not(target_os = "linux"))]

use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, portable_shm_segment_name,
};
use myelon_env::runtime as runtime_env;
use std::sync::{Mutex, MutexGuard};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Copy, Clone, Default)]
struct Event {
    value: u64,
}

fn unique_name(prefix: &str) -> String {
    portable_shm_segment_name(prefix)
}

struct AffinityEnvGuard {
    _lock: MutexGuard<'static, ()>,
    saved: Vec<(&'static str, Option<String>)>,
}

impl AffinityEnvGuard {
    fn set(vars: &[(&'static str, &'static str)]) -> Self {
        let lock = ENV_LOCK.lock().expect("env lock should not be poisoned");
        let mut saved = Vec::with_capacity(vars.len());
        for (key, value) in vars {
            saved.push((*key, std::env::var(key).ok()));
            unsafe {
                std::env::set_var(key, value);
            }
        }

        Self { _lock: lock, saved }
    }
}

impl Drop for AffinityEnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.saved.iter().rev() {
            match value {
                Some(previous) => unsafe {
                    std::env::set_var(key, previous);
                },
                None => unsafe {
                    std::env::remove_var(key);
                },
            }
        }
    }
}

#[test]
fn builder_process_core_requests_do_not_break_manual_bringup_on_non_linux(
) -> Result<(), Box<dyn std::error::Error>> {
    let name = unique_name("afman");
    let _producer = build_shared_single_producer::<Event>(&name, 64)
        .with_process_core(0)
        .build_producer(Event::default)?;
    let _consumer = attach_shared_consumer::<Event>(&name, 64)
        .with_process_core(1)
        .build_consumer()?;

    Ok(())
}

#[test]
fn builder_affinity_requests_do_not_break_auto_consumer_bringup_on_non_linux(
) -> Result<(), Box<dyn std::error::Error>> {
    let name = unique_name("afauto");
    let _producer = build_shared_single_producer::<Event>(&name, 64)
        .with_process_core(0)
        .build_producer(Event::default)?;
    let _consumer = attach_shared_consumer::<Event>(&name, 64)
        .with_process_core(1)
        .with_consumer_core(2)
        .handle_events_with(|event, _sequence, _end_of_batch| {
            let _ = event.value;
        })?;

    Ok(())
}

#[test]
fn env_affinity_requests_do_not_break_bringup_on_non_linux(
) -> Result<(), Box<dyn std::error::Error>> {
    let _env = AffinityEnvGuard::set(&[
        (runtime_env::PRODUCER_CORE, "3"),
        (runtime_env::CONSUMER_CORE, "4"),
        (runtime_env::AUTO_CONSUMER_CORE, "5"),
    ]);

    let name = unique_name("afenv");
    let _producer =
        build_shared_single_producer::<Event>(&name, 64).build_producer(Event::default)?;
    let _consumer = attach_shared_consumer::<Event>(&name, 64).handle_events_with(
        |event, _sequence, _end_of_batch| {
            let _ = event.value;
        },
    )?;

    Ok(())
}
