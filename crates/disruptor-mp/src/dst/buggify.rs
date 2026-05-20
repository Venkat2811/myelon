use myelon_env::{dst::buggify as dst_env, read};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuggifyConfig {
    pub seed: u64,
    pub activation_percent: u8,
    pub fire_percent: u8,
}

impl BuggifyConfig {
    fn from_env() -> Option<Self> {
        let enabled = read::optional(dst_env::ENABLED)?;
        if enabled == "0" || enabled.eq_ignore_ascii_case("false") {
            return None;
        }

        let seed = read::parse_or(dst_env::SEED, 1);
        let activation_percent = read::parse_or(dst_env::ACTIVATION_PERCENT, 25);
        let fire_percent = read::parse_or(dst_env::FIRE_PERCENT, 25);

        Some(Self {
            seed,
            activation_percent,
            fire_percent,
        })
    }
}

fn counters() -> &'static Mutex<HashMap<(&'static str, u32), u64>> {
    static COUNTERS: OnceLock<Mutex<HashMap<(&'static str, u32), u64>>> = OnceLock::new();
    COUNTERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

fn callsite_hash(seed: u64, file: &'static str, line: u32, ordinal: u64) -> u64 {
    let mut hash = seed ^ (line as u64).wrapping_mul(0x9e3779b97f4a7c15);
    for byte in file.as_bytes() {
        hash = hash.rotate_left(5) ^ (*byte as u64);
        hash = hash.wrapping_mul(0x517cc1b727220a95);
    }
    mix(hash ^ ordinal.wrapping_mul(0x94d049bb133111eb))
}

pub fn reset() {
    counters()
        .lock()
        .expect("dst buggify counters should not be poisoned")
        .clear();
}

pub struct ScopedBuggify {
    previous_enabled: Option<String>,
    previous_seed: Option<String>,
    previous_activation_percent: Option<String>,
    previous_fire_percent: Option<String>,
}

impl ScopedBuggify {
    pub fn new(seed: u64) -> Self {
        let guard = Self {
            previous_enabled: std::env::var(dst_env::ENABLED).ok(),
            previous_seed: std::env::var(dst_env::SEED).ok(),
            previous_activation_percent: std::env::var(dst_env::ACTIVATION_PERCENT).ok(),
            previous_fire_percent: std::env::var(dst_env::FIRE_PERCENT).ok(),
        };

        std::env::set_var(dst_env::ENABLED, "1");
        std::env::set_var(dst_env::SEED, seed.to_string());
        reset();
        guard
    }
}

impl Drop for ScopedBuggify {
    fn drop(&mut self) {
        if let Some(value) = &self.previous_enabled {
            std::env::set_var(dst_env::ENABLED, value);
        } else {
            std::env::remove_var(dst_env::ENABLED);
        }

        if let Some(value) = &self.previous_seed {
            std::env::set_var(dst_env::SEED, value);
        } else {
            std::env::remove_var(dst_env::SEED);
        }

        if let Some(value) = &self.previous_activation_percent {
            std::env::set_var(dst_env::ACTIVATION_PERCENT, value);
        } else {
            std::env::remove_var(dst_env::ACTIVATION_PERCENT);
        }

        if let Some(value) = &self.previous_fire_percent {
            std::env::set_var(dst_env::FIRE_PERCENT, value);
        } else {
            std::env::remove_var(dst_env::FIRE_PERCENT);
        }

        reset();
    }
}

pub fn buggify(file: &'static str, line: u32) -> bool {
    let Some(config) = BuggifyConfig::from_env() else {
        return false;
    };

    let activation_roll = callsite_hash(config.seed, file, line, 0) % 100;
    if activation_roll >= config.activation_percent as u64 {
        return false;
    }

    let ordinal = {
        let mut counters = counters()
            .lock()
            .expect("dst buggify counters should not be poisoned");
        let entry = counters.entry((file, line)).or_insert(0);
        let ordinal = *entry;
        *entry += 1;
        ordinal
    };

    let fire_roll = callsite_hash(config.seed ^ 0xa5a5_5a5a_d15c_a11e, file, line, ordinal) % 100;
    fire_roll < config.fire_percent as u64
}

#[macro_export]
macro_rules! dst_buggify {
    () => {
        $super::buggify::buggify(file!(), line!())
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn disabled_without_env() {
        let _guard = env_lock().lock().expect("env lock");
        std::env::remove_var(dst_env::ENABLED);
        std::env::remove_var(dst_env::SEED);
        reset();
        assert!(!buggify(file!(), line!()));
    }

    #[test]
    fn deterministic_with_seed() {
        let _guard = env_lock().lock().expect("env lock");
        std::env::set_var(dst_env::ENABLED, "1");
        std::env::set_var(dst_env::SEED, "42");
        std::env::set_var(dst_env::ACTIVATION_PERCENT, "100");
        std::env::set_var(dst_env::FIRE_PERCENT, "50");

        reset();
        let first = (0..8).map(|_| buggify("a.rs", 10)).collect::<Vec<_>>();
        reset();
        let second = (0..8).map(|_| buggify("a.rs", 10)).collect::<Vec<_>>();
        assert_eq!(first, second);

        std::env::remove_var(dst_env::ENABLED);
        std::env::remove_var(dst_env::SEED);
        std::env::remove_var(dst_env::ACTIVATION_PERCENT);
        std::env::remove_var(dst_env::FIRE_PERCENT);
        reset();
    }
}
