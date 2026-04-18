use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Conservative shared-memory segment-name budget that stays portable on macOS.
pub const PORTABLE_SHM_SEGMENT_NAME_MAX_LEN: usize = 14;
const PORTABLE_SHM_SEGMENT_SUFFIX_LEN: usize = 8;
const PORTABLE_SHM_SEGMENT_SUFFIX_MODULUS: u64 = 2_821_109_907_456; // 36^8

static NEXT_SEGMENT_SUFFIX: AtomicU64 = AtomicU64::new(0);

fn encode_base36(mut value: u64, width: usize) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = vec![b'0'; width];

    for slot in out.iter_mut().rev() {
        *slot = ALPHABET[(value % 36) as usize];
        value /= 36;
    }

    String::from_utf8(out).expect("base36 segment suffix should stay ASCII")
}

/// Generate a shared-memory segment name that fits the portable macOS-safe budget.
///
/// The returned name contains only ASCII alphanumeric characters and remains within
/// the strictest platform budget used by the project so benchmarks, tests, and
/// examples can share one naming contract.
pub fn portable_shm_segment_name(prefix: &str) -> String {
    let prefix_budget = PORTABLE_SHM_SEGMENT_NAME_MAX_LEN - PORTABLE_SHM_SEGMENT_SUFFIX_LEN;
    let mut short_prefix: String = prefix
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .take(prefix_budget)
        .collect();

    if short_prefix.is_empty() {
        short_prefix.push('d');
    }

    let time_seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let pid_seed = u64::from(std::process::id());
    let counter_seed = NEXT_SEGMENT_SUFFIX.fetch_add(1, Ordering::Relaxed);
    let suffix_seed = (time_seed ^ pid_seed.rotate_left(13) ^ counter_seed.rotate_left(27))
        % PORTABLE_SHM_SEGMENT_SUFFIX_MODULUS;
    let suffix = encode_base36(suffix_seed, PORTABLE_SHM_SEGMENT_SUFFIX_LEN);
    let name = format!("{short_prefix}{suffix}");

    debug_assert!(
        name.len() <= PORTABLE_SHM_SEGMENT_NAME_MAX_LEN,
        "portable segment name '{}' exceeds budget {}",
        name,
        PORTABLE_SHM_SEGMENT_NAME_MAX_LEN
    );

    name
}

#[cfg(test)]
mod tests {
    use super::{portable_shm_segment_name, PORTABLE_SHM_SEGMENT_NAME_MAX_LEN};

    #[test]
    fn generated_names_stay_within_portable_budget() {
        let name = portable_shm_segment_name("benchmark_all_wait_strategies_auto");
        assert!(name.len() <= PORTABLE_SHM_SEGMENT_NAME_MAX_LEN);
        assert!(name.chars().all(|ch| ch.is_ascii_alphanumeric()));
    }

    #[test]
    fn empty_prefix_falls_back_to_default_prefix() {
        let name = portable_shm_segment_name("---");
        assert!(name.starts_with('d'));
        assert!(name.len() <= PORTABLE_SHM_SEGMENT_NAME_MAX_LEN);
    }

    #[test]
    fn generated_names_change_across_calls() {
        let first = portable_shm_segment_name("bench");
        let second = portable_shm_segment_name("bench");
        assert_ne!(first, second);
    }
}
