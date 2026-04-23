use std::time::{Duration, Instant};

pub const CONTROL_BYTES: usize = 24;

pub fn ensure_message_size(message_size: usize) -> Result<(), String> {
    if message_size < CONTROL_BYTES {
        return Err(format!(
            "message_size must be at least {CONTROL_BYTES} bytes to hold ping-pong control data"
        ));
    }
    Ok(())
}

pub fn payload(sequence: u64, timestamp_ns: u64, intended_send_time_ns: u64, message_size: usize) -> Vec<u8> {
    let mut bytes = vec![0_u8; message_size];
    bytes[..8].copy_from_slice(&sequence.to_le_bytes());
    bytes[8..16].copy_from_slice(&timestamp_ns.to_le_bytes());
    bytes[16..24].copy_from_slice(&intended_send_time_ns.to_le_bytes());
    bytes
}

pub fn parse_payload(payload: &[u8]) -> Result<(u64, u64, u64), String> {
    if payload.len() < CONTROL_BYTES {
        return Err(format!(
            "payload length {} is smaller than required control bytes {CONTROL_BYTES}",
            payload.len()
        ));
    }

    let sequence = u64::from_le_bytes(payload[..8].try_into().expect("sequence slice"));
    let timestamp_ns = u64::from_le_bytes(payload[8..16].try_into().expect("timestamp slice"));
    let intended_send_time_ns =
        u64::from_le_bytes(payload[16..24].try_into().expect("intended slice"));
    Ok((sequence, timestamp_ns, intended_send_time_ns))
}

pub fn pace_until(intended: Instant) {
    loop {
        let now = Instant::now();
        if now >= intended {
            break;
        }
        let wait = intended - now;
        if wait > Duration::from_millis(1) {
            std::thread::sleep(wait - Duration::from_micros(100));
        } else {
            std::hint::spin_loop();
        }
    }
}

pub fn interval_for_rate(target_rate: u64) -> Result<Duration, String> {
    if target_rate == 0 {
        return Err("target_rate must be > 0".into());
    }
    Ok(Duration::from_nanos(std::cmp::max(
        1,
        1_000_000_000u64 / target_rate,
    )))
}

pub fn scoped_name(prefix: &str, base: &str, suffix: &str) -> String {
    let sanitized = base
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' => ch,
            'A'..='Z' => ch.to_ascii_lowercase(),
            _ => '-',
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    let sanitized = if sanitized.is_empty() { "run" } else { &sanitized };
    let mut name = format!("{prefix}-{sanitized}-{suffix}");
    if name.len() > 200 {
        name.truncate(200);
    }
    name
}
