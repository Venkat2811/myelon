use clap::Parser;
use crossbar::{error::Error as CrossbarError, Channel, Config, WaitStrategy};
use hdrhistogram::Histogram;
use serde::Serialize;
use std::error::Error;
use std::thread;
use std::time::{Duration, Instant};

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const CROSSBAR_BLOCK_OVERHEAD: usize = 64;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "crossbar 2-process ping-pong benchmark", long_about = None)]
struct Args {
    /// Mode: server echoes payloads; client measures RTT
    #[arg(long, value_parser = ["server", "client"], default_value = "client")]
    mode: String,

    /// Unique base name shared by server and client
    #[arg(long, default_value_t = default_base_name())]
    base: String,

    /// Payload size in bytes
    #[arg(long, short = 's', default_value_t = 64)]
    message_size: u32,

    /// Warmup messages (not recorded)
    #[arg(long, default_value_t = 10_000)]
    warmup: u64,

    /// Number of measured messages
    #[arg(long, short = 'n', default_value_t = 100_000)]
    num_messages: u64,

    /// Output JSON compatible with the competitive harness
    #[arg(long, default_value_t = false)]
    json: bool,

    /// Fixed-rate mode target (messages per second). If set, enables CO-corrected mode
    #[arg(long)]
    target_rate: Option<u64>,

    /// Peer connection timeout in milliseconds
    #[arg(long, default_value_t = 5_000)]
    timeout_ms: u64,
}

fn default_base_name() -> String {
    format!("crossbar_pp_{}", std::process::id())
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkConfigOut {
    message_size: usize,
    num_messages: u64,
    warmup_messages: u64,
    buffer_size: usize,
    wait_strategy: String,
}

#[derive(Debug, Clone, Serialize)]
struct LatencyStatsOut {
    count: u64,
    min: u64,
    max: u64,
    mean: f64,
    stdev: f64,
    p1: u64,
    p10: u64,
    p25: u64,
    p50: u64,
    p90: u64,
    p95: u64,
    p99: u64,
    p999: u64,
    p9999: u64,
    p99999: u64,
    p999999: u64,
}

impl From<&Histogram<u64>> for LatencyStatsOut {
    fn from(h: &Histogram<u64>) -> Self {
        Self {
            count: h.len(),
            min: h.min(),
            max: h.max(),
            mean: h.mean(),
            stdev: h.stdev(),
            p1: h.value_at_percentile(1.0),
            p10: h.value_at_percentile(10.0),
            p25: h.value_at_percentile(25.0),
            p50: h.value_at_percentile(50.0),
            p90: h.value_at_percentile(90.0),
            p95: h.value_at_percentile(95.0),
            p99: h.value_at_percentile(99.0),
            p999: h.value_at_percentile(99.9),
            p9999: h.value_at_percentile(99.99),
            p99999: h.value_at_percentile(99.999),
            p999999: h.value_at_percentile(99.9999),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkResultsOut {
    adapter: &'static str,
    config: BenchmarkConfigOut,
    throughput: f64,
    messages_processed: u64,
    duration_secs: f64,
    latency_stats: LatencyStatsOut,
    timestamp: String,
}

fn benchmark_config(message_size: usize) -> Config {
    let (block_count, ring_depth, stale_timeout) = if message_size >= 64 * 1024 * 1024 {
        (16, 2, Duration::from_secs(120))
    } else if message_size >= 32 * 1024 * 1024 {
        (16, 2, Duration::from_secs(90))
    } else if message_size >= 16 * 1024 * 1024 {
        (8, 4, Duration::from_secs(60))
    } else if message_size >= 8 * 1024 * 1024 {
        (8, 4, Duration::from_secs(45))
    } else if message_size >= 2 * 1024 * 1024 {
        (16, 8, Duration::from_secs(30))
    } else if message_size >= 1024 * 1024 {
        (16, 8, Duration::from_secs(15))
    } else {
        (4_096, 1_024, Duration::from_secs(5))
    };

    Config {
        max_topics: 1,
        block_count,
        block_size: block_size_for_payload(message_size) as u32,
        ring_depth,
        heartbeat_interval: Duration::from_millis(100),
        stale_timeout,
    }
}

fn block_size_for_payload(message_size: usize) -> usize {
    std::cmp::max(4_096, message_size + CROSSBAR_BLOCK_OVERHEAD)
}

fn main() -> AnyResult<()> {
    let args = Args::parse();
    match args.mode.as_str() {
        "server" => run_server(&args),
        "client" => run_client(&args),
        _ => unreachable!(),
    }
}

fn run_server(args: &Args) -> AnyResult<()> {
    let timeout = Duration::from_millis(args.timeout_ms);
    let mut channel = Channel::listen(
        &args.base,
        benchmark_config(args.message_size as usize),
        timeout,
    )?;
    let mut scratch = vec![0_u8; args.message_size as usize];

    loop {
        let msg = match channel.recv_with(WaitStrategy::BusySpin) {
            Ok(msg) => msg,
            Err(CrossbarError::PublisherDead) => return Ok(()),
            Err(other) => return Err(Box::new(other)),
        };
        let len = msg.len();
        scratch[..len].copy_from_slice(&msg[..len]);
        drop(msg);

        let mut loan = channel.loan()?;
        loan.as_mut_slice()[..len].copy_from_slice(&scratch[..len]);
        loan.set_len(len)?;
        loan.publish();
    }
}

fn run_client(args: &Args) -> AnyResult<()> {
    if let Some(rate) = args.target_rate {
        if rate == 0 {
            return Err("target_rate must be > 0".into());
        }
    }

    let timeout = Duration::from_millis(args.timeout_ms);
    let mut channel = Channel::connect(
        &args.base,
        benchmark_config(args.message_size as usize),
        timeout,
    )?;
    let payload = vec![0x5A_u8; args.message_size as usize];

    for _ in 0..args.warmup {
        round_trip(&mut channel, &payload)?;
    }

    let mut hist = Histogram::<u64>::new(3)?;
    let mut co_hist: Option<Histogram<u64>> = None;
    let start = Instant::now();

    if let Some(target_rate) = args.target_rate {
        let mut co = Histogram::<u64>::new(3)?;
        let interval = Duration::from_nanos(std::cmp::max(1, 1_000_000_000u64 / target_rate));
        let base = Instant::now();
        let mut intended = base;

        for i in 0..args.num_messages {
            if i > 0 {
                intended += interval;
            }
            pace_until(intended);

            let send_time = Instant::now();
            round_trip(&mut channel, &payload)?;
            let recv_time = Instant::now();

            hist.record(recv_time.duration_since(send_time).as_nanos() as u64)
                .ok();
            co.record(recv_time.duration_since(intended).as_nanos() as u64)
                .ok();
        }
        co_hist = Some(co);
    } else {
        for _ in 0..args.num_messages {
            let send_time = Instant::now();
            round_trip(&mut channel, &payload)?;
            hist.record(send_time.elapsed().as_nanos() as u64).ok();
        }
    }

    let duration = start.elapsed();
    let throughput = args.num_messages as f64 / duration.as_secs_f64();

    if args.json {
        let out = BenchmarkResultsOut {
            adapter: "crossbar",
            config: BenchmarkConfigOut {
                message_size: args.message_size as usize,
                num_messages: args.num_messages,
                warmup_messages: args.warmup,
                buffer_size: benchmark_config(args.message_size as usize).block_size as usize,
                wait_strategy: "busy_spin".to_string(),
            },
            throughput,
            messages_processed: args.num_messages,
            duration_secs: duration.as_secs_f64(),
            latency_stats: LatencyStatsOut::from(&hist),
            timestamp: chrono::Utc::now().to_rfc3339(),
        };
        let mut v = serde_json::to_value(&out)?;
        if let Some(rate) = args.target_rate {
            v["measurement_mode"] = serde_json::Value::String("fixed_rate".into());
            v["target_rate"] = serde_json::json!(rate);
            if let Some(co) = co_hist.as_ref() {
                v["coordinated_omission_stats"] = serde_json::to_value(LatencyStatsOut::from(co))?;
            }
        } else {
            v["measurement_mode"] = serde_json::Value::String("max_throughput".into());
        }
        println!("{}", serde_json::to_string_pretty(&v)?);
    } else {
        println!("crossbar Ping-Pong");
        println!("Size: {} bytes", args.message_size);
        println!("Throughput: {:.2} msg/s", throughput);
        println!("P50: {} ns", hist.value_at_percentile(50.0));
        println!("P99: {} ns", hist.value_at_percentile(99.0));
    }

    Ok(())
}

fn round_trip(channel: &mut Channel, payload: &[u8]) -> AnyResult<()> {
    let mut loan = channel.loan()?;
    loan.as_mut_slice()[..payload.len()].copy_from_slice(payload);
    loan.set_len(payload.len())?;
    loan.publish();

    let sample = channel.recv_with(WaitStrategy::BusySpin)?;
    if sample.len() != payload.len() {
        return Err(format!(
            "unexpected echo length: expected {}, got {}",
            payload.len(),
            sample.len()
        )
        .into());
    }
    Ok(())
}

fn pace_until(intended: Instant) {
    loop {
        let now = Instant::now();
        if now >= intended {
            break;
        }
        let wait = intended - now;
        if wait > Duration::from_millis(1) {
            thread::sleep(wait - Duration::from_micros(100));
        } else {
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{benchmark_config, block_size_for_payload, CROSSBAR_BLOCK_OVERHEAD};
    use std::time::Duration;

    #[test]
    fn block_size_always_leaves_room_for_crossbar_metadata() {
        assert_eq!(block_size_for_payload(64), 4_096);
        assert_eq!(
            block_size_for_payload(2 * 1024 * 1024),
            2 * 1024 * 1024 + CROSSBAR_BLOCK_OVERHEAD
        );
    }

    #[test]
    fn benchmark_config_uses_large_payload_block_sizing() {
        let cfg = benchmark_config(2 * 1024 * 1024);
        assert_eq!(
            cfg.block_size as usize,
            2 * 1024 * 1024 + CROSSBAR_BLOCK_OVERHEAD
        );
        assert_eq!(cfg.block_count, 16);
        assert_eq!(cfg.ring_depth, 8);
        assert_eq!(cfg.stale_timeout, Duration::from_secs(30));
    }

    #[test]
    fn benchmark_config_relaxes_stale_timeout_for_large_payloads() {
        assert_eq!(benchmark_config(64).stale_timeout, Duration::from_secs(5));
        assert_eq!(
            benchmark_config(1024 * 1024).stale_timeout,
            Duration::from_secs(15)
        );
        assert_eq!(
            benchmark_config(2 * 1024 * 1024).stale_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(
            benchmark_config(64 * 1024 * 1024).stale_timeout,
            Duration::from_secs(120)
        );
    }

    #[test]
    fn benchmark_config_scales_pool_for_huge_payloads() {
        assert_eq!(benchmark_config(16 * 1024 * 1024).block_count, 8);
        assert_eq!(benchmark_config(32 * 1024 * 1024).block_count, 16);
        assert_eq!(benchmark_config(64 * 1024 * 1024).block_count, 16);
    }
}
