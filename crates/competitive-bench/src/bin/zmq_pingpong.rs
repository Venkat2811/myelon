use clap::Parser;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::error::Error;
use std::fs;
use std::thread;
use std::time::{Duration, Instant};

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "ZeroMQ 2-process ping-pong benchmark", long_about = None)]
struct Args {
    /// Mode: server echoes payloads; client measures RTT
    #[arg(long, value_parser = ["server", "client"], default_value = "client")]
    mode: String,

    /// Unique base name (used to derive IPC endpoint)
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

    /// Output JSON compatible with disruptor harness
    #[arg(long, default_value_t = false)]
    json: bool,

    /// Fixed-rate mode target (messages per second). If set, enables CO-corrected mode
    #[arg(long)]
    target_rate: Option<u64>,

    /// Transport: ipc | ipc-abs | tcp
    #[arg(long, default_value = "ipc")]
    transport: String,
}

fn default_base_name() -> String {
    format!("zmq_pp_{}", std::process::id())
}

fn endpoint(base: &str, transport: &str) -> String {
    match transport {
        "ipc" => format!("ipc:///dev/shm/{}.zmq", base),
        "ipc-abs" => format!("ipc://@{}", base), // Linux abstract UDS
        "tcp" => {
            // Stable pseudo-random port derived from base
            let sum: u32 = base.as_bytes().iter().map(|b| *b as u32).sum();
            let port = 35000 + (sum % 2000) as u16; // 35000..36999
            format!("tcp://127.0.0.1:{}", port)
        }
        other => panic!("unsupported transport: {}", other),
    }
}

fn endpoint_path(base: &str) -> String {
    format!("/dev/shm/{}.zmq", base)
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
    config: BenchmarkConfigOut,
    throughput: f64,
    messages_processed: u64,
    duration_secs: f64,
    latency_stats: LatencyStatsOut,
    timestamp: String,
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
    let ctx = zmq::Context::new();
    let ep = endpoint(&args.base, &args.transport);
    let path = endpoint_path(&args.base);
    let _ = fs::remove_file(&path);

    let socket = ctx.socket(zmq::PAIR)?;
    socket.set_linger(0)?;
    socket.set_immediate(true)?;
    socket.set_rcvhwm(0)?;
    socket.set_sndhwm(0)?;
    socket.bind(&ep)?;

    let mut msg = zmq::Message::new();
    loop {
        socket.recv(&mut msg, 0)?;
        socket.send(&*msg, 0)?;
    }
}

fn run_client(args: &Args) -> AnyResult<()> {
    if let Some(rate) = args.target_rate {
        if rate == 0 {
            return Err("target_rate must be > 0".into());
        }
    }

    let ctx = zmq::Context::new();
    let ep = endpoint(&args.base, &args.transport);
    let socket = ctx.socket(zmq::PAIR)?;
    socket.set_linger(0)?;
    socket.set_immediate(true)?;
    socket.set_rcvhwm(0)?;
    socket.set_sndhwm(0)?;
    socket.connect(&ep)?;

    let payload = vec![0_u8; args.message_size as usize];
    let mut recv_msg = zmq::Message::new();

    // Warmup
    for _ in 0..args.warmup {
        socket.send(&payload, 0)?;
        socket.recv(&mut recv_msg, 0)?;
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
            if i == 0 {
                intended = base;
            } else {
                intended += interval;
            }
            // Absolute pacing to avoid drift
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
            let t0 = Instant::now();
            socket.send(&payload, 0)?;
            socket.recv(&mut recv_msg, 0)?;
            let recv = Instant::now();
            let rtt = recv.duration_since(t0).as_nanos() as u64;
            let co_lat = recv.duration_since(intended).as_nanos() as u64;
            hist.record(rtt).ok();
            co.record(co_lat).ok();
        }
        co_hist = Some(co);
    } else {
        for _ in 0..args.num_messages {
            let t0 = Instant::now();
            socket.send(&payload, 0)?;
            socket.recv(&mut recv_msg, 0)?;
            let rtt = t0.elapsed().as_nanos() as u64;
            hist.record(rtt).ok();
        }
    }
    let duration = start.elapsed();
    let throughput = args.num_messages as f64 / duration.as_secs_f64();

    if args.json {
        let out = BenchmarkResultsOut {
            config: BenchmarkConfigOut {
                message_size: args.message_size as usize,
                num_messages: args.num_messages,
                warmup_messages: args.warmup,
                buffer_size: 0,
                wait_strategy: "zmq".to_string(),
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
        println!("zmq Ping-Pong");
        println!("Size: {} bytes", args.message_size);
        println!("Throughput: {:.2} msg/s", throughput);
        println!(
            "P50: {} ns, P90: {} ns, P99: {} ns",
            hist.value_at_percentile(50.0),
            hist.value_at_percentile(90.0),
            hist.value_at_percentile(99.0),
        );
        if let Some(co) = co_hist.as_ref() {
            println!("CO-corrected P50: {} ns", co.value_at_percentile(50.0));
        }
    }

    Ok(())
}
