use clap::Parser;
use crate::infra::result_json::{BenchmarkConfigOut, BenchmarkResultsOut, LatencyStatsOut};
use hdrhistogram::Histogram;
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

fn ipc_dir() -> &'static str {
    if cfg!(target_os = "macos") {
        "/tmp"
    } else {
        "/dev/shm"
    }
}

fn endpoint(base: &str, transport: &str) -> String {
    match transport {
        "ipc" => format!("ipc://{}/{}.zmq", ipc_dir(), base),
        "ipc-abs" => {
            if cfg!(target_os = "linux") {
                format!("ipc://@{}", base) // Linux abstract UDS
            } else {
                // Abstract namespace not available on macOS — fall back to filesystem UDS
                format!("ipc://{}/{}.abs.zmq", ipc_dir(), base)
            }
        }
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
    format!("{}/{}.zmq", ipc_dir(), base)
}

pub fn main() -> AnyResult<()> {
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
        let adapter_name = match args.transport.as_str() {
            "tcp" => "zeromq-tcp",
            "ipc-abs" => "zeromq-ipc-abs",
            _ => "zeromq-ipc",
        };
        let out = BenchmarkResultsOut {
            adapter: adapter_name.to_string(),
            family: "pingpong".to_string(),
            config: BenchmarkConfigOut {
                message_size: args.message_size as usize,
                num_messages: args.num_messages,
                warmup_messages: args.warmup,
                buffer_size: 0,
                wait_strategy: "zmq".to_string(),
                consumers: None,
            },
            throughput,
            fanout_throughput: None,
            messages_processed: args.num_messages,
            duration_secs: duration.as_secs_f64(),
            publish_duration_secs: None,
            latency_stats: LatencyStatsOut::from(&hist),
            timestamp: chrono::Utc::now().to_rfc3339(),
            verification_passed: None,
            measurement_mode: Some(
                if args.target_rate.is_some() {
                    "fixed_rate"
                } else {
                    "max_throughput"
                }
                .to_string(),
            ),
            target_rate: args.target_rate,
            consumer_count: None,
            coordinated_omission_stats: co_hist.as_ref().map(LatencyStatsOut::from),
        };
        println!("{}", serde_json::to_string_pretty(&out)?);
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
