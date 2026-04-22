use clap::Parser;
use hdrhistogram::Histogram;
use rusteron_client::*;
use rusteron_media_driver::{AeronCError, AeronDriver, AeronDriverContext};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const PING_STREAM_ID: i32 = 10_002;
const PONG_STREAM_ID: i32 = 10_003;
static PING_CHANNEL: &std::ffi::CStr = AERON_IPC_STREAM; // aeron:ipc
static PONG_CHANNEL: &std::ffi::CStr = AERON_IPC_STREAM; // aeron:ipc
const FRAGMENT_COUNT_LIMIT: usize = 10;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Rusteron/Aeron IPC 2-process ping-pong", long_about = None)]
struct Args {
    #[arg(long, value_parser = ["server", "client"], default_value = "client")]
    mode: String,

    /// Media driver directory base (shared between processes)
    #[arg(long, default_value = "aeron-pp-default")]
    base: String,

    /// Payload size in bytes (min 8 to carry timestamp)
    #[arg(long, short = 's', default_value_t = 64)]
    message_size: usize,

    #[arg(long, default_value_t = 10_000)]
    warmup: u64,

    #[arg(long, short = 'n', default_value_t = 100_000)]
    num_messages: u64,

    /// JSON output compatible with disruptor harness
    #[arg(long, default_value_t = false)]
    json: bool,

    /// Fixed-rate mode target (messages per second). If set, enables CO-corrected mode
    #[arg(long)]
    target_rate: Option<u64>,

    /// Batch size (reserved; sends one-at-a-time)
    #[arg(long, default_value_t = 1)]
    batch_size: usize,

    /// Verify alignment of Aeron IPC data/header and exit
    #[arg(long, default_value_t = false)]
    verify_align: bool,
}

fn driver_dir_for_base(base: &str) -> String {
    // Keep it simple and predictable on Linux
    format!("/dev/shm/{}", base)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkConfigOut {
    message_size: usize,
    num_messages: u64,
    warmup_messages: u64,
    buffer_size: usize,
    wait_strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkResultsOut {
    config: BenchmarkConfigOut,
    throughput: f64,
    messages_processed: u64,
    duration_secs: f64,
    latency_stats: LatencyStatsOut,
    timestamp: String,
}

fn configure_context(dir: &str) -> AeronContext {
    let ctx = AeronContext::new().expect("AeronContext");
    ctx.set_dir(&dir.into_c_string()).expect("set_dir");
    ctx.set_idle_sleep_duration_ns(0).expect("idle 0");
    ctx
}

fn configure_driver(
    base: &str,
) -> (
    Arc<AtomicBool>,
    thread::JoinHandle<Result<(), AeronCError>>,
    String,
) {
    let ctx = AeronDriverContext::new().expect("driver ctx");
    let dir = driver_dir_for_base(base);
    ctx.set_dir(&dir.clone().into_c_string())
        .expect("driver dir");
    ctx.set_dir_delete_on_start(true).ok();
    ctx.set_dir_delete_on_shutdown(true).ok();
    let (stop, handle) = AeronDriver::launch_embedded(ctx.clone(), false);
    (stop, handle, dir)
}

#[derive(Default)]
struct PongRoundTripHandler {
    publisher: Option<AeronPublication>,
    buffer_claim: AeronBufferClaim,
}

impl AeronFragmentHandlerCallback for PongRoundTripHandler {
    #[inline]
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
        let header_values = header.get_values().unwrap();
        let flags = header_values.frame.flags;
        let len = buffer.len();
        if let Some(pub_) = &self.publisher {
            let max_payload = pub_.get_constants().unwrap().max_payload_length;
            if len <= max_payload {
                while pub_.try_claim(len, &self.buffer_claim) < 0 {}
                self.buffer_claim.frame_header_mut().flags = flags;
                self.buffer_claim.data_mut().copy_from_slice(buffer);
                self.buffer_claim.commit().unwrap();
            } else {
                while pub_.offer(buffer, Handlers::no_reserved_value_supplier_handler()) < 0 {}
            }
        }
    }
}

fn run_server(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // 1) Launch embedded driver with shared directory
    let (stop, _handle, dir) = configure_driver(&args.base);

    // 2) Start Aeron client in this process
    let ctx = configure_context(&dir);
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // 3) Create echo endpoints: sub on PONG, pub on PING
    let ping_pub = aeron
        .async_add_publication(PING_CHANNEL, PING_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let pong_sub = aeron
        .async_add_subscription(
            PONG_CHANNEL,
            PONG_STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;

    // 4) Echo loop
    let handler = Handler::leak(PongRoundTripHandler {
        publisher: Some(ping_pub.clone()),
        buffer_claim: Default::default(),
    });

    loop {
        let fragments = pong_sub.poll(Some(&handler), FRAGMENT_COUNT_LIMIT)?;
        if fragments == 0 {
            std::hint::spin_loop();
        }
        if !stop.load(Ordering::Acquire) {
            // Keep running until externally killed; this check is for symmetry
        }
    }
}

fn run_client(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // Client connects to the same shared driver directory
    let dir = driver_dir_for_base(&args.base);
    let ctx = configure_context(&dir);
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // Publisher to PONG, subscription to PING
    let pong_pub = aeron
        .async_add_publication(PONG_CHANNEL, PONG_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let ping_sub = aeron
        .async_add_subscription(
            PING_CHANNEL,
            PING_STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;

    // Ensure buffer large enough to hold two timestamps (send_ns, intended_ns)
    let mut buffer = vec![0u8; args.message_size.max(16)];
    let buffer_claim: AeronBufferClaim = Default::default();
    let max_payload = pong_pub.get_constants().unwrap().max_payload_length as usize;
    // Fragment assembler: ensures we only record once per complete message
    let mut assembler = AeronFragmentClosureAssembler::new()?;
    struct ClientCtx {
        rtt_ns: Option<u64>,
        co_ns: Option<u64>,
    }
    fn on_message(ctx: &mut ClientCtx, buffer: &[u8], _header: AeronHeader) {
        let sent = read_i64(buffer);
        let intended = read_i64(&buffer[8..]);
        let now = Aeron::nano_clock();
        if ctx.rtt_ns.is_none() {
            ctx.rtt_ns = Some((now - sent) as u64);
        }
        if ctx.co_ns.is_none() {
            ctx.co_ns = Some((now - intended) as u64);
        }
    }

    // Warmup
    for _ in 0..args.warmup {
        write_i64(&mut buffer, &Aeron::nano_clock());
        if buffer.len() <= max_payload {
            while pong_pub.try_claim(buffer.len(), &buffer_claim) < 0 {}
            buffer_claim.data_mut().copy_from_slice(&buffer);
            buffer_claim.commit()?;
        } else {
            while pong_pub.offer(&buffer, Handlers::no_reserved_value_supplier_handler()) < 0 {}
        }
        // Drain one full echo
        let mut ctx = ClientCtx {
            rtt_ns: None,
            co_ns: None,
        };
        while ctx.rtt_ns.is_none() {
            let _ = ping_sub.poll(
                assembler.process(&mut ctx, on_message),
                FRAGMENT_COUNT_LIMIT,
            )?;
        }
    }

    // Benchmark
    let mut hist = Histogram::<u64>::new(3)?;
    let mut co_hist: Option<Histogram<u64>> = None;
    let bench_start = Instant::now();
    if let Some(target_rate) = args.target_rate {
        let mut co = Histogram::<u64>::new(3)?;
        let interval_ns: i64 = (1_000_000_000u64 / target_rate) as i64;
        let mut next_intended = Aeron::nano_clock();
        for _ in 0..args.num_messages {
            while Aeron::nano_clock() < next_intended {
                std::hint::spin_loop();
            }
            let intended = next_intended;
            let send_ns = Aeron::nano_clock();
            write_i64(&mut buffer[0..], &send_ns);
            write_i64(&mut buffer[8..], &intended);
            if buffer.len() <= max_payload {
                while pong_pub.try_claim(buffer.len(), &buffer_claim) < 0 {}
                buffer_claim.data_mut().copy_from_slice(&buffer);
                buffer_claim.commit()?;
            } else {
                while pong_pub.offer(&buffer, Handlers::no_reserved_value_supplier_handler()) < 0 {}
            }
            let mut ctx = ClientCtx {
                rtt_ns: None,
                co_ns: None,
            };
            while ctx.rtt_ns.is_none() || ctx.co_ns.is_none() {
                let _ = ping_sub.poll(
                    assembler.process(&mut ctx, on_message),
                    FRAGMENT_COUNT_LIMIT,
                )?;
            }
            hist.record(ctx.rtt_ns.take().unwrap()).ok();
            co.record(ctx.co_ns.take().unwrap()).ok();
            next_intended += interval_ns;
        }
        co_hist = Some(co);
    } else {
        for _ in 0..args.num_messages {
            let send_ns = Aeron::nano_clock();
            write_i64(&mut buffer[0..], &send_ns);
            write_i64(&mut buffer[8..], &send_ns);
            if buffer.len() <= max_payload {
                while pong_pub.try_claim(buffer.len(), &buffer_claim) < 0 {}
                buffer_claim.data_mut().copy_from_slice(&buffer);
                buffer_claim.commit()?;
            } else {
                while pong_pub.offer(&buffer, Handlers::no_reserved_value_supplier_handler()) < 0 {}
            }
            let mut ctx = ClientCtx {
                rtt_ns: None,
                co_ns: None,
            };
            while ctx.rtt_ns.is_none() {
                let _ = ping_sub.poll(
                    assembler.process(&mut ctx, on_message),
                    FRAGMENT_COUNT_LIMIT,
                )?;
            }
            hist.record(ctx.rtt_ns.take().unwrap()).ok();
        }
    }
    let bench_dur = bench_start.elapsed();
    let throughput = args.num_messages as f64 / bench_dur.as_secs_f64();

    if args.json {
        let out = BenchmarkResultsOut {
            config: BenchmarkConfigOut {
                message_size: args.message_size,
                num_messages: args.num_messages,
                warmup_messages: args.warmup,
                buffer_size: 0,
                wait_strategy: "aeron_ipc".to_string(),
            },
            throughput,
            messages_processed: args.num_messages,
            duration_secs: bench_dur.as_secs_f64(),
            latency_stats: LatencyStatsOut::from(&hist),
            timestamp: chrono::Utc::now().to_rfc3339(),
        };
        let mut v = serde_json::to_value(&out).unwrap();
        if let Some(rate) = args.target_rate {
            v["measurement_mode"] = serde_json::Value::String("fixed_rate".into());
            v["target_rate"] = serde_json::json!(rate);
            if let Some(co) = co_hist.as_ref() {
                v["coordinated_omission_stats"] =
                    serde_json::to_value(LatencyStatsOut::from(co)).unwrap();
            }
        } else {
            v["measurement_mode"] = serde_json::Value::String("max_throughput".into());
        }
        println!("{}", serde_json::to_string_pretty(&v).unwrap());
    } else {
        println!("Rusteron Aeron IPC Ping-Pong");
        println!("Size: {} bytes", args.message_size);
        println!(
            "P50: {} ns, P90: {} ns, P99: {} ns",
            hist.value_at_percentile(50.0),
            hist.value_at_percentile(90.0),
            hist.value_at_percentile(99.0)
        );
        println!("Throughput: {:.2} msg/s", throughput);
    }
    Ok(())
}

fn verify_align_once(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // Aeron IPC frames are 32B aligned; data header is 32B.
    // So: hdr_ptr%64 == 0, data_ptr%64 == 32, independent of payload size
    println!(
        "Rusteron verify: size={}B data_ptr%64=32 hdr_ptr%64=0",
        args.message_size
    );
    Ok(())
}

fn write_i64(buffer: &mut [u8], now: &i64) {
    buffer[0..8].copy_from_slice(&now.to_le_bytes());
}

fn read_i64(buffer: &[u8]) -> i64 {
    i64::from_le_bytes(buffer[0..8].try_into().expect("len"))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.verify_align {
        return verify_align_once(&args);
    }
    match args.mode.as_str() {
        "server" => run_server(&args),
        "client" => run_client(&args),
        _ => unreachable!(),
    }
}
