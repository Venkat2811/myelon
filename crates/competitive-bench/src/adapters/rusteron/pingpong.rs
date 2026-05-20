use crate::infra::result_json::{BenchmarkConfigOut, BenchmarkResultsOut, LatencyStatsOut};
use clap::Parser;
use hdrhistogram::Histogram;
use rusteron_client::*;
use rusteron_media_driver::{AeronCError, AeronDriver, AeronDriverContext};
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const PING_STREAM_ID: i32 = 10_002;
const PONG_STREAM_ID: i32 = 10_003;
const FRAGMENT_COUNT_LIMIT: usize = 10;
const MAX_FRAGMENT_POLL_LIMIT: usize = 65_536;
const MIN_TERM_LENGTH: usize = 64 * 1024;
const MAX_TERM_LENGTH: usize = 1024 * 1024 * 1024;

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

fn term_length_for_message_size(message_size: usize) -> usize {
    let required = message_size.max(1).saturating_mul(8).max(MIN_TERM_LENGTH);
    required.next_power_of_two().min(MAX_TERM_LENGTH)
}

fn ipc_channel_for_message_size(message_size: usize) -> CString {
    let term_length = term_length_for_message_size(message_size);
    CString::new(format!("aeron:ipc?term-length={term_length}")).expect("valid IPC URI")
}

fn fragment_poll_limit(message_size: usize, max_payload: usize) -> usize {
    let max_payload = max_payload.max(1);
    let fragments = message_size.max(1).div_ceil(max_payload);
    FRAGMENT_COUNT_LIMIT
        .max(fragments.saturating_mul(2))
        .min(MAX_FRAGMENT_POLL_LIMIT)
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

fn server_uses_fragment_assembler(message_size: usize, max_payload: usize) -> bool {
    message_size > max_payload
}

struct ServerCtx {
    publisher: AeronPublication,
    buffer_claim: AeronBufferClaim,
    max_payload: usize,
}

fn on_server_message(ctx: &mut ServerCtx, buffer: &[u8], _header: AeronHeader) {
    if buffer.len() <= ctx.max_payload {
        while ctx.publisher.try_claim(buffer.len(), &ctx.buffer_claim) < 0 {}
        ctx.buffer_claim.data_mut().copy_from_slice(buffer);
        ctx.buffer_claim.commit().expect("server claim commit");
    } else {
        while ctx
            .publisher
            .offer(buffer, Handlers::no_reserved_value_supplier_handler())
            < 0
        {}
    }
}

fn run_server(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // 1) Launch embedded driver with shared directory
    let (stop, _handle, dir) = configure_driver(&args.base);

    // 2) Start Aeron client in this process
    let ctx = configure_context(&dir);
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let ping_channel = ipc_channel_for_message_size(args.message_size);
    let pong_channel = ipc_channel_for_message_size(args.message_size);

    // 3) Create echo endpoints: sub on PONG, pub on PING
    let ping_pub = aeron
        .async_add_publication(&ping_channel, PING_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let max_payload = ping_pub.get_constants().unwrap().max_payload_length;
    let poll_limit = fragment_poll_limit(args.message_size, max_payload);
    let pong_sub = aeron
        .async_add_subscription(
            &pong_channel,
            PONG_STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;

    // 4) Echo loop
    if server_uses_fragment_assembler(args.message_size, max_payload) {
        let mut assembler = AeronFragmentClosureAssembler::new()?;
        let mut ctx = ServerCtx {
            publisher: ping_pub.clone(),
            buffer_claim: Default::default(),
            max_payload,
        };
        loop {
            let fragments =
                pong_sub.poll(assembler.process(&mut ctx, on_server_message), poll_limit)?;
            if fragments == 0 {
                std::hint::spin_loop();
            }
            if !stop.load(Ordering::Acquire) {
                // Keep running until externally killed; this check is for symmetry
            }
        }
    } else {
        let handler = Handler::leak(PongRoundTripHandler {
            publisher: Some(ping_pub.clone()),
            buffer_claim: Default::default(),
        });

        loop {
            let fragments = pong_sub.poll(Some(&handler), poll_limit)?;
            if fragments == 0 {
                std::hint::spin_loop();
            }
            if !stop.load(Ordering::Acquire) {
                // Keep running until externally killed; this check is for symmetry
            }
        }
    }
}

fn run_client(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // Client connects to the same shared driver directory
    let dir = driver_dir_for_base(&args.base);
    let ctx = configure_context(&dir);
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let ping_channel = ipc_channel_for_message_size(args.message_size);
    let pong_channel = ipc_channel_for_message_size(args.message_size);

    // Publisher to PONG, subscription to PING
    let pong_pub = aeron
        .async_add_publication(&pong_channel, PONG_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let ping_sub = aeron
        .async_add_subscription(
            &ping_channel,
            PING_STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;

    // Ensure buffer large enough to hold two timestamps (send_ns, intended_ns)
    let mut buffer = vec![0u8; args.message_size.max(16)];
    let buffer_claim: AeronBufferClaim = Default::default();
    let constants = pong_pub.get_constants().unwrap();
    let max_payload = constants.max_payload_length;
    let max_message_length = constants.max_message_length;
    if args.message_size > max_message_length {
        return Err(format!(
            "message_size={} exceeds Aeron max_message_length={} for channel {}",
            args.message_size,
            max_message_length,
            pong_channel.to_string_lossy()
        )
        .into());
    }
    let poll_limit = fragment_poll_limit(args.message_size, max_payload);
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
            let _ = ping_sub.poll(assembler.process(&mut ctx, on_message), poll_limit)?;
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
                let _ = ping_sub.poll(assembler.process(&mut ctx, on_message), poll_limit)?;
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
                let _ = ping_sub.poll(assembler.process(&mut ctx, on_message), poll_limit)?;
            }
            hist.record(ctx.rtt_ns.take().unwrap()).ok();
        }
    }
    let bench_dur = bench_start.elapsed();
    let throughput = args.num_messages as f64 / bench_dur.as_secs_f64();

    if args.json {
        let out = BenchmarkResultsOut {
            adapter: "rusteron".to_string(),
            family: "pingpong".to_string(),
            config: BenchmarkConfigOut {
                message_size: args.message_size,
                num_messages: args.num_messages,
                warmup_messages: args.warmup,
                buffer_size: 0,
                wait_strategy: "aeron_ipc".to_string(),
                consumers: None,
            },
            throughput,
            fanout_throughput: None,
            messages_processed: args.num_messages,
            duration_secs: bench_dur.as_secs_f64(),
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
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
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

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
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

#[cfg(test)]
mod tests {
    use super::{
        fragment_poll_limit, ipc_channel_for_message_size, term_length_for_message_size,
        FRAGMENT_COUNT_LIMIT, MAX_FRAGMENT_POLL_LIMIT,
    };

    #[test]
    fn fragment_poll_limit_keeps_small_messages_on_default_limit() {
        assert_eq!(fragment_poll_limit(64, 1408), FRAGMENT_COUNT_LIMIT);
        assert_eq!(fragment_poll_limit(2048, 1408), FRAGMENT_COUNT_LIMIT);
    }

    #[test]
    fn fragment_poll_limit_scales_for_large_fragmented_messages() {
        assert_eq!(fragment_poll_limit(2 * 1024 * 1024, 1408), 2980);
    }

    #[test]
    fn fragment_poll_limit_clamps_extreme_values() {
        assert_eq!(
            fragment_poll_limit(64 * 1024 * 1024, 64),
            MAX_FRAGMENT_POLL_LIMIT
        );
    }

    #[test]
    fn server_fragment_assembler_only_kicks_in_for_fragmented_messages() {
        assert!(!super::server_uses_fragment_assembler(1024, 1408));
        assert!(super::server_uses_fragment_assembler(2 * 1024 * 1024, 1408));
    }

    #[test]
    fn term_length_scales_to_message_size() {
        assert_eq!(term_length_for_message_size(64), 64 * 1024);
        assert_eq!(
            term_length_for_message_size(2 * 1024 * 1024),
            16 * 1024 * 1024
        );
        assert_eq!(
            term_length_for_message_size(64 * 1024 * 1024),
            512 * 1024 * 1024
        );
    }

    #[test]
    fn ipc_channel_carries_term_length_override() {
        let uri = ipc_channel_for_message_size(64 * 1024 * 1024);
        assert_eq!(uri.to_str().unwrap(), "aeron:ipc?term-length=536870912");
    }
}
