use clap::Parser;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use shmipc::{
    config::SizePercentPair,
    consts::MemMapType,
    session::{SessionManager, SessionManagerConfig},
    stream::Stream,
    transport::{DefaultUnixConnect, DefaultUnixListen},
    Listener,
};
use std::os::unix::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::{io::Result as TokioResult, sync::oneshot};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "shmipc-rs 2-process ping-pong benchmark", long_about = None)]
struct Args {
    /// Mode: server echoes payloads; client measures RTT
    #[arg(long, value_parser = ["server", "client"], default_value = "client")]
    mode: String,

    /// Unique base name (used to derive UDS path and shm prefixes)
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

    /// Batch size (reserved; currently sends one-at-a-time)
    #[arg(long, default_value_t = 1)]
    batch_size: usize,

    /// Do not spawn an embedded server in client mode (for strict split runs)
    #[arg(long, default_value_t = false)]
    no_embedded_server: bool,
}

fn default_base_name() -> String {
    format!("shmipc_pp_{}", std::process::id())
}

fn uds_path(base: &str) -> String {
    if cfg!(target_os = "macos") {
        format!("/tmp/{}.sock", base)
    } else {
        format!("/dev/shm/{}.sock", base)
    }
}

fn benchmark_config(base: &str, message_size: u32) -> SessionManagerConfig {
    let mut c = SessionManagerConfig::new();
    // Match upstream bench defaults
    c.config_mut().queue_cap = if message_size >= 32 * 1024 * 1024 {
        4
    } else if message_size >= 8 * 1024 * 1024 {
        16
    } else {
        65_536
    };
    c.config_mut().connection_write_timeout = Duration::from_secs(1);
    let minimum_cap = 256usize << 20;
    let requested_cap = (message_size as usize).saturating_mul(4);
    let cap = minimum_cap.max(requested_cap).min(u32::MAX as usize) as u32;
    c.config_mut().share_memory_buffer_cap = cap;
    c.config_mut().mem_map_type = MemMapType::MemMapTypeMemFd;
    // Ensure unique shm prefix per run
    c.config_mut().share_memory_path_prefix.push_str(base);
    c
}

fn configured_buffer_slice_sizes(message_size: u32) -> Vec<SizePercentPair> {
    if message_size >= 1024 * 1024 {
        vec![SizePercentPair {
            size: message_size + 256,
            percent: 100,
        }]
    } else {
        vec![
            SizePercentPair {
                size: message_size + 256,
                percent: 70,
            },
            SizePercentPair {
                size: (16 << 10) + 256,
                percent: 20,
            },
            SizePercentPair {
                size: (64 << 10) + 256,
                percent: 10,
            },
        ]
    }
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> TokioResult<()> {
    let args = Args::parse();

    match args.mode.as_str() {
        "server" => run_server(&args).await?,
        "client" => run_client(&args).await?,
        _ => unreachable!(),
    }

    Ok(())
}

async fn run_server(args: &Args) -> TokioResult<()> {
    let base = &args.base;
    let path = uds_path(base);
    let mut cfg = benchmark_config(base, args.message_size);

    // Size buckets similar to upstream bench: favor requested size
    cfg.config_mut().buffer_slice_sizes = configured_buffer_slice_sizes(args.message_size);

    // Clean any stale socket path
    let _ = std::fs::remove_file(&path);

    let mut listener = Listener::new(
        DefaultUnixListen,
        SocketAddr::from_pathname(path.clone()).unwrap(),
        cfg.config().clone(),
    )
    .await
    .expect("failed to bind shmipc listener");

    // Server accepts one connection and echoes back payloads forever
    loop {
        let mut stream = listener.accept().await?;
        let message_size = args.message_size;
        tokio::spawn(async move {
            loop {
                match must_read(&mut stream, message_size).await {
                    Ok(false) => {
                        let _ = stream.close().await;
                        return;
                    }
                    Ok(true) => {
                        stream.release_read_and_reuse();
                        if must_write(&mut stream, message_size).await.is_err() {
                            let _ = stream.close().await;
                            return;
                        }
                    }
                    Err(_) => {
                        let _ = stream.close().await;
                        return;
                    }
                }
            }
        });
    }
}

async fn run_client(args: &Args) -> TokioResult<()> {
    let base = &args.base;
    let path = uds_path(base);
    let mut cfg = benchmark_config(base, args.message_size);

    cfg.config_mut().buffer_slice_sizes = configured_buffer_slice_sizes(args.message_size);

    // Optionally skip embedded server when running split mode
    if !args.no_embedded_server {
        let (tx_ready, rx_ready) = oneshot::channel();
        let args_clone = args.clone();
        tokio::spawn(async move {
            // Fire and forget server
            let _ = tx_ready.send(());
            let _ = run_server(&args_clone).await;
        });
        let _ = rx_ready.await;
    }

    // Connect client with brief retry to tolerate server startup
    let addr = SocketAddr::from_pathname(path.clone()).unwrap();
    let mut client_opt = None;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match SessionManager::new(cfg.clone(), DefaultUnixConnect, addr.clone()).await {
            Ok(c) => {
                client_opt = Some(c);
                break;
            }
            Err(_) => {
                if Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let client = client_opt.expect("failed to connect shmipc session");
    let mut stream = client.get_stream().expect("get_stream failed");

    // Warmup
    for _ in 0..args.warmup {
        must_write(&mut stream, args.message_size).await?;
        let _ = must_read(&mut stream, args.message_size).await?;
        stream.release_read_and_reuse();
    }

    let mut hist = Histogram::<u64>::new(3).expect("hist");
    let mut co_hist: Option<Histogram<u64>> = None;

    let start = Instant::now();
    if let Some(target_rate) = args.target_rate {
        // Fixed-rate mode (CO-corrected) using absolute scheduling to avoid drift
        let mut co = Histogram::<u64>::new(3).expect("co_hist");
        let interval = Duration::from_nanos(1_000_000_000u64 / target_rate);
        let base = Instant::now();
        for i in 0..args.num_messages {
            let intended = base + interval * (i as u32);
            // Pace to intended
            loop {
                let now = Instant::now();
                if now >= intended {
                    break;
                }
                let wait = intended - now;
                if wait > Duration::from_millis(1) {
                    tokio::time::sleep(wait - Duration::from_micros(100)).await;
                } else {
                    std::hint::spin_loop();
                }
            }
            let t0 = Instant::now();
            must_write(&mut stream, args.message_size).await?;
            let _ = must_read(&mut stream, args.message_size).await?;
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
            must_write(&mut stream, args.message_size).await?;
            let _ = must_read(&mut stream, args.message_size).await?;
            stream.release_read_and_reuse();
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
                wait_strategy: "library".to_string(),
            },
            throughput,
            messages_processed: args.num_messages,
            duration_secs: duration.as_secs_f64(),
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
        println!("shmipc-rs Ping-Pong");
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

    stream
        .close()
        .await
        .map_err(|e| std::io::Error::other(format!("close err: {e}")))?;
    client.close().await;

    Ok(())
}

async fn must_write(s: &mut Stream, size: u32) -> TokioResult<()> {
    if size > 0 {
        let buf = s
            .reserve(size as usize)
            .map_err(|e| std::io::Error::other(format!("reserve err: {e}")))?;
        buf.fill(0);
    }
    loop {
        match s.flush(false).await {
            Err(e) => match e {
                shmipc::Error::QueueFull => {
                    // backoff briefly
                    tokio::time::sleep(Duration::from_micros(1)).await;
                    continue;
                }
                _ => return Err(std::io::Error::other(format!("flush err: {}", e))),
            },
            Ok(_) => return Ok(()),
        }
    }
}

async fn must_read(s: &mut Stream, size: u32) -> TokioResult<bool> {
    match s.discard(size as usize).await {
        Err(e) => match e {
            shmipc::Error::StreamClosed | shmipc::Error::EndOfStream => Ok(false),
            _ => Err(std::io::Error::other(format!("read err: {}", e))),
        },
        Ok(_) => Ok(true),
    }
}
