#[cfg(unix)]
mod unix_impl {
    use chrono::Utc;
    use clap::Parser;
    use competitive_bench::pingpong_support::{
        ensure_message_size, interval_for_rate, pace_until, parse_payload, payload, scoped_name,
    };
    use competitive_bench::result_json::{BenchmarkConfigOut, BenchmarkResultsOut, LatencyStatsOut};
    use hdrhistogram::Histogram;
    use iceoryx2::prelude::*;
    use iceoryx2::port::subscriber::Subscriber;
    use iceoryx2::service::port_factory::publish_subscribe::PortFactory;
    use std::error::Error;
    use std::time::{Duration, Instant};

    type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

    #[derive(Parser, Debug, Clone)]
    #[command(author, version, about = "iceoryx2 2-process ping-pong benchmark", long_about = None)]
    struct Args {
        #[arg(long, value_parser = ["server", "client"], default_value = "client")]
        mode: String,

        #[arg(long, default_value_t = default_base_name())]
        base: String,

        #[arg(long, short = 's', default_value_t = 64)]
        message_size: usize,

        #[arg(long, default_value_t = 10_000)]
        warmup: u64,

        #[arg(long, short = 'n', default_value_t = 100_000)]
        num_messages: u64,

        #[arg(long, default_value_t = false)]
        json: bool,

        #[arg(long)]
        target_rate: Option<u64>,

        #[arg(long, default_value_t = 5_000)]
        timeout_ms: u64,
    }

    fn default_base_name() -> String {
        format!("iceoryx2_pp_{}", std::process::id())
    }

    pub fn main() -> AnyResult<()> {
        let args = Args::parse();
        ensure_message_size(args.message_size)?;
        match args.mode.as_str() {
            "server" => run_server(&args),
            "client" => run_client(&args),
            _ => unreachable!(),
        }
    }

    fn service_name(base: &str, suffix: &str) -> AnyResult<ServiceName> {
        Ok(scoped_name("ix2", base, suffix).as_str().try_into()?)
    }

    fn next_payload(
        subscriber: &Subscriber<ipc::Service, [u8], ()>,
        timeout_ms: u64,
    ) -> AnyResult<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            match subscriber.receive()? {
                Some(sample) => return Ok(sample[..].to_vec()),
                None => {
                    if Instant::now() >= deadline {
                        return Err(format!("iceoryx2 timed out after {timeout_ms}ms").into());
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }

    fn service_factory(
        node: &Node<ipc::Service>,
        name: &ServiceName,
    ) -> AnyResult<PortFactory<ipc::Service, [u8], ()>> {
        Ok(node
            .service_builder(name)
            .publish_subscribe::<[u8]>()
            .enable_safe_overflow(true)
            .max_publishers(1)
            .max_subscribers(1)
            .open_or_create()?)
    }

    fn run_server(args: &Args) -> AnyResult<()> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let request_name = service_name(&args.base, "request")?;
        let reply_name = service_name(&args.base, "reply")?;
        let request_service = service_factory(&node, &request_name)?;
        let reply_service = service_factory(&node, &reply_name)?;
        let request_subscriber = request_service.subscriber_builder().create()?;
        let reply_publisher = reply_service
            .publisher_builder()
            .initial_max_slice_len(args.message_size)
            .create()?;

        loop {
            let incoming = next_payload(&request_subscriber, args.timeout_ms)?;
            let sample = reply_publisher.loan_slice_uninit(incoming.len())?;
            let sample = sample.write_from_slice(&incoming);
            sample.send()?;
        }
    }

    fn run_client(args: &Args) -> AnyResult<()> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        let request_name = service_name(&args.base, "request")?;
        let reply_name = service_name(&args.base, "reply")?;
        let request_service = service_factory(&node, &request_name)?;
        let reply_service = service_factory(&node, &reply_name)?;
        let request_publisher = request_service
            .publisher_builder()
            .initial_max_slice_len(args.message_size)
            .create()?;
        let reply_subscriber = reply_service.subscriber_builder().create()?;

        for sequence in 0..args.warmup {
            let sample = request_publisher.loan_slice_uninit(args.message_size)?;
            let sample = sample.write_from_slice(&payload(sequence, 0, 0, args.message_size));
            sample.send()?;
            let _ = next_payload(&reply_subscriber, args.timeout_ms)?;
        }

        let mut throughput_hist = Histogram::<u64>::new(3)?;
        let mut co_hist = if args.target_rate.is_some() {
            Some(Histogram::<u64>::new(3)?)
        } else {
            None
        };

        let base = Instant::now();
        let start = base;
        let interval = match args.target_rate {
            Some(rate) => Some(interval_for_rate(rate)?),
            None => None,
        };

        for sequence in 0..args.num_messages {
            let intended_instant = interval.map(|step| base + step.mul_f64(sequence as f64));
            if let Some(intended) = intended_instant {
                pace_until(intended);
            }

            let send_instant = Instant::now();
            let send_time_ns = send_instant.duration_since(base).as_nanos() as u64;
            let intended_send_time_ns = intended_instant
                .map(|planned| planned.duration_since(base).as_nanos() as u64)
                .unwrap_or(send_time_ns);
            let outbound = payload(
                sequence,
                send_time_ns,
                intended_send_time_ns,
                args.message_size,
            );
            let sample = request_publisher.loan_slice_uninit(outbound.len())?;
            let sample = sample.write_from_slice(&outbound);
            sample.send()?;

            let echoed = next_payload(&reply_subscriber, args.timeout_ms)?;
            let (_echoed_sequence, _echoed_send_ns, echoed_intended_ns) = parse_payload(&echoed)?;
            let rtt_ns = send_instant.elapsed().as_nanos() as u64;
            throughput_hist.record(rtt_ns).ok();
            if let Some(hist) = co_hist.as_mut() {
                let recv_from_base_ns = Instant::now().duration_since(base).as_nanos() as u64;
                hist.record(recv_from_base_ns.saturating_sub(echoed_intended_ns))
                    .ok();
            }
        }

        let duration = start.elapsed();
        let throughput = args.num_messages as f64 / duration.as_secs_f64();
        emit_results(args, throughput, duration, &throughput_hist, co_hist.as_ref())
    }

    fn emit_results(
        args: &Args,
        throughput: f64,
        duration: Duration,
        latency_hist: &Histogram<u64>,
        co_hist: Option<&Histogram<u64>>,
    ) -> AnyResult<()> {
        if !args.json {
            println!(
                "iceoryx2-shm size={} throughput={:.2} latency_p50={}ns latency_p99={}ns",
                args.message_size,
                throughput,
                latency_hist.value_at_percentile(50.0),
                latency_hist.value_at_percentile(99.0)
            );
            return Ok(());
        }

        let out = BenchmarkResultsOut {
            adapter: "iceoryx2-shm".to_string(),
            family: "pingpong".to_string(),
            config: BenchmarkConfigOut {
                message_size: args.message_size,
                num_messages: args.num_messages,
                warmup_messages: args.warmup,
                buffer_size: args.message_size,
                wait_strategy: "busy_spin".to_string(),
                consumers: None,
            },
            throughput,
            fanout_throughput: None,
            messages_processed: args.num_messages,
            duration_secs: duration.as_secs_f64(),
            publish_duration_secs: None,
            latency_stats: LatencyStatsOut::from(latency_hist),
            timestamp: Utc::now().to_rfc3339(),
            verification_passed: None,
            measurement_mode: args.target_rate.map(|_| "fixed_rate".to_string()),
            target_rate: args.target_rate,
            consumer_count: None,
        };

        let mut value = serde_json::to_value(&out)?;
        if let Some(hist) = co_hist {
            value["coordinated_omission_stats"] = serde_json::to_value(LatencyStatsOut::from(hist))?;
        }
        println!("{}", serde_json::to_string_pretty(&value)?);
        Ok(())
    }
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    unix_impl::main()
}

#[cfg(not(unix))]
fn main() {
    eprintln!("iceoryx2_pingpong is only supported on unix targets");
    std::process::exit(1);
}
