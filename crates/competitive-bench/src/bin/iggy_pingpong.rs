use bytes::Bytes;
use clap::Parser;
use competitive_bench::pingpong_support::{
    ensure_message_size, interval_for_rate, pace_until, parse_payload, payload, scoped_name,
};
use competitive_bench::result_json::{BenchmarkConfigOut, BenchmarkResultsOut, LatencyStatsOut};
use hdrhistogram::Histogram;
use iggy::prelude::{
    Client, CompressionAlgorithm, Consumer, Identifier, IggyClient, IggyExpiry, IggyMessage,
    MaxTopicSize, MessageClient, Partitioning, PollingStrategy, StreamClient, TopicClient,
};
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Apache Iggy TCP brokered 2-process ping-pong benchmark")]
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

    #[arg(long, default_value = "127.0.0.1:8090")]
    address: String,

    #[arg(long, default_value = "iggy")]
    username: String,

    #[arg(long, default_value = "iggy")]
    password: String,

    #[arg(long, default_value_t = 120)]
    connect_retries: u32,

    #[arg(long, default_value_t = 500)]
    connect_retry_ms: u64,

    #[arg(long, default_value_t = 30_000)]
    poll_timeout_ms: u64,
}

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
struct TopicEndpoint {
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: u32,
}

fn default_base_name() -> String {
    format!("iggy_pp_{}", std::process::id())
}

fn connection_string(args: &Args) -> String {
    format!("iggy://{}:{}@{}", args.username, args.password, args.address)
}

fn stream_name(args: &Args) -> String {
    scoped_name("cbi", &args.base, "stream")
}

fn request_topic(args: &Args) -> String {
    scoped_name("cbi", &args.base, "request")
}

fn reply_topic(args: &Args) -> String {
    scoped_name("cbi", &args.base, "reply")
}

fn consumer_id(args: &Args, role: &str) -> AnyResult<Consumer> {
    Ok(Consumer::new(Identifier::named(&scoped_name("cbi", &args.base, role))?))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> AnyResult<()> {
    let args = Args::parse();
    ensure_message_size(args.message_size)?;

    match args.mode.as_str() {
        "server" => run_server(&args).await,
        "client" => run_client(&args).await,
        _ => unreachable!(),
    }
}

async fn connect_client(args: &Args) -> AnyResult<IggyClient> {
    let connection = connection_string(args);
    let mut last_error = None;
    for _attempt in 0..args.connect_retries {
        let client = IggyClient::from_connection_string(&connection)?;
        match client.connect().await {
            Ok(()) => return Ok(client),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(args.connect_retry_ms)).await;
            }
        }
    }

    Err(format!(
        "failed to connect to Iggy at {} after {} attempts: {:?}",
        args.address, args.connect_retries, last_error
    )
    .into())
}

async fn ensure_stream_and_topics(
    client: &IggyClient,
    stream: &str,
    request: &str,
    reply: &str,
) -> AnyResult<(TopicEndpoint, TopicEndpoint)> {
    let stream_id = Identifier::named(stream)?;
    if client.get_stream(&stream_id).await?.is_none() {
        if let Err(error) = client.create_stream(stream).await {
            if client.get_stream(&stream_id).await?.is_none() {
                return Err(Box::new(error));
            }
        }
    }

    let request_endpoint = ensure_topic(client, &stream_id, request).await?;
    let reply_endpoint = ensure_topic(client, &stream_id, reply).await?;
    Ok((request_endpoint, reply_endpoint))
}

async fn ensure_topic(
    client: &IggyClient,
    stream_id: &Identifier,
    topic: &str,
) -> AnyResult<TopicEndpoint> {
    let topic_id = Identifier::named(topic)?;
    if client.get_topic(stream_id, &topic_id).await?.is_none() {
        if let Err(error) = client
            .create_topic(
                stream_id,
                topic,
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
            )
            .await
        {
            if client.get_topic(stream_id, &topic_id).await?.is_none() {
                return Err(Box::new(error));
            }
        }
    }

    let details = client
        .get_topic(stream_id, &topic_id)
        .await?
        .ok_or_else(|| format!("Iggy topic {topic} disappeared after creation"))?;
    let partition_id = details
        .partitions
        .first()
        .map(|partition| partition.id)
        .ok_or_else(|| format!("Iggy topic {topic} has no partitions"))?;

    Ok(TopicEndpoint {
        stream_id: stream_id.clone(),
        topic_id,
        partition_id,
    })
}

async fn send_one(client: &IggyClient, endpoint: &TopicEndpoint, message: IggyMessage) -> AnyResult<()> {
    let mut messages = vec![message];
    client
        .send_messages(
            &endpoint.stream_id,
            &endpoint.topic_id,
            &Partitioning::partition_id(endpoint.partition_id),
            &mut messages,
        )
        .await?;
    Ok(())
}

async fn next_payload(
    client: &IggyClient,
    endpoint: &TopicEndpoint,
    consumer: &Consumer,
    next_offset: &mut u64,
    timeout_ms: u64,
) -> AnyResult<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let polled = client
            .poll_messages(
                &endpoint.stream_id,
                &endpoint.topic_id,
                Some(endpoint.partition_id),
                consumer,
                &PollingStrategy::offset(*next_offset),
                1,
                false,
            )
            .await?;
        if let Some(message) = polled.messages.into_iter().next() {
            *next_offset = polled.current_offset;
            return Ok(message.payload.to_vec());
        }
        if Instant::now() >= deadline {
            return Err(format!("Iggy consumer timed out after {timeout_ms}ms").into());
        }
        sleep(Duration::from_millis(10)).await;
    }
}

fn make_message(sequence: u64, timestamp_ns: u64, intended_send_time_ns: u64, size: usize) -> AnyResult<IggyMessage> {
    Ok(IggyMessage::builder()
        .id(sequence.into())
        .payload(Bytes::from(payload(sequence, timestamp_ns, intended_send_time_ns, size)))
        .build()?)
}

async fn run_server(args: &Args) -> AnyResult<()> {
    let client = connect_client(args).await?;
    let stream = stream_name(args);
    let request = request_topic(args);
    let reply = reply_topic(args);
    let (request_endpoint, reply_endpoint) = ensure_stream_and_topics(&client, &stream, &request, &reply).await?;
    let consumer = consumer_id(args, "server")?;
    let mut request_offset = 0_u64;

    loop {
        let incoming = next_payload(&client, &request_endpoint, &consumer, &mut request_offset, args.poll_timeout_ms).await?;
        let echoed = IggyMessage::builder().payload(Bytes::from(incoming)).build()?;
        send_one(&client, &reply_endpoint, echoed).await?;
    }
}

async fn run_client(args: &Args) -> AnyResult<()> {
    let client = connect_client(args).await?;
    let stream = stream_name(args);
    let request = request_topic(args);
    let reply = reply_topic(args);
    let (request_endpoint, reply_endpoint) = ensure_stream_and_topics(&client, &stream, &request, &reply).await?;
    let consumer = consumer_id(args, "client")?;
    let mut reply_offset = 0_u64;

    for sequence in 0..args.warmup {
        let message = make_message(sequence, 0, 0, args.message_size)?;
        send_one(&client, &request_endpoint, message).await?;
        let _ = next_payload(&client, &reply_endpoint, &consumer, &mut reply_offset, args.poll_timeout_ms).await?;
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
        let message = make_message(sequence, send_time_ns, intended_send_time_ns, args.message_size)?;
        send_one(&client, &request_endpoint, message).await?;

        let echoed = next_payload(&client, &reply_endpoint, &consumer, &mut reply_offset, args.poll_timeout_ms).await?;
        let (_echoed_sequence, _echoed_send_ns, echoed_intended_ns) = parse_payload(&echoed)?;
        let receive_ns = send_instant.elapsed().as_nanos() as u64;
        throughput_hist.record(receive_ns).ok();
        if let Some(hist) = co_hist.as_mut() {
            let recv_from_base_ns = Instant::now().duration_since(base).as_nanos() as u64;
            hist.record(recv_from_base_ns.saturating_sub(echoed_intended_ns)).ok();
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
            "iggy-tcp size={} throughput={:.2} latency_p50={}ns latency_p99={}ns",
            args.message_size,
            throughput,
            latency_hist.value_at_percentile(50.0),
            latency_hist.value_at_percentile(99.0)
        );
        return Ok(());
    }

    let out = BenchmarkResultsOut {
        adapter: "iggy-tcp".to_string(),
        family: "pingpong".to_string(),
        config: BenchmarkConfigOut {
            message_size: args.message_size,
            num_messages: args.num_messages,
            warmup_messages: args.warmup,
            buffer_size: 1,
            wait_strategy: "brokered_tcp".to_string(),
            consumers: None,
        },
        throughput,
        fanout_throughput: None,
        messages_processed: args.num_messages,
        duration_secs: duration.as_secs_f64(),
        publish_duration_secs: None,
        latency_stats: LatencyStatsOut::from(latency_hist),
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
    };

    let mut value = serde_json::to_value(&out)?;
    if let Some(hist) = co_hist {
        value["coordinated_omission_stats"] = serde_json::to_value(LatencyStatsOut::from(hist))?;
    }
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
