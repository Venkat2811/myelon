use clap::Parser;
use competitive_bench::pingpong_support::{
    ensure_message_size, interval_for_rate, pace_until, parse_payload, payload, scoped_name,
};
use competitive_bench::result_json::{BenchmarkConfigOut, BenchmarkResultsOut, LatencyStatsOut};
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use rskafka::client::{
    consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder},
    error::{Error as KafkaError, ProtocolError},
    partition::{Compression, OffsetAt, UnknownTopicHandling},
    Client, ClientBuilder,
};
use rskafka::record::Record;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

const PARTITION_ID: i32 = 0;

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Redpanda Kafka/TCP brokered 2-process ping-pong benchmark")]
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

    #[arg(long, default_value = "127.0.0.1:19092")]
    bootstrap: String,

    #[arg(long, default_value_t = 120)]
    connect_retries: u32,

    #[arg(long, default_value_t = 500)]
    connect_retry_ms: u64,

    #[arg(long, default_value_t = 30_000)]
    poll_timeout_ms: u64,

    #[arg(long, default_value_t = 100)]
    fetch_wait_ms: i32,
}

fn default_base_name() -> String {
    format!("redpanda_pp_{}", std::process::id())
}

fn request_topic(args: &Args) -> String {
    scoped_name("cbr", &args.base, "request")
}

fn reply_topic(args: &Args) -> String {
    scoped_name("cbr", &args.base, "reply")
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

async fn connect_client(args: &Args) -> AnyResult<Client> {
    let mut last_error = None;
    for _attempt in 0..args.connect_retries {
        match ClientBuilder::new(vec![args.bootstrap.clone()]).build().await {
            Ok(client) => return Ok(client),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(args.connect_retry_ms)).await;
            }
        }
    }

    Err(format!(
        "failed to connect to Redpanda at {} after {} attempts: {:?}",
        args.bootstrap, args.connect_retries, last_error
    )
    .into())
}

async fn ensure_topics(client: &Client, request: &str, reply: &str) -> AnyResult<()> {
    let controller = client.controller_client()?;
    ensure_topic(&controller, request).await?;
    ensure_topic(&controller, reply).await?;
    Ok(())
}

async fn ensure_topic(
    controller: &rskafka::client::controller::ControllerClient,
    topic: &str,
) -> AnyResult<()> {
    match controller.create_topic(topic, 1, 1, 10_000).await {
        Ok(()) => Ok(()),
        Err(KafkaError::ServerError {
            protocol_error: ProtocolError::TopicAlreadyExists,
            ..
        }) => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

fn make_record(sequence: u64, timestamp_ns: u64, intended_send_time_ns: u64, size: usize) -> Record {
    Record {
        key: None,
        value: Some(payload(sequence, timestamp_ns, intended_send_time_ns, size)),
        headers: BTreeMap::new(),
        timestamp: chrono::Utc::now(),
    }
}

fn echo_record(value: Vec<u8>) -> Record {
    Record {
        key: None,
        value: Some(value),
        headers: BTreeMap::new(),
        timestamp: chrono::Utc::now(),
    }
}

async fn build_consumer(
    partition: Arc<rskafka::client::partition::PartitionClient>,
    fetch_wait_ms: i32,
) -> AnyResult<StreamConsumer> {
    let _ = partition.get_offset(OffsetAt::Latest).await?;
    Ok(StreamConsumerBuilder::new(partition, StartOffset::Earliest)
        .with_max_wait_ms(fetch_wait_ms)
        .build())
}

async fn next_payload(consumer: &mut StreamConsumer, timeout_ms: u64) -> AnyResult<Vec<u8>> {
    match timeout(Duration::from_millis(timeout_ms), consumer.next()).await {
        Ok(Some(Ok((record_and_offset, _high_watermark)))) => Ok(record_and_offset
            .record
            .value
            .unwrap_or_default()),
        Ok(Some(Err(error))) => Err(Box::new(error)),
        Ok(None) => Err("Redpanda consumer stream ended unexpectedly".into()),
        Err(_) => Err(format!("Redpanda consumer timed out after {timeout_ms}ms").into()),
    }
}

async fn run_server(args: &Args) -> AnyResult<()> {
    let client = connect_client(args).await?;
    let request = request_topic(args);
    let reply = reply_topic(args);
    ensure_topics(&client, &request, &reply).await?;

    let request_partition = Arc::new(
        client
            .partition_client(&request, PARTITION_ID, UnknownTopicHandling::Retry)
            .await?,
    );
    let reply_partition = client
        .partition_client(&reply, PARTITION_ID, UnknownTopicHandling::Retry)
        .await?;
    let mut request_consumer = build_consumer(request_partition, args.fetch_wait_ms).await?;

    loop {
        let incoming = next_payload(&mut request_consumer, args.poll_timeout_ms).await?;
        reply_partition
            .produce(vec![echo_record(incoming)], Compression::NoCompression)
            .await?;
    }
}

async fn run_client(args: &Args) -> AnyResult<()> {
    let client = connect_client(args).await?;
    let request = request_topic(args);
    let reply = reply_topic(args);
    ensure_topics(&client, &request, &reply).await?;

    let request_partition = client
        .partition_client(&request, PARTITION_ID, UnknownTopicHandling::Retry)
        .await?;
    let reply_partition = Arc::new(
        client
            .partition_client(&reply, PARTITION_ID, UnknownTopicHandling::Retry)
            .await?,
    );
    let mut reply_consumer = build_consumer(reply_partition, args.fetch_wait_ms).await?;

    for sequence in 0..args.warmup {
        request_partition
            .produce(
                vec![make_record(sequence, 0, 0, args.message_size)],
                Compression::NoCompression,
            )
            .await?;
        let _ = next_payload(&mut reply_consumer, args.poll_timeout_ms).await?;
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
        request_partition
            .produce(
                vec![make_record(
                    sequence,
                    send_time_ns,
                    intended_send_time_ns,
                    args.message_size,
                )],
                Compression::NoCompression,
            )
            .await?;
        let echoed = next_payload(&mut reply_consumer, args.poll_timeout_ms).await?;
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
            "redpanda-kafka size={} throughput={:.2} latency_p50={}ns latency_p99={}ns",
            args.message_size,
            throughput,
            latency_hist.value_at_percentile(50.0),
            latency_hist.value_at_percentile(99.0)
        );
        return Ok(());
    }

    let out = BenchmarkResultsOut {
        adapter: "redpanda-kafka".to_string(),
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
