use crate::infra::naming::unique_shm_segment;
use crate::infra::signal_latency::monotonic_raw_ns;
use disruptor_mp::observability::{
    ids, CounterSnapshot, CountersFile, COUNTERS_FILE_RESERVED_BYTES,
};
use disruptor_mp::SharedCursor;
use serde::Serialize;
use shared_memory::{Shmem, ShmemConf};
use std::ptr::NonNull;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub const SIGNAL_COUNTERS_SHM_ENV: &str = crate::infra::env::SIGNAL_COUNTERS_SHM_ID;
pub const SIGNAL_COUNTERS_MODE_ENV: &str = crate::infra::env::SIGNAL_COUNTERS_MODE;
pub const SIGNAL_SINGLE_CONSUMER_ID: &str = "ad_0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignalCountersMode {
    Full,
    Lite,
}

impl SignalCountersMode {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "full" => Ok(Self::Full),
            "lite" => Ok(Self::Lite),
            other => Err(format!("unknown signal counters mode: {other}")),
        }
    }

    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        match std::env::var(SIGNAL_COUNTERS_MODE_ENV) {
            Ok(raw) => Self::parse(&raw).map_err(Into::into),
            Err(std::env::VarError::NotPresent) => Ok(Self::Full),
            Err(error) => Err(Box::new(error)),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Lite => "lite",
        }
    }
}

pub struct SharedSignalCounters {
    _shmem: Shmem,
    file: CountersFile,
}

impl SharedSignalCounters {
    fn force_unlink(name: &str) {
        unsafe {
            if let Ok(c_str) = std::ffi::CString::new(name) {
                libc::shm_unlink(c_str.as_ptr());
            }
        }
    }

    pub fn create(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::force_unlink(name);
        let shmem = ShmemConf::new()
            .size(COUNTERS_FILE_RESERVED_BYTES)
            .os_id(name)
            .create()?;
        let ptr = NonNull::new(shmem.as_ptr()).ok_or("null counters shm ptr")?;
        unsafe {
            std::ptr::write_bytes(ptr.as_ptr(), 0, COUNTERS_FILE_RESERVED_BYTES);
        }
        let file = unsafe { CountersFile::init(ptr) };
        Ok(Self {
            _shmem: shmem,
            file,
        })
    }

    pub fn open_with_timeout(
        name: &str,
        timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            match ShmemConf::new().os_id(name).open() {
                Ok(shmem) => {
                    let ptr = NonNull::new(shmem.as_ptr()).ok_or("null counters shm ptr")?;
                    let file = unsafe { CountersFile::attach(ptr)? };
                    return Ok(Self {
                        _shmem: shmem,
                        file,
                    });
                }
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => return Err(Box::new(error)),
            }
        }
    }

    pub fn file(&self) -> &CountersFile {
        &self.file
    }
}

pub fn unique_signal_counters_name() -> String {
    unique_shm_segment("sigctr")
}

#[derive(Debug, Default, Clone, Copy)]
struct CounterValues {
    events_published: u64,
    events_consumed: u64,
    producer_full_events: u64,
    consumer_empty_spins: u64,
    consumer_lag_max: u64,
}

impl CounterValues {
    fn from_snapshot(snapshot: &[CounterSnapshot]) -> Self {
        let mut values = Self::default();
        for counter in snapshot {
            match counter.id {
                ids::EVENTS_PUBLISHED => values.events_published = counter.value,
                ids::EVENTS_CONSUMED => values.events_consumed = counter.value,
                ids::PRODUCER_FULL_EVENTS => values.producer_full_events = counter.value,
                ids::CONSUMER_EMPTY_SPINS => values.consumer_empty_spins = counter.value,
                ids::CONSUMER_LAG_MAX => values.consumer_lag_max = counter.value,
                _ => {}
            }
        }
        values
    }
}

#[derive(Debug, Clone)]
struct CounterBacklogSample {
    backlog: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignalCountersEstimate {
    pub warmup_messages: u64,
    pub measured_messages: u64,
    pub poll_interval_us: u64,
    pub sample_count: usize,
    pub published_total: u64,
    pub consumed_total: u64,
    pub producer_full_events: u64,
    pub consumer_empty_spins: u64,
    pub consumer_lag_max: u64,
    pub observed_duration_ns: u64,
    pub observed_publish_ops_sec: f64,
    pub observed_consume_ops_sec: f64,
    pub avg_backlog: f64,
    pub p50_backlog: u64,
    pub p95_backlog: u64,
    pub p99_backlog: u64,
    pub p999_backlog: u64,
    pub p9999_backlog: u64,
    pub p99999_backlog: u64,
    pub max_backlog: u64,
    pub estimated_mean_queue_latency_ns: f64,
    pub estimated_p50_queue_latency_ns: f64,
    pub estimated_p95_queue_latency_ns: f64,
    pub estimated_p99_queue_latency_ns: f64,
    pub estimated_p999_queue_latency_ns: f64,
    pub estimated_p9999_queue_latency_ns: f64,
    pub estimated_p99999_queue_latency_ns: f64,
    pub estimated_max_queue_latency_ns: f64,
    pub note: String,
}

impl SignalCountersEstimate {
    fn empty(warmup_messages: u64, measured_messages: u64, poll_interval_us: u64) -> Self {
        Self {
            warmup_messages,
            measured_messages,
            poll_interval_us,
            sample_count: 0,
            published_total: 0,
            consumed_total: 0,
            producer_full_events: 0,
            consumer_empty_spins: 0,
            consumer_lag_max: 0,
            observed_duration_ns: 0,
            observed_publish_ops_sec: 0.0,
            observed_consume_ops_sec: 0.0,
            avg_backlog: 0.0,
            p50_backlog: 0,
            p95_backlog: 0,
            p99_backlog: 0,
            p999_backlog: 0,
            p9999_backlog: 0,
            p99999_backlog: 0,
            max_backlog: 0,
            estimated_mean_queue_latency_ns: 0.0,
            estimated_p50_queue_latency_ns: 0.0,
            estimated_p95_queue_latency_ns: 0.0,
            estimated_p99_queue_latency_ns: 0.0,
            estimated_p999_queue_latency_ns: 0.0,
            estimated_p9999_queue_latency_ns: 0.0,
            estimated_p99999_queue_latency_ns: 0.0,
            estimated_max_queue_latency_ns: 0.0,
            note: "no counters samples collected".to_string(),
        }
    }
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn backlog_to_latency_ns(backlog: f64, consume_ops_sec: f64) -> f64 {
    if consume_ops_sec <= 0.0 {
        0.0
    } else {
        backlog * 1_000_000_000.0 / consume_ops_sec
    }
}

#[inline]
fn absolute_event_count(sequence: i64) -> u64 {
    if sequence < 0 {
        0
    } else {
        sequence as u64 + 1
    }
}

pub struct SignalCountersObserver {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<Result<SignalCountersEstimate, String>>>,
}

impl SignalCountersObserver {
    pub fn spawn(
        shm_name: String,
        warmup_messages: u64,
        measured_messages: u64,
        poll_interval: Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_worker = Arc::clone(&stop);
        let poll_interval_us = poll_interval.as_micros() as u64;
        let join = std::thread::Builder::new()
            .name("perf-bench-signal-counters".into())
            .spawn(move || {
                run_observer(
                    shm_name,
                    warmup_messages,
                    measured_messages,
                    poll_interval,
                    poll_interval_us,
                    stop_worker,
                )
            })
            .expect("spawn counters observer");
        Self {
            stop,
            join: Some(join),
        }
    }

    pub fn stop_and_join(mut self) -> Result<SignalCountersEstimate, String> {
        self.stop.store(true, Ordering::Relaxed);
        match self.join.take().expect("observer join handle").join() {
            Ok(result) => result,
            Err(_) => Err("signal counters observer panicked".to_string()),
        }
    }
}

pub struct SignalSequenceObserver {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<Result<SignalCountersEstimate, String>>>,
}

impl SignalSequenceObserver {
    pub fn spawn(
        segment: String,
        consumer_id: String,
        warmup_messages: u64,
        measured_messages: u64,
        poll_interval: Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_worker = Arc::clone(&stop);
        let poll_interval_us = poll_interval.as_micros() as u64;
        let join = std::thread::Builder::new()
            .name("perf-bench-signal-sequences".into())
            .spawn(move || {
                run_sequence_observer(
                    segment,
                    consumer_id,
                    warmup_messages,
                    measured_messages,
                    poll_interval,
                    poll_interval_us,
                    stop_worker,
                )
            })
            .expect("spawn sequence observer");
        Self {
            stop,
            join: Some(join),
        }
    }

    pub fn stop_and_join(mut self) -> Result<SignalCountersEstimate, String> {
        self.stop.store(true, Ordering::Relaxed);
        match self
            .join
            .take()
            .expect("sequence observer join handle")
            .join()
        {
            Ok(result) => result,
            Err(_) => Err("signal sequence observer panicked".to_string()),
        }
    }
}

fn run_observer(
    shm_name: String,
    warmup_messages: u64,
    measured_messages: u64,
    poll_interval: Duration,
    poll_interval_us: u64,
    stop: Arc<AtomicBool>,
) -> Result<SignalCountersEstimate, String> {
    let counters = SharedSignalCounters::open_with_timeout(&shm_name, Duration::from_secs(15))
        .map_err(|error| format!("open shared signal counters: {error}"))?;
    let target_total = warmup_messages.saturating_add(measured_messages);
    let mut started = false;
    let mut start_ns = 0u64;
    let mut first_values = CounterValues::default();
    let mut last_values = CounterValues::default();
    let mut samples = Vec::new();

    loop {
        let snapshot = counters.file().snapshot();
        let values = CounterValues::from_snapshot(&snapshot);
        if !started && values.events_published >= warmup_messages {
            started = true;
            start_ns = monotonic_raw_ns();
            first_values = values;
        }
        if started {
            let published = values
                .events_published
                .saturating_sub(first_values.events_published);
            let consumed = values
                .events_consumed
                .saturating_sub(first_values.events_consumed);
            samples.push(CounterBacklogSample {
                backlog: published.saturating_sub(consumed),
            });
            last_values = values;
            if values.events_published >= target_total && values.events_consumed >= target_total {
                break;
            }
        }
        if stop.load(Ordering::Relaxed) && started {
            break;
        }
        if poll_interval.is_zero() {
            std::hint::spin_loop();
        } else {
            std::thread::sleep(poll_interval);
        }
    }

    if !started {
        return Ok(SignalCountersEstimate::empty(
            warmup_messages,
            measured_messages,
            poll_interval_us,
        ));
    }

    let end_ns = monotonic_raw_ns();
    let observed_duration_ns = end_ns.saturating_sub(start_ns);
    let published_total = last_values
        .events_published
        .saturating_sub(first_values.events_published);
    let consumed_total = last_values
        .events_consumed
        .saturating_sub(first_values.events_consumed);
    let duration_sec = observed_duration_ns as f64 / 1_000_000_000.0;
    let observed_publish_ops_sec = if duration_sec > 0.0 {
        published_total as f64 / duration_sec
    } else {
        0.0
    };
    let observed_consume_ops_sec = if duration_sec > 0.0 {
        consumed_total as f64 / duration_sec
    } else {
        0.0
    };

    let mut backlogs: Vec<u64> = samples.iter().map(|sample| sample.backlog).collect();
    backlogs.sort_unstable();
    let avg_backlog = if backlogs.is_empty() {
        0.0
    } else {
        backlogs.iter().map(|value| *value as f64).sum::<f64>() / backlogs.len() as f64
    };
    let p50_backlog = percentile(&backlogs, 0.50);
    let p95_backlog = percentile(&backlogs, 0.95);
    let p99_backlog = percentile(&backlogs, 0.99);
    let p999_backlog = percentile(&backlogs, 0.999);
    let p9999_backlog = percentile(&backlogs, 0.9999);
    let p99999_backlog = percentile(&backlogs, 0.99999);
    let max_backlog = backlogs.last().copied().unwrap_or(0);

    Ok(SignalCountersEstimate {
        warmup_messages,
        measured_messages,
        poll_interval_us,
        sample_count: backlogs.len(),
        published_total,
        consumed_total,
        producer_full_events: last_values.producer_full_events,
        consumer_empty_spins: last_values.consumer_empty_spins,
        consumer_lag_max: last_values.consumer_lag_max,
        observed_duration_ns,
        observed_publish_ops_sec,
        observed_consume_ops_sec,
        avg_backlog,
        p50_backlog,
        p95_backlog,
        p99_backlog,
        p999_backlog,
        p9999_backlog,
        p99999_backlog,
        max_backlog,
        estimated_mean_queue_latency_ns: backlog_to_latency_ns(avg_backlog, observed_consume_ops_sec),
        estimated_p50_queue_latency_ns: backlog_to_latency_ns(
            p50_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p95_queue_latency_ns: backlog_to_latency_ns(
            p95_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p99_queue_latency_ns: backlog_to_latency_ns(
            p99_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p999_queue_latency_ns: backlog_to_latency_ns(
            p999_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p9999_queue_latency_ns: backlog_to_latency_ns(
            p9999_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p99999_queue_latency_ns: backlog_to_latency_ns(
            p99999_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_max_queue_latency_ns: backlog_to_latency_ns(
            max_backlog as f64,
            observed_consume_ops_sec,
        ),
        note: "queueing-only estimate from shared RFC-0040 counters; excludes base propagation latency".to_string(),
    })
}

fn run_sequence_observer(
    segment: String,
    consumer_id: String,
    warmup_messages: u64,
    measured_messages: u64,
    poll_interval: Duration,
    poll_interval_us: u64,
    stop: Arc<AtomicBool>,
) -> Result<SignalCountersEstimate, String> {
    let producer_name = format!("{segment}_producer_seq");
    let consumer_name = format!("{segment}_{consumer_id}_seq");
    let producer_cursor = open_cursor_with_timeout(&producer_name, Duration::from_secs(15))?;
    let consumer_cursor = open_cursor_with_timeout(&consumer_name, Duration::from_secs(15))?;

    let mut started = false;
    let mut start_ns = 0u64;
    let mut first_published = 0u64;
    let mut first_consumed = 0u64;
    let mut last_published = 0u64;
    let mut last_consumed = 0u64;
    let mut samples = Vec::new();

    loop {
        let producer_seq = producer_cursor.load(Ordering::Acquire);
        let consumer_seq = consumer_cursor.load(Ordering::Acquire);
        let published_total_abs = absolute_event_count(producer_seq);
        let consumed_total_abs = absolute_event_count(consumer_seq);

        if !started && published_total_abs >= warmup_messages {
            started = true;
            start_ns = monotonic_raw_ns();
            first_published = published_total_abs;
            first_consumed = consumed_total_abs;
        }

        if started {
            let published = published_total_abs.saturating_sub(first_published);
            let consumed = consumed_total_abs.saturating_sub(first_consumed);
            samples.push(CounterBacklogSample {
                backlog: published.saturating_sub(consumed),
            });
            last_published = published_total_abs;
            last_consumed = consumed_total_abs;
            if published_total_abs >= warmup_messages.saturating_add(measured_messages)
                && consumed_total_abs >= warmup_messages.saturating_add(measured_messages)
            {
                break;
            }
        }

        if stop.load(Ordering::Relaxed) && started {
            break;
        }
        if poll_interval.is_zero() {
            std::hint::spin_loop();
        } else {
            std::thread::sleep(poll_interval);
        }
    }

    if !started {
        return Ok(SignalCountersEstimate::empty(
            warmup_messages,
            measured_messages,
            poll_interval_us,
        ));
    }

    let end_ns = monotonic_raw_ns();
    let observed_duration_ns = end_ns.saturating_sub(start_ns);
    let published_total = last_published.saturating_sub(first_published);
    let consumed_total = last_consumed.saturating_sub(first_consumed);
    let duration_sec = observed_duration_ns as f64 / 1_000_000_000.0;
    let observed_publish_ops_sec = if duration_sec > 0.0 {
        published_total as f64 / duration_sec
    } else {
        0.0
    };
    let observed_consume_ops_sec = if duration_sec > 0.0 {
        consumed_total as f64 / duration_sec
    } else {
        0.0
    };

    let mut backlogs: Vec<u64> = samples.iter().map(|sample| sample.backlog).collect();
    backlogs.sort_unstable();
    let avg_backlog = if backlogs.is_empty() {
        0.0
    } else {
        backlogs.iter().map(|value| *value as f64).sum::<f64>() / backlogs.len() as f64
    };
    let p50_backlog = percentile(&backlogs, 0.50);
    let p95_backlog = percentile(&backlogs, 0.95);
    let p99_backlog = percentile(&backlogs, 0.99);
    let p999_backlog = percentile(&backlogs, 0.999);
    let p9999_backlog = percentile(&backlogs, 0.9999);
    let p99999_backlog = percentile(&backlogs, 0.99999);
    let max_backlog = backlogs.last().copied().unwrap_or(0);

    Ok(SignalCountersEstimate {
        warmup_messages,
        measured_messages,
        poll_interval_us,
        sample_count: backlogs.len(),
        published_total,
        consumed_total,
        producer_full_events: 0,
        consumer_empty_spins: 0,
        consumer_lag_max: 0,
        observed_duration_ns,
        observed_publish_ops_sec,
        observed_consume_ops_sec,
        avg_backlog,
        p50_backlog,
        p95_backlog,
        p99_backlog,
        p999_backlog,
        p9999_backlog,
        p99999_backlog,
        max_backlog,
        estimated_mean_queue_latency_ns: backlog_to_latency_ns(avg_backlog, observed_consume_ops_sec),
        estimated_p50_queue_latency_ns: backlog_to_latency_ns(
            p50_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p95_queue_latency_ns: backlog_to_latency_ns(
            p95_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p99_queue_latency_ns: backlog_to_latency_ns(
            p99_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p999_queue_latency_ns: backlog_to_latency_ns(
            p999_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p9999_queue_latency_ns: backlog_to_latency_ns(
            p9999_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_p99999_queue_latency_ns: backlog_to_latency_ns(
            p99999_backlog as f64,
            observed_consume_ops_sec,
        ),
        estimated_max_queue_latency_ns: backlog_to_latency_ns(
            max_backlog as f64,
            observed_consume_ops_sec,
        ),
        note: "queueing-only estimate from existing producer/consumer sequence cursors; zero extra hot-path metrics atomics; excludes base propagation latency".to_string(),
    })
}

fn open_cursor_with_timeout(name: &str, timeout: Duration) -> Result<SharedCursor, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match SharedCursor::attach(name) {
            Ok(cursor) => return Ok(cursor),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            Err(error) => return Err(format!("attach cursor {name}: {error}")),
        }
    }
}
