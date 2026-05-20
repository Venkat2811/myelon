use shared_memory::{Shmem, ShmemConf};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
pub struct SignalEvent {
    pub sequence: u64,
    pub data: u64,
    pub stamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalLatencyMode {
    None,
    InlineMonoRawNs,
    InlineRdtsc,
    SidecarMonoRawNs,
    SidecarRdtsc,
    InlineMonoRawNsOffline,
    InlineRdtscOffline,
    SidecarMonoRawNsOffline,
    SidecarRdtscOffline,
}

impl SignalLatencyMode {
    pub fn canonical() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self::InlineRdtscOffline
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            Self::InlineMonoRawNsOffline
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "none" => Ok(Self::None),
            "inline-ns" => Ok(Self::InlineMonoRawNs),
            "inline-tsc" => Ok(Self::InlineRdtsc),
            "sidecar-ns" => Ok(Self::SidecarMonoRawNs),
            "sidecar-tsc" => Ok(Self::SidecarRdtsc),
            "inline-ns-offline" => Ok(Self::InlineMonoRawNsOffline),
            "inline-tsc-offline" => Ok(Self::InlineRdtscOffline),
            "sidecar-ns-offline" => Ok(Self::SidecarMonoRawNsOffline),
            "sidecar-tsc-offline" => Ok(Self::SidecarRdtscOffline),
            other => Err(format!(
                "unsupported signal latency mode: {other} (expected one of: none, inline-ns, inline-tsc, sidecar-ns, sidecar-tsc, inline-ns-offline, inline-tsc-offline, sidecar-ns-offline, sidecar-tsc-offline)"
            )),
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let raw = std::env::var(crate::infra::env::SIGNAL_LATENCY_MODE)
            .unwrap_or_else(|_| "none".to_string());
        Self::parse(&raw)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InlineMonoRawNs => "inline-ns",
            Self::InlineRdtsc => "inline-tsc",
            Self::SidecarMonoRawNs => "sidecar-ns",
            Self::SidecarRdtsc => "sidecar-tsc",
            Self::InlineMonoRawNsOffline => "inline-ns-offline",
            Self::InlineRdtscOffline => "inline-tsc-offline",
            Self::SidecarMonoRawNsOffline => "sidecar-ns-offline",
            Self::SidecarRdtscOffline => "sidecar-tsc-offline",
        }
    }

    pub fn records_latency(self) -> bool {
        !matches!(self, Self::None)
    }

    pub fn uses_sidecar(self) -> bool {
        matches!(
            self,
            Self::SidecarMonoRawNs
                | Self::SidecarRdtsc
                | Self::SidecarMonoRawNsOffline
                | Self::SidecarRdtscOffline
        )
    }

    pub fn uses_rdtsc(self) -> bool {
        matches!(
            self,
            Self::InlineRdtsc
                | Self::SidecarRdtsc
                | Self::InlineRdtscOffline
                | Self::SidecarRdtscOffline
        )
    }

    pub fn is_inline(self) -> bool {
        matches!(
            self,
            Self::InlineMonoRawNs
                | Self::InlineRdtsc
                | Self::InlineMonoRawNsOffline
                | Self::InlineRdtscOffline
        )
    }

    pub fn uses_offline_samples(self) -> bool {
        matches!(
            self,
            Self::InlineMonoRawNsOffline
                | Self::InlineRdtscOffline
                | Self::SidecarMonoRawNsOffline
                | Self::SidecarRdtscOffline
        )
    }
}

pub fn sample_every_from_env() -> u64 {
    std::env::var(crate::infra::env::SIGNAL_SAMPLE_EVERY)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

#[inline]
pub fn should_sample(sequence: u64, sample_every: u64) -> bool {
    sample_every <= 1 || sequence.is_multiple_of(sample_every)
}

#[inline]
pub fn monotonic_raw_ns() -> u64 {
    unsafe {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let rc = libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
        if rc != 0 {
            return 0;
        }
        (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64)
    }
}

#[inline]
pub fn read_stamp(mode: SignalLatencyMode) -> u64 {
    match mode {
        SignalLatencyMode::None => 0,
        SignalLatencyMode::InlineMonoRawNs
        | SignalLatencyMode::SidecarMonoRawNs
        | SignalLatencyMode::InlineMonoRawNsOffline
        | SignalLatencyMode::SidecarMonoRawNsOffline => monotonic_raw_ns(),
        SignalLatencyMode::InlineRdtsc
        | SignalLatencyMode::SidecarRdtsc
        | SignalLatencyMode::InlineRdtscOffline
        | SignalLatencyMode::SidecarRdtscOffline => rdtsc_now().unwrap_or(0),
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
pub fn rdtsc_now() -> Option<u64> {
    let mut aux = 0u32;
    Some(unsafe { core::arch::x86_64::__rdtscp(&mut aux) })
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
pub fn rdtsc_now() -> Option<u64> {
    None
}

#[derive(Debug, Clone, Copy)]
pub struct TscCalibration {
    ns_per_tick: f64,
}

impl TscCalibration {
    pub fn calibrate() -> Option<Self> {
        let start_tick = rdtsc_now()?;
        let start_ns = monotonic_raw_ns();
        let deadline = start_ns.saturating_add(20_000_000);
        let mut end_ns = start_ns;
        while end_ns < deadline {
            std::hint::spin_loop();
            end_ns = monotonic_raw_ns();
        }
        let end_tick = rdtsc_now()?;
        let tick_delta = end_tick.saturating_sub(start_tick);
        let ns_delta = end_ns.saturating_sub(start_ns);
        if tick_delta == 0 || ns_delta == 0 {
            return None;
        }
        Some(Self {
            ns_per_tick: ns_delta as f64 / tick_delta as f64,
        })
    }

    #[inline]
    pub fn delta_to_ns(&self, send_tick: u64, recv_tick: u64) -> u64 {
        let ticks = recv_tick.saturating_sub(send_tick);
        (ticks as f64 * self.ns_per_tick).round() as u64
    }
}

pub trait TimestampSidecar {
    fn store(&mut self, sequence: u64, value: u64);
    fn load(&self, sequence: u64) -> u64;
}

pub struct ShmTimestampSidecar {
    _shmem: Shmem,
    ptr: *mut u64,
    slots: usize,
}

impl ShmTimestampSidecar {
    fn force_unlink(name: &str) {
        unsafe {
            if let Ok(c_str) = std::ffi::CString::new(name) {
                libc::shm_unlink(c_str.as_ptr());
            }
        }
    }

    pub fn create(name: &str, slots: usize) -> Result<Self, Box<dyn std::error::Error>> {
        Self::force_unlink(name);
        let bytes = slots
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or("sidecar size overflow")?;
        let shmem = ShmemConf::new().size(bytes).os_id(name).create()?;
        let ptr = shmem.as_ptr() as *mut u64;
        unsafe {
            std::ptr::write_bytes(ptr, 0, slots);
        }
        Ok(Self {
            _shmem: shmem,
            ptr,
            slots,
        })
    }

    pub fn open_with_timeout(
        name: &str,
        slots: usize,
        timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            match ShmemConf::new().os_id(name).open() {
                Ok(shmem) => {
                    let ptr = shmem.as_ptr() as *mut u64;
                    return Ok(Self {
                        _shmem: shmem,
                        ptr,
                        slots,
                    });
                }
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25))
                }
                Err(error) => return Err(Box::new(error)),
            }
        }
    }
}

impl TimestampSidecar for ShmTimestampSidecar {
    #[inline]
    fn store(&mut self, sequence: u64, value: u64) {
        let idx = (sequence as usize) % self.slots;
        unsafe { self.ptr.add(idx).write(value) };
    }

    #[inline]
    fn load(&self, sequence: u64) -> u64 {
        let idx = (sequence as usize) % self.slots;
        unsafe { self.ptr.add(idx).read() }
    }
}

pub struct MmapTimestampSidecar {
    _file: File,
    ptr: *mut u64,
    slots: usize,
    bytes: usize,
}

impl MmapTimestampSidecar {
    pub fn create(path: &Path, slots: usize) -> Result<Self, Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let bytes = slots
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or("sidecar size overflow")?;
        file.set_len(bytes as u64)?;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                std::os::fd::AsRawFd::as_raw_fd(&file),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(Box::new(io::Error::last_os_error()));
        }
        let ptr = ptr as *mut u64;
        unsafe {
            std::ptr::write_bytes(ptr, 0, slots);
        }
        Ok(Self {
            _file: file,
            ptr,
            slots,
            bytes,
        })
    }

    pub fn open_with_timeout(
        path: &Path,
        slots: usize,
        timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            match OpenOptions::new().read(true).write(true).open(path) {
                Ok(file) => {
                    let bytes = slots
                        .checked_mul(std::mem::size_of::<u64>())
                        .ok_or("sidecar size overflow")?;
                    let ptr = unsafe {
                        libc::mmap(
                            std::ptr::null_mut(),
                            bytes,
                            libc::PROT_READ | libc::PROT_WRITE,
                            libc::MAP_SHARED,
                            std::os::fd::AsRawFd::as_raw_fd(&file),
                            0,
                        )
                    };
                    if ptr == libc::MAP_FAILED {
                        return Err(Box::new(io::Error::last_os_error()));
                    }
                    return Ok(Self {
                        _file: file,
                        ptr: ptr as *mut u64,
                        slots,
                        bytes,
                    });
                }
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25))
                }
                Err(error) => return Err(Box::new(error)),
            }
        }
    }
}

impl TimestampSidecar for MmapTimestampSidecar {
    #[inline]
    fn store(&mut self, sequence: u64, value: u64) {
        let idx = (sequence as usize) % self.slots;
        unsafe { self.ptr.add(idx).write(value) };
    }

    #[inline]
    fn load(&self, sequence: u64) -> u64 {
        let idx = (sequence as usize) % self.slots;
        unsafe { self.ptr.add(idx).read() }
    }
}

impl Drop for MmapTimestampSidecar {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr.cast::<libc::c_void>(), self.bytes);
        }
    }
}

pub fn mmap_sidecar_path_from_env() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(PathBuf::from(std::env::var(
        crate::infra::env::SIGNAL_SIDECAR_MMAP_PATH,
    )?))
}

pub fn shm_sidecar_name_from_env() -> Result<String, Box<dyn std::error::Error>> {
    Ok(std::env::var(crate::infra::env::SIGNAL_SIDECAR_SHM_ID)?)
}

pub struct OfflineSignalSamples {
    values: Vec<u64>,
    len: usize,
}

impl OfflineSignalSamples {
    pub fn new(max_samples: usize) -> Self {
        Self {
            values: vec![0; max_samples.max(1)],
            len: 0,
        }
    }

    #[inline]
    pub fn record(&mut self, value: u64) {
        if value == 0 || self.len >= self.values.len() {
            return;
        }
        self.values[self.len] = value;
        self.len += 1;
    }

    pub fn into_latency_stats(
        self,
        mode: SignalLatencyMode,
        tsc: Option<&TscCalibration>,
    ) -> Option<crate::infra::latency::LatencyStats> {
        let mut recorder = crate::infra::latency::LatencyRecorder::default_range();
        for raw in &self.values[..self.len] {
            let value_ns = match mode {
                SignalLatencyMode::InlineRdtscOffline | SignalLatencyMode::SidecarRdtscOffline => {
                    tsc.map(|calibration| calibration.delta_to_ns(0, *raw))?
                }
                SignalLatencyMode::InlineMonoRawNsOffline
                | SignalLatencyMode::SidecarMonoRawNsOffline => *raw,
                _ => *raw,
            };
            recorder.record(value_ns);
        }
        recorder.stats()
    }
}

pub fn sample_capacity(events: u64, sample_every: u64) -> usize {
    let stride = sample_every.max(1);
    let count = events.saturating_add(stride - 1) / stride;
    count.try_into().unwrap_or(usize::MAX)
}

#[inline]
pub fn sampled_delta(mode: SignalLatencyMode, send_stamp: u64, recv_stamp: u64) -> Option<u64> {
    if send_stamp == 0 || recv_stamp == 0 || recv_stamp <= send_stamp {
        return None;
    }
    match mode {
        SignalLatencyMode::InlineRdtsc
        | SignalLatencyMode::SidecarRdtsc
        | SignalLatencyMode::InlineRdtscOffline
        | SignalLatencyMode::SidecarRdtscOffline => Some(recv_stamp - send_stamp),
        SignalLatencyMode::InlineMonoRawNs
        | SignalLatencyMode::SidecarMonoRawNs
        | SignalLatencyMode::InlineMonoRawNsOffline
        | SignalLatencyMode::SidecarMonoRawNsOffline => Some(recv_stamp - send_stamp),
        SignalLatencyMode::None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_latency_mode_parses() {
        assert_eq!(
            SignalLatencyMode::parse("none").unwrap(),
            SignalLatencyMode::None
        );
        assert_eq!(
            SignalLatencyMode::parse("inline-ns").unwrap(),
            SignalLatencyMode::InlineMonoRawNs
        );
        assert_eq!(
            SignalLatencyMode::parse("inline-tsc").unwrap(),
            SignalLatencyMode::InlineRdtsc
        );
        assert_eq!(
            SignalLatencyMode::parse("sidecar-ns").unwrap(),
            SignalLatencyMode::SidecarMonoRawNs
        );
        assert_eq!(
            SignalLatencyMode::parse("sidecar-tsc").unwrap(),
            SignalLatencyMode::SidecarRdtsc
        );
        assert_eq!(
            SignalLatencyMode::parse("inline-tsc-offline").unwrap(),
            SignalLatencyMode::InlineRdtscOffline
        );
        assert_eq!(
            SignalLatencyMode::parse("sidecar-ns-offline").unwrap(),
            SignalLatencyMode::SidecarMonoRawNsOffline
        );
        assert!(SignalLatencyMode::parse("bogus").is_err());
    }

    #[test]
    fn sample_every_zero_defaults_to_one() {
        std::env::set_var(crate::infra::env::SIGNAL_SAMPLE_EVERY, "0");
        assert_eq!(sample_every_from_env(), 1);
        std::env::remove_var(crate::infra::env::SIGNAL_SAMPLE_EVERY);
    }

    #[test]
    fn should_sample_honors_stride() {
        assert!(should_sample(0, 64));
        assert!(!should_sample(1, 64));
        assert!(should_sample(64, 64));
    }

    #[test]
    fn sample_capacity_rounds_up() {
        assert_eq!(sample_capacity(1000, 64), 16);
        assert_eq!(sample_capacity(1001, 64), 16);
        assert_eq!(sample_capacity(1002, 64), 16);
        assert_eq!(sample_capacity(1025, 64), 17);
    }

    #[test]
    fn monotonic_raw_ns_monotonic() {
        let a = monotonic_raw_ns();
        let b = monotonic_raw_ns();
        assert!(b >= a);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn tsc_calibration_produces_scale() {
        let cal = TscCalibration::calibrate().expect("tsc calibration");
        let start = rdtsc_now().unwrap();
        let end = rdtsc_now().unwrap();
        let _ = cal.delta_to_ns(start, end);
    }
}
