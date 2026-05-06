//! Aeron-style counters file for low-overhead observability.
//!
//! See RFC 0040. Each shared-memory segment reserves a small header zone
//! holding an array of cache-line-padded `CounterSlot`s. Writers (producer
//! and consumer hot paths) increment a slot via a single relaxed atomic
//! `fetch_add`; out-of-process readers attach to the segment and walk the
//! same array.
//!
//! Cost model:
//! - hot path increment ≈ 1 ns on x86, 1.5 ns on aarch64 (relaxed atomic).
//! - reader path is non-blocking; counters are eventually-consistent
//!   monotonic values — no synchronisation needed with the writer.
//!
//! Submodules:
//! - [`aggregator`] (feature `metrics`) — worker thread that pumps the
//!   counters file into the `metrics`-rs facade on a periodic interval,
//!   bridging hot-path counters to Prometheus / OTLP / any other
//!   recorder the embedding application installs.

#[cfg(feature = "metrics")]
pub mod aggregator;
#[cfg(feature = "metrics")]
pub use aggregator::{AggregatorConfig, AggregatorHandle};

use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Magic sentinel marking a valid counters file header. Spells "MYC0" in
/// little-endian — Myelon Counters v0.
pub const COUNTERS_MAGIC: u32 = u32::from_le_bytes(*b"MYC0");

/// Bytes reserved at the top of a shared-memory segment for the counters
/// file. Holds the header plus up to ~62 fixed-size slots, which is well
/// above the per-segment counter set defined in RFC 0040.
pub const COUNTERS_FILE_RESERVED_BYTES: usize = 4096;

/// Cache line size assumed for slot alignment / inter-counter padding.
pub const CACHE_LINE_BYTES: usize = 64;

/// Maximum number of counter slots a single segment can host. Picked so
/// `header + MAX_SLOTS * CounterSlot` fits inside `COUNTERS_FILE_RESERVED_BYTES`.
pub const MAX_COUNTER_SLOTS: usize = 62;

/// Maximum bytes available for a counter label, null-padded UTF-8.
pub const COUNTER_LABEL_BYTES: usize = 48;

/// Slot is in use (writer claimed it).
pub const COUNTER_FLAG_IN_USE: u32 = 1 << 0;
/// Slot belongs to a producer hot path.
pub const COUNTER_FLAG_PRODUCER: u32 = 1 << 1;
/// Slot belongs to a consumer hot path.
pub const COUNTER_FLAG_CONSUMER: u32 = 1 << 2;

/// Header for the counters file region. Lives at the start of the
/// reserved zone described in RFC 0040.
#[repr(C, align(64))]
pub struct CountersHeader {
    /// `COUNTERS_MAGIC` when valid.
    pub magic: u32,
    /// Layout version; bumped when slot stride or header shape changes.
    pub version: u32,
    /// Number of slots actually in use; advances monotonically.
    pub slot_count: AtomicU32,
    /// Maximum slots the array can hold (always `MAX_COUNTER_SLOTS`).
    pub slot_capacity: u32,
    /// Bytes per slot (`size_of::<CounterSlot>()`); included so external
    /// readers don't need to depend on this crate's structs.
    pub slot_stride: u32,
    /// Bytes from the start of this header to the first slot.
    pub slots_offset: u32,
    /// Reserved for future use; pad up to one cache line.
    _reserved: [u8; 64 - 24],
}

const _: () = assert!(std::mem::size_of::<CountersHeader>() == CACHE_LINE_BYTES);

/// One Aeron-style counter slot. `#[repr(C, align(64))]` keeps each slot
/// on its own cache line so writers don't false-share with readers or
/// neighbouring slots.
#[repr(C, align(64))]
pub struct CounterSlot {
    /// Stable counter identifier (see RFC 0040 §Counters).
    pub id: u32,
    /// `COUNTER_FLAG_*` bits.
    pub flags: AtomicU32,
    /// Monotonic counter value.
    pub value: AtomicU64,
    /// Null-padded UTF-8 label, e.g. `"events_published"`.
    pub label: [u8; COUNTER_LABEL_BYTES],
}

const _: () = assert!(std::mem::size_of::<CounterSlot>() == CACHE_LINE_BYTES);

/// Mutable handle to a `CounterSlot`.
///
/// Created by writers (producer / consumer construction) and held by
/// reference for the lifetime of the segment. Increments are relaxed
/// atomic; reads from a separate thread see eventually-consistent
/// values.
#[derive(Clone, Copy, Debug)]
pub struct CounterHandle {
    inner: NonNull<CounterSlot>,
}

unsafe impl Send for CounterHandle {}
unsafe impl Sync for CounterHandle {}

impl CounterHandle {
    /// Build a handle from a slot pointer. Caller must ensure the slot
    /// lives at least as long as the handle (typically tied to a shared
    /// memory mapping).
    ///
    /// # Safety
    /// `ptr` must point to a valid `CounterSlot` in a mapping that
    /// outlives the returned handle.
    pub unsafe fn from_ptr(ptr: NonNull<CounterSlot>) -> Self {
        Self { inner: ptr }
    }

    /// Increment by 1.
    #[inline(always)]
    pub fn inc(&self) {
        // SAFETY: handle was constructed from a valid slot pointer with
        // a lifetime tied to the underlying mapping.
        let slot = unsafe { self.inner.as_ref() };
        slot.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Add `n`.
    #[inline(always)]
    pub fn add(&self, n: u64) {
        let slot = unsafe { self.inner.as_ref() };
        slot.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Update the stored maximum if `value` exceeds the current
    /// reading. Useful for high-water-mark counters such as
    /// `consumer_lag_max`.
    #[inline(always)]
    pub fn record_max(&self, value: u64) {
        let slot = unsafe { self.inner.as_ref() };
        let mut current = slot.value.load(Ordering::Relaxed);
        while value > current {
            match slot.value.compare_exchange_weak(
                current,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Read the current value (relaxed).
    #[inline]
    pub fn get(&self) -> u64 {
        let slot = unsafe { self.inner.as_ref() };
        slot.value.load(Ordering::Relaxed)
    }
}

/// View over a counters file region. Owns no memory; constructed from a
/// pointer into a shared-memory mapping or a stack buffer (test only).
#[derive(Debug)]
pub struct CountersFile {
    base: NonNull<u8>,
    len: usize,
}

unsafe impl Send for CountersFile {}
unsafe impl Sync for CountersFile {}

impl CountersFile {
    /// Initialise a freshly-zeroed reserved region as a counters file.
    /// Must be called exactly once per segment, by whichever process
    /// creates the segment.
    ///
    /// # Safety
    /// `ptr` must point to at least `COUNTERS_FILE_RESERVED_BYTES` of
    /// writable memory that outlives the returned view, and that memory
    /// must be zero-initialised before this call.
    pub unsafe fn init(ptr: NonNull<u8>) -> Self {
        let header_ptr = ptr.as_ptr() as *mut CountersHeader;
        let slots_offset = std::mem::size_of::<CountersHeader>() as u32;
        // SAFETY: caller's contract guarantees `ptr` is writable for at
        // least `COUNTERS_FILE_RESERVED_BYTES` and that the memory is
        // zero-initialised, which is what `write` requires here.
        unsafe {
            std::ptr::write(
                header_ptr,
                CountersHeader {
                    magic: COUNTERS_MAGIC,
                    version: 0,
                    slot_count: AtomicU32::new(0),
                    slot_capacity: MAX_COUNTER_SLOTS as u32,
                    slot_stride: std::mem::size_of::<CounterSlot>() as u32,
                    slots_offset,
                    _reserved: [0u8; 64 - 24],
                },
            );
        }
        // Slots are already zeroed by the caller's mmap/SHM allocation.
        Self {
            base: ptr,
            len: COUNTERS_FILE_RESERVED_BYTES,
        }
    }

    /// Attach to an existing counters file region. Validates the magic
    /// and version.
    ///
    /// # Safety
    /// `ptr` must point to at least `COUNTERS_FILE_RESERVED_BYTES` of
    /// readable memory whose lifetime contains the returned view. The
    /// memory must already have been initialised by a writer.
    pub unsafe fn attach(ptr: NonNull<u8>) -> Result<Self, AttachError> {
        // SAFETY: caller's contract guarantees `ptr` points at an
        // initialised `CountersHeader` whose backing memory remains
        // valid for at least the lifetime of the returned view.
        let header = unsafe { &*(ptr.as_ptr() as *const CountersHeader) };
        if header.magic != COUNTERS_MAGIC {
            return Err(AttachError::BadMagic(header.magic));
        }
        if header.version != 0 {
            return Err(AttachError::UnsupportedVersion(header.version));
        }
        Ok(Self {
            base: ptr,
            len: COUNTERS_FILE_RESERVED_BYTES,
        })
    }

    /// Header reference.
    #[inline]
    pub fn header(&self) -> &CountersHeader {
        unsafe { &*(self.base.as_ptr() as *const CountersHeader) }
    }

    fn slot_ptr(&self, idx: usize) -> NonNull<CounterSlot> {
        let header = self.header();
        let off = header.slots_offset as usize + idx * std::mem::size_of::<CounterSlot>();
        debug_assert!(off + std::mem::size_of::<CounterSlot>() <= self.len);
        // SAFETY: bounds checked above; offset comes from the in-band header.
        unsafe { NonNull::new_unchecked(self.base.as_ptr().add(off) as *mut CounterSlot) }
    }

    /// Reserve a new slot, populate `id`, `flags`, and `label`, and
    /// return a writer handle. Returns `None` when the slot capacity is
    /// exhausted.
    pub fn register(&self, id: u32, flags: u32, label: &str) -> Option<CounterHandle> {
        let header = self.header();
        let idx = header.slot_count.fetch_add(1, Ordering::AcqRel) as usize;
        if idx >= MAX_COUNTER_SLOTS {
            // Roll back so subsequent register() calls don't keep
            // incrementing past capacity.
            header.slot_count.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        let ptr = self.slot_ptr(idx);
        // SAFETY: pointer is in-bounds and not yet observed by readers
        // — slot_count was incremented above before any writer or reader
        // could see this index, and the Release on `flags` below
        // publishes the populated slot.
        unsafe {
            let raw = ptr.as_ptr();
            std::ptr::addr_of_mut!((*raw).id).write(id);
            let label_ptr = std::ptr::addr_of_mut!((*raw).label);
            (*label_ptr).fill(0);
            let bytes = label.as_bytes();
            let n = bytes.len().min(COUNTER_LABEL_BYTES);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), label_ptr.cast::<u8>(), n);
            // value left at its zero initialisation.
            (*raw)
                .flags
                .store(flags | COUNTER_FLAG_IN_USE, Ordering::Release);
        }
        Some(unsafe { CounterHandle::from_ptr(ptr) })
    }

    /// Snapshot all in-use slots — `(id, flags, value, label)` tuples.
    /// Intended for external readers and tests.
    pub fn snapshot(&self) -> Vec<CounterSnapshot> {
        let header = self.header();
        let count = header.slot_count.load(Ordering::Acquire) as usize;
        let count = count.min(MAX_COUNTER_SLOTS);
        let mut out = Vec::with_capacity(count);
        for idx in 0..count {
            let ptr = self.slot_ptr(idx);
            let slot = unsafe { ptr.as_ref() };
            let flags = slot.flags.load(Ordering::Acquire);
            if flags & COUNTER_FLAG_IN_USE == 0 {
                continue;
            }
            let label_len = slot
                .label
                .iter()
                .position(|b| *b == 0)
                .unwrap_or(COUNTER_LABEL_BYTES);
            let label = String::from_utf8_lossy(&slot.label[..label_len]).into_owned();
            out.push(CounterSnapshot {
                id: slot.id,
                flags,
                value: slot.value.load(Ordering::Relaxed),
                label,
            });
        }
        out
    }
}

/// Error returned by [`CountersFile::attach`].
#[derive(Debug)]
pub enum AttachError {
    /// Magic word didn't match `COUNTERS_MAGIC`.
    BadMagic(u32),
    /// Header version is from a newer layout this build doesn't know.
    UnsupportedVersion(u32),
}

impl std::fmt::Display for AttachError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttachError::BadMagic(m) => write!(f, "counters file: bad magic 0x{m:08x}"),
            AttachError::UnsupportedVersion(v) => {
                write!(f, "counters file: unsupported version {v}")
            }
        }
    }
}

impl std::error::Error for AttachError {}

/// Plain-data view of one counter slot, useful for tests and external
/// readers (e.g. a future `myelon-stat` binary).
#[derive(Debug, Clone)]
pub struct CounterSnapshot {
    /// Stable counter identifier.
    pub id: u32,
    /// `COUNTER_FLAG_*` bits.
    pub flags: u32,
    /// Current value at snapshot time.
    pub value: u64,
    /// Decoded label.
    pub label: String,
}

// ---------- counter ID registry --------------------------------------------

/// Stable counter IDs used by `disruptor-mp`. See RFC 0040 §Counters for
/// the canonical table. Values above 0x100 are reserved for application
/// extension via [`CountersFile::register`].
pub mod ids {
    // Producer hot path
    /// `events_published` — successful `publish()` calls.
    pub const EVENTS_PUBLISHED: u32 = 0x10;
    /// `events_published_bytes` — cumulative payload bytes published.
    pub const EVENTS_PUBLISHED_BYTES: u32 = 0x11;
    /// `producer_full_events` — `publish()` saw ring full.
    pub const PRODUCER_FULL_EVENTS: u32 = 0x12;
    /// `producer_park_count` — publisher parked on full.
    pub const PRODUCER_PARK_COUNT: u32 = 0x13;
    /// `producer_unpark_count` — publisher unparked by consumer.
    pub const PRODUCER_UNPARK_COUNT: u32 = 0x14;

    // Consumer hot path
    /// `events_consumed` — successful `try_consume_next()` calls.
    pub const EVENTS_CONSUMED: u32 = 0x20;
    /// `events_consumed_bytes` — cumulative payload bytes consumed.
    pub const EVENTS_CONSUMED_BYTES: u32 = 0x21;
    /// `consumer_empty_spins` — `try_consume_next` saw ring empty.
    pub const CONSUMER_EMPTY_SPINS: u32 = 0x22;
    /// `consumer_park_count` — consumer parked on empty.
    pub const CONSUMER_PARK_COUNT: u32 = 0x23;
    /// `consumer_unpark_count` — consumer unparked by producer.
    pub const CONSUMER_UNPARK_COUNT: u32 = 0x24;
    /// `consumer_lag_max` — high-water mark of `producer_seq − consumer_seq`.
    pub const CONSUMER_LAG_MAX: u32 = 0x25;

    // Coordination (myelon typed layer)
    /// `frame_publish_total` — `FramedTransport::publish()` calls.
    pub const FRAME_PUBLISH_TOTAL: u32 = 0x30;
    /// `frame_fragment_count` — frames split across slots.
    pub const FRAME_FRAGMENT_COUNT: u32 = 0x31;
    /// `frame_reassemble_count` — frames reassembled on consume.
    pub const FRAME_REASSEMBLE_COUNT: u32 = 0x32;
    /// `codec_encode_total` — `Codec::encode()` calls.
    pub const CODEC_ENCODE_TOTAL: u32 = 0x33;
    /// `codec_decode_total` — `Codec::decode()` calls.
    pub const CODEC_DECODE_TOTAL: u32 = 0x34;

    /// First ID reserved for application extension.
    pub const APP_RESERVED_BASE: u32 = 0x100;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 64-byte aligned region matching the layout expectations of
    /// `CountersHeader` / `CounterSlot`. Real shared-memory mappings
    /// satisfy this alignment naturally; tests allocate it explicitly.
    #[repr(C, align(64))]
    struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);

    /// Allocate a zeroed, cache-line-aligned region. Heap-resident so
    /// the pointer stays stable for the lifetime of the test.
    fn fresh_region() -> Box<AlignedRegion> {
        Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES]))
    }

    fn ptr_from(buf: &mut AlignedRegion) -> NonNull<u8> {
        NonNull::new(buf.0.as_mut_ptr()).unwrap()
    }

    #[test]
    fn header_is_one_cache_line() {
        assert_eq!(std::mem::size_of::<CountersHeader>(), CACHE_LINE_BYTES);
    }

    #[test]
    fn slot_is_one_cache_line() {
        assert_eq!(std::mem::size_of::<CounterSlot>(), CACHE_LINE_BYTES);
    }

    #[test]
    fn header_plus_slots_fit_in_reserved_region() {
        let bytes = std::mem::size_of::<CountersHeader>()
            + MAX_COUNTER_SLOTS * std::mem::size_of::<CounterSlot>();
        assert!(bytes <= COUNTERS_FILE_RESERVED_BYTES);
    }

    #[test]
    fn init_then_attach_roundtrip() {
        let mut buf = fresh_region();
        let file = unsafe { CountersFile::init(ptr_from(&mut buf)) };
        let header = file.header();
        assert_eq!(header.magic, COUNTERS_MAGIC);
        assert_eq!(header.slot_capacity, MAX_COUNTER_SLOTS as u32);
        assert_eq!(
            header.slot_stride as usize,
            std::mem::size_of::<CounterSlot>()
        );
        // Re-attach to the same memory; the writer view above shares
        // the buffer non-mutably, so simply let it fall out of scope.
        let _ = file;
        let attached = unsafe { CountersFile::attach(ptr_from(&mut buf)) }.unwrap();
        assert_eq!(attached.header().magic, COUNTERS_MAGIC);
    }

    #[test]
    fn attach_rejects_bad_magic() {
        let mut buf = fresh_region();
        // Don't init — magic stays zero.
        let err = unsafe { CountersFile::attach(ptr_from(&mut buf)) }.unwrap_err();
        match err {
            AttachError::BadMagic(0) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn register_inc_get_snapshot_roundtrip() {
        let mut buf = fresh_region();
        let file = unsafe { CountersFile::init(ptr_from(&mut buf)) };

        let pub_h = file
            .register(
                ids::EVENTS_PUBLISHED,
                COUNTER_FLAG_PRODUCER,
                "events_published",
            )
            .unwrap();
        let con_h = file
            .register(
                ids::EVENTS_CONSUMED,
                COUNTER_FLAG_CONSUMER,
                "events_consumed",
            )
            .unwrap();
        let lag_h = file
            .register(
                ids::CONSUMER_LAG_MAX,
                COUNTER_FLAG_CONSUMER,
                "consumer_lag_max",
            )
            .unwrap();

        for _ in 0..123 {
            pub_h.inc();
        }
        con_h.add(456);
        lag_h.record_max(7);
        lag_h.record_max(3); // lower than current; ignored.
        lag_h.record_max(11);

        assert_eq!(pub_h.get(), 123);
        assert_eq!(con_h.get(), 456);
        assert_eq!(lag_h.get(), 11);

        let snap = file.snapshot();
        let by_id = |id: u32| snap.iter().find(|c| c.id == id).cloned();
        let p = by_id(ids::EVENTS_PUBLISHED).unwrap();
        assert_eq!(p.value, 123);
        assert_eq!(p.label, "events_published");
        assert_eq!(p.flags & COUNTER_FLAG_IN_USE, COUNTER_FLAG_IN_USE);
        assert_eq!(p.flags & COUNTER_FLAG_PRODUCER, COUNTER_FLAG_PRODUCER);
        let c = by_id(ids::EVENTS_CONSUMED).unwrap();
        assert_eq!(c.value, 456);
        assert_eq!(c.flags & COUNTER_FLAG_CONSUMER, COUNTER_FLAG_CONSUMER);
        let lag = by_id(ids::CONSUMER_LAG_MAX).unwrap();
        assert_eq!(lag.value, 11);
    }

    #[test]
    fn register_returns_none_at_capacity() {
        let mut buf = fresh_region();
        let file = unsafe { CountersFile::init(ptr_from(&mut buf)) };
        for i in 0..MAX_COUNTER_SLOTS {
            let h = file.register(i as u32, COUNTER_FLAG_PRODUCER, "x");
            assert!(h.is_some(), "slot {i} should fit");
        }
        assert!(file
            .register(0xDEAD, COUNTER_FLAG_PRODUCER, "overflow")
            .is_none());
        // slot_count must be clamped (rolled back when over-capacity).
        assert_eq!(
            file.header().slot_count.load(Ordering::Acquire) as usize,
            MAX_COUNTER_SLOTS
        );
    }

    #[test]
    fn external_attach_sees_writer_increments() {
        // Simulates a separate "process": one CountersFile initialises &
        // writes, a second view attaches to the same memory and reads.
        let mut buf = fresh_region();
        // Take the raw pointer once so both views share it without
        // creating overlapping mutable borrows of `buf`.
        let raw = ptr_from(&mut buf);
        let writer = unsafe { CountersFile::init(raw) };
        let h = writer
            .register(
                ids::EVENTS_PUBLISHED,
                COUNTER_FLAG_PRODUCER,
                "events_published",
            )
            .unwrap();
        for _ in 0..100 {
            h.inc();
        }
        let reader = unsafe { CountersFile::attach(raw) }.unwrap();
        let snap = reader.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].id, ids::EVENTS_PUBLISHED);
        assert_eq!(snap[0].value, 100);
        assert_eq!(snap[0].label, "events_published");
    }
}
