//! Typed transport wrappers that integrate [`Codec`] with the raw
//! [`FramedTransportProducer`] / [`FramedTransportConsumer`].
//!
//! These provide a clean, format-agnostic publish/recv API.
//! The serialization format is determined by the [`Codec`] impl on the
//! message type, not by the transport.

use crate::codec::{Codec, CodecError};
use crate::transport::{
    FramedTransportConsumer, FramedTransportFrame, FramedTransportProducer,
    MmapFramedTransportConsumer, MmapFramedTransportProducer, MyelonWaitStrategy, ReassemblyBuffer,
    ReceivedMessageMeta,
};
use crate::{MultiProcessResult, RequiredConsumerError, RequiredConsumerLivenessConfig};
use disruptor_mp::MmapTransportLayout;
use std::fmt;

/// Error returned by [`TypedProducer::publish_managed`] / similar
/// fallible typed-publish calls.
#[derive(Debug)]
pub enum TypedPublishError {
    /// The message-side codec failed to encode.
    Codec(CodecError),
    /// A required-consumer liveness check failed before the publish
    /// could be accepted.
    RequiredConsumer(RequiredConsumerError),
}

impl fmt::Display for TypedPublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(f, "{error}"),
            Self::RequiredConsumer(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for TypedPublishError {}

impl From<CodecError> for TypedPublishError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<RequiredConsumerError> for TypedPublishError {
    fn from(value: RequiredConsumerError) -> Self {
        Self::RequiredConsumer(value)
    }
}

/// A producer that serializes messages via [`Codec`] before publishing
/// to the underlying [`FramedTransportProducer`].
pub struct TypedProducer<F: FramedTransportFrame> {
    inner: FramedTransportProducer<F>,
    /// Frame-layer counter for `frame_publish_total` (RFC 0040).
    /// Populated by [`Self::attach_counters`]; absent ⇒ no-op.
    frame_publish_total: Option<crate::observability::CounterHandle>,
    /// Codec-layer counter for `codec_encode_total`.
    codec_encode_total: Option<crate::observability::CounterHandle>,
}

impl<F: FramedTransportFrame> TypedProducer<F> {
    /// Create a new typed producer that owns the ring buffer.
    pub fn create(name: &str, depth: usize) -> MultiProcessResult<Self> {
        Self::create_with_consumers(name, depth, 1)
    }

    /// Create a new typed producer sized for N discoverable consumers.
    pub fn create_with_consumers(
        name: &str,
        depth: usize,
        max_consumers: usize,
    ) -> MultiProcessResult<Self> {
        let inner = FramedTransportProducer::create_with_consumers(name, depth, max_consumers)?;
        Ok(Self {
            inner,
            frame_publish_total: None,
            codec_encode_total: None,
        })
    }

    /// Wrap an existing raw producer.
    pub fn from_raw(inner: FramedTransportProducer<F>) -> Self {
        Self {
            inner,
            frame_publish_total: None,
            codec_encode_total: None,
        }
    }

    /// Register typed-layer counters (`frame_publish_total`,
    /// `codec_encode_total`) in the supplied counters file. After this
    /// call, every successful `publish` increments both with one
    /// relaxed atomic each. RFC 0040 §Counters.
    pub fn attach_counters(&mut self, file: &crate::observability::CountersFile) {
        use crate::observability::{ids, COUNTER_FLAG_PRODUCER};
        self.frame_publish_total = file.register(
            ids::FRAME_PUBLISH_TOTAL,
            COUNTER_FLAG_PRODUCER,
            "frame_publish_total",
        );
        self.codec_encode_total = file.register(
            ids::CODEC_ENCODE_TOTAL,
            COUNTER_FLAG_PRODUCER,
            "codec_encode_total",
        );
    }

    /// Encode and publish a message through the ring buffer.
    #[inline(always)]
    pub fn publish<T: Codec>(&mut self, msg: &T, kind: u8) -> Result<(), CodecError> {
        let bytes = msg.encode()?;
        if let Some(h) = &self.codec_encode_total {
            h.inc();
        }
        self.inner.publish(bytes.as_ref(), kind);
        if let Some(h) = &self.frame_publish_total {
            h.inc();
        }
        Ok(())
    }

    /// Publish raw bytes (bypass codec — for pre-encoded data or compat).
    #[inline(always)]
    pub fn publish_raw(&mut self, bytes: &[u8], kind: u8) {
        self.inner.publish(bytes, kind);
        if let Some(h) = &self.frame_publish_total {
            h.inc();
        }
    }

    /// Trigger consumer discovery for backpressure.
    /// See [`FramedTransportProducer::discover_consumers`].
    pub fn discover_consumers(&mut self, timeout: std::time::Duration) -> bool {
        self.inner.discover_consumers(timeout)
    }

    /// Attach a known consumer cursor by explicit consumer id.
    pub fn discover_consumer_id(
        &mut self,
        consumer_id: &str,
        timeout: std::time::Duration,
    ) -> bool {
        self.inner.discover_consumer_id(consumer_id, timeout)
    }

    /// Forward to the underlying
    /// [`FramedTransportProducer::enable_required_consumer_liveness`].
    pub fn enable_required_consumer_liveness(
        &mut self,
        config: RequiredConsumerLivenessConfig,
    ) -> &mut Self {
        self.inner.enable_required_consumer_liveness(config);
        self
    }

    /// Variant of [`Self::publish`] that surfaces required-consumer
    /// liveness errors instead of blocking forever when a consumer
    /// configured as `required` is no longer alive.
    ///
    /// # Errors
    ///
    /// Returns [`TypedPublishError::Codec`] if encoding fails or
    /// [`TypedPublishError::RequiredConsumer`] if liveness fails.
    pub fn publish_managed<T: Codec>(
        &mut self,
        msg: &T,
        kind: u8,
    ) -> Result<(), TypedPublishError> {
        let bytes = msg.encode()?;
        self.inner.publish_managed(bytes.as_ref(), kind)?;
        Ok(())
    }

    /// Access the underlying raw producer.
    pub fn raw(&mut self) -> &mut FramedTransportProducer<F> {
        &mut self.inner
    }
}

/// A consumer that deserializes messages via [`Codec`] after receiving
/// from the underlying [`FramedTransportConsumer`].
pub struct TypedConsumer<F: FramedTransportFrame> {
    inner: FramedTransportConsumer<F>,
    /// Frame-layer counter for `frame_reassemble_count` (RFC 0040).
    frame_reassemble_count: Option<crate::observability::CounterHandle>,
    /// Codec-layer counter for `codec_decode_total`.
    codec_decode_total: Option<crate::observability::CounterHandle>,
}

impl<F: FramedTransportFrame> TypedConsumer<F> {
    /// Attach to an existing ring buffer as a typed consumer.
    pub fn attach(
        name: &str,
        depth: usize,
        wait_strategy: MyelonWaitStrategy,
    ) -> MultiProcessResult<Self> {
        let inner = FramedTransportConsumer::attach(name, depth, wait_strategy)?;
        Ok(Self {
            inner,
            frame_reassemble_count: None,
            codec_decode_total: None,
        })
    }

    /// Attach to an existing ring buffer as a typed consumer with an explicit consumer id.
    pub fn attach_with_consumer_id(
        name: &str,
        depth: usize,
        consumer_id: &str,
        wait_strategy: MyelonWaitStrategy,
    ) -> MultiProcessResult<Self> {
        let inner = FramedTransportConsumer::attach_with_consumer_id(
            name,
            depth,
            consumer_id,
            wait_strategy,
        )?;
        Ok(Self {
            inner,
            frame_reassemble_count: None,
            codec_decode_total: None,
        })
    }

    /// Wrap an existing raw consumer.
    pub fn from_raw(inner: FramedTransportConsumer<F>) -> Self {
        Self {
            inner,
            frame_reassemble_count: None,
            codec_decode_total: None,
        }
    }

    /// Register typed-layer counters (`frame_reassemble_count`,
    /// `codec_decode_total`) in the supplied counters file. RFC 0040.
    pub fn attach_counters(&mut self, file: &crate::observability::CountersFile) {
        use crate::observability::{ids, COUNTER_FLAG_CONSUMER};
        self.frame_reassemble_count = file.register(
            ids::FRAME_REASSEMBLE_COUNT,
            COUNTER_FLAG_CONSUMER,
            "frame_reassemble_count",
        );
        self.codec_decode_total = file.register(
            ids::CODEC_DECODE_TOTAL,
            COUNTER_FLAG_CONSUMER,
            "codec_decode_total",
        );
    }

    /// Receive a message through the explicit owned/copying path.
    ///
    /// Returns `(kind, message)` where `kind` is the message type discriminant.
    pub fn recv_owned<T: Codec>(&mut self) -> Result<(u8, T), CodecError> {
        let (kind, bytes) = self.inner.recv_message_blocking_owned();
        if let Some(h) = &self.frame_reassemble_count {
            h.inc();
        }
        let msg = T::decode(&bytes)?;
        if let Some(h) = &self.codec_decode_total {
            h.inc();
        }
        Ok((kind, msg))
    }

    /// Compatibility alias for the explicit owned/copying path.
    pub fn recv<T: Codec>(&mut self) -> Result<(u8, T), CodecError> {
        self.recv_owned()
    }

    /// Receive a message through the explicit owned/copying path and preserve transport metadata.
    pub fn recv_owned_with_meta<T: Codec>(
        &mut self,
    ) -> Result<(u8, T, ReceivedMessageMeta), CodecError> {
        let (meta, bytes) = self.inner.recv_message_blocking_owned_with_meta();
        let msg = T::decode(&bytes)?;
        Ok((meta.kind, msg, meta))
    }

    /// Compatibility alias for the explicit owned/copying path.
    pub fn recv_with_meta<T: Codec>(&mut self) -> Result<(u8, T, ReceivedMessageMeta), CodecError> {
        self.recv_owned_with_meta()
    }

    /// Receive raw bytes through the explicit owned/copying path.
    pub fn recv_raw_owned(&mut self) -> (u8, Vec<u8>) {
        self.inner.recv_message_blocking_owned()
    }

    /// Compatibility alias for the explicit owned/copying path.
    pub fn recv_raw(&mut self) -> (u8, Vec<u8>) {
        self.recv_raw_owned()
    }

    /// Receive raw bytes through the leased/borrowed path.
    pub fn recv_raw_leased<R>(
        &mut self,
        reassembly_buf: &mut ReassemblyBuffer,
        handler: impl FnOnce(u8, &[u8]) -> R,
    ) -> R {
        self.inner
            .recv_message_blocking_leased(reassembly_buf, handler)
    }

    /// Receive raw bytes plus transport metadata through the leased/borrowed path.
    pub fn recv_raw_leased_with_meta<R>(
        &mut self,
        reassembly_buf: &mut ReassemblyBuffer,
        handler: impl FnOnce(ReceivedMessageMeta, &[u8]) -> R,
    ) -> R {
        self.inner
            .recv_message_blocking_leased_with_meta(reassembly_buf, handler)
    }

    /// Receive and access a message through the leased/borrowed zero-copy path.
    pub fn recv_leased<T, H, R>(
        &mut self,
        reassembly_buf: &mut ReassemblyBuffer,
        mut handler: H,
    ) -> Result<R, CodecError>
    where
        T: crate::codec::ZeroCopyCodec,
        H: for<'a> FnMut(u8, T::Archived<'a>) -> R,
    {
        self.recv_leased_with_meta::<T, _, _>(reassembly_buf, |meta, archived| {
            handler(meta.kind, archived)
        })
    }

    /// Receive and access a message through the leased/borrowed zero-copy path with metadata.
    pub fn recv_leased_with_meta<T, H, R>(
        &mut self,
        reassembly_buf: &mut ReassemblyBuffer,
        mut handler: H,
    ) -> Result<R, CodecError>
    where
        T: crate::codec::ZeroCopyCodec,
        H: for<'a> FnMut(ReceivedMessageMeta, T::Archived<'a>) -> R,
    {
        self.inner
            .recv_message_blocking_leased_with_meta(reassembly_buf, |meta, bytes| {
                let archived = T::access(bytes)?;
                Ok(handler(meta, archived))
            })
    }

    /// Batch-receive and decode: processes all available messages without blocking.
    ///
    /// Uses `process_available_messages()` internally for batch frame processing,
    /// then decodes each complete message via `T::decode()`.
    /// Returns the number of messages delivered to the handler.
    #[inline(always)]
    pub fn process_available<T: Codec, H>(
        &mut self,
        reassembly_buf: &mut crate::transport::ReassemblyBuffer,
        mut handler: H,
    ) -> usize
    where
        H: FnMut(u8, T),
    {
        let mut delivered = 0usize;
        self.inner
            .process_available_messages(reassembly_buf, |kind, bytes| {
                let msg = T::decode(bytes).unwrap_or_else(|err| {
                    panic!(
                        "TypedConsumer::process_available decode failed for {} bytes: {err}",
                        bytes.len()
                    )
                });
                handler(kind, msg);
                delivered += 1;
            });
        delivered
    }

    /// Batch-receive with zero-copy decode (no allocation, no deserialization).
    ///
    /// For single-frame messages, the handler gets a zero-copy accessor into
    /// the ring buffer slot. For multi-frame messages, the accessor points
    /// into the reassembly buffer.
    ///
    /// This is the fastest possible receive path — `access()` costs ~1.4ns
    /// (pointer cast) instead of `decode()`'s ~100-500μs (full deserialization).
    ///
    /// # Requirements
    ///
    /// `T` must implement [`ZeroCopyCodec`](crate::codec::ZeroCopyCodec).
    /// Only `rkyv` and `flatbuffers` support this; bincode does not.
    #[inline(always)]
    pub fn process_available_zero_copy<T, H>(
        &mut self,
        reassembly_buf: &mut crate::transport::ReassemblyBuffer,
        mut handler: H,
    ) -> usize
    where
        T: crate::codec::ZeroCopyCodec,
        H: for<'a> FnMut(u8, T::Archived<'a>),
    {
        let mut delivered = 0usize;
        self.inner
            .process_available_messages(reassembly_buf, |kind, bytes| {
                #[cfg(feature = "dst")]
                dst_fixtures::dst_assertions::assert_sometimes(
                    true,
                    "zero-copy access used",
                    format!("bytes={}", bytes.len()),
                );
                let archived = T::access(bytes).unwrap_or_else(|err| {
                    panic!(
                        "TypedConsumer::process_available_zero_copy access failed for {} bytes: {err}",
                        bytes.len()
                    )
                });
                handler(kind, archived);
                delivered += 1;
            });
        delivered
    }

    /// Access the underlying raw consumer.
    pub fn raw(&mut self) -> &mut FramedTransportConsumer<F> {
        &mut self.inner
    }
}

/// An mmap-backed producer that serializes messages via [`Codec`] before
/// publishing to the underlying framed mmap transport.
pub struct MmapTypedProducer<F: FramedTransportFrame> {
    inner: MmapFramedTransportProducer<F>,
}

impl<F: FramedTransportFrame> MmapTypedProducer<F> {
    /// Create a new mmap-backed typed producer that owns the ring files.
    pub fn create(layout: MmapTransportLayout, depth: usize) -> MultiProcessResult<Self> {
        let inner = MmapFramedTransportProducer::create(layout, depth)?;
        Ok(Self { inner })
    }

    /// Wrap an existing raw mmap producer.
    pub fn from_raw(inner: MmapFramedTransportProducer<F>) -> Self {
        Self { inner }
    }

    /// Encode and publish a message through the ring buffer.
    #[inline(always)]
    pub fn publish<T: Codec>(&mut self, msg: &T, kind: u8) -> Result<(), CodecError> {
        let bytes = msg.encode()?;
        self.inner.publish(bytes.as_ref(), kind);
        Ok(())
    }

    /// Publish raw bytes (bypass codec).
    #[inline(always)]
    pub fn publish_raw(&mut self, bytes: &[u8], kind: u8) {
        self.inner.publish(bytes, kind);
    }

    /// Forward to the underlying
    /// [`MmapFramedTransportProducer::enable_required_consumer_liveness`].
    pub fn enable_required_consumer_liveness(
        &mut self,
        config: RequiredConsumerLivenessConfig,
    ) -> &mut Self {
        self.inner.enable_required_consumer_liveness(config);
        self
    }

    /// Forward to the underlying
    /// [`MmapFramedTransportProducer::discover_consumer_id`].
    pub fn discover_consumer_id(
        &mut self,
        consumer_id: &str,
        timeout: std::time::Duration,
    ) -> bool {
        self.inner.discover_consumer_id(consumer_id, timeout)
    }

    /// Variant of [`Self::publish`] that surfaces required-consumer
    /// liveness errors instead of blocking forever.
    ///
    /// # Errors
    ///
    /// Returns [`TypedPublishError::Codec`] if encoding fails or
    /// [`TypedPublishError::RequiredConsumer`] if liveness fails.
    pub fn publish_managed<T: Codec>(
        &mut self,
        msg: &T,
        kind: u8,
    ) -> Result<(), TypedPublishError> {
        let bytes = msg.encode()?;
        self.inner.publish_managed(bytes.as_ref(), kind)?;
        Ok(())
    }

    /// Access the underlying raw mmap producer.
    pub fn raw(&mut self) -> &mut MmapFramedTransportProducer<F> {
        &mut self.inner
    }
}

/// An mmap-backed consumer that deserializes messages via [`Codec`] after
/// receiving from the underlying framed mmap transport.
pub struct MmapTypedConsumer<F: FramedTransportFrame> {
    inner: MmapFramedTransportConsumer<F>,
}

impl<F: FramedTransportFrame> MmapTypedConsumer<F> {
    /// Attach to an existing mmap-backed ring buffer as a typed consumer.
    pub fn attach(
        layout: MmapTransportLayout,
        depth: usize,
        consumer_id: &str,
        wait_strategy: MyelonWaitStrategy,
    ) -> MultiProcessResult<Self> {
        let inner = MmapFramedTransportConsumer::attach(layout, depth, consumer_id, wait_strategy)?;
        Ok(Self { inner })
    }

    /// Wrap an existing raw mmap consumer.
    pub fn from_raw(inner: MmapFramedTransportConsumer<F>) -> Self {
        Self { inner }
    }

    /// Receive a message through the explicit owned/copying path.
    pub fn recv_owned<T: Codec>(&mut self) -> Result<(u8, T), CodecError> {
        let (kind, bytes) = self.inner.recv_message_blocking_owned();
        let msg = T::decode(&bytes)?;
        Ok((kind, msg))
    }

    /// Compatibility alias for the explicit owned/copying path.
    pub fn recv<T: Codec>(&mut self) -> Result<(u8, T), CodecError> {
        self.recv_owned()
    }

    /// Receive a message through the explicit owned/copying path and preserve transport metadata.
    pub fn recv_owned_with_meta<T: Codec>(
        &mut self,
    ) -> Result<(u8, T, ReceivedMessageMeta), CodecError> {
        let (meta, bytes) = self.inner.recv_message_blocking_owned_with_meta();
        let msg = T::decode(&bytes)?;
        Ok((meta.kind, msg, meta))
    }

    /// Compatibility alias for the explicit owned/copying path.
    pub fn recv_with_meta<T: Codec>(&mut self) -> Result<(u8, T, ReceivedMessageMeta), CodecError> {
        self.recv_owned_with_meta()
    }

    /// Receive raw bytes through the explicit owned/copying path.
    pub fn recv_raw_owned(&mut self) -> (u8, Vec<u8>) {
        self.inner.recv_message_blocking_owned()
    }

    /// Compatibility alias for the explicit owned/copying path.
    pub fn recv_raw(&mut self) -> (u8, Vec<u8>) {
        self.recv_raw_owned()
    }

    /// Receive raw bytes through the leased/borrowed path.
    pub fn recv_raw_leased<R>(
        &mut self,
        reassembly_buf: &mut ReassemblyBuffer,
        handler: impl FnOnce(u8, &[u8]) -> R,
    ) -> R {
        self.inner
            .recv_message_blocking_leased(reassembly_buf, handler)
    }

    /// Receive raw bytes plus transport metadata through the leased/borrowed path.
    pub fn recv_raw_leased_with_meta<R>(
        &mut self,
        reassembly_buf: &mut ReassemblyBuffer,
        handler: impl FnOnce(ReceivedMessageMeta, &[u8]) -> R,
    ) -> R {
        self.inner
            .recv_message_blocking_leased_with_meta(reassembly_buf, handler)
    }

    /// Receive and access a message through the leased/borrowed zero-copy path.
    pub fn recv_leased<T, H, R>(
        &mut self,
        reassembly_buf: &mut ReassemblyBuffer,
        mut handler: H,
    ) -> Result<R, CodecError>
    where
        T: crate::codec::ZeroCopyCodec,
        H: for<'a> FnMut(u8, T::Archived<'a>) -> R,
    {
        self.recv_leased_with_meta::<T, _, _>(reassembly_buf, |meta, archived| {
            handler(meta.kind, archived)
        })
    }

    /// Receive and access a message through the leased/borrowed zero-copy path with metadata.
    pub fn recv_leased_with_meta<T, H, R>(
        &mut self,
        reassembly_buf: &mut ReassemblyBuffer,
        mut handler: H,
    ) -> Result<R, CodecError>
    where
        T: crate::codec::ZeroCopyCodec,
        H: for<'a> FnMut(ReceivedMessageMeta, T::Archived<'a>) -> R,
    {
        self.inner
            .recv_message_blocking_leased_with_meta(reassembly_buf, |meta, bytes| {
                let archived = T::access(bytes)?;
                Ok(handler(meta, archived))
            })
    }

    /// Batch-receive with zero-copy decode over mmap.
    /// See [`TypedConsumer::process_available_zero_copy`] for docs.
    #[inline(always)]
    pub fn process_available_zero_copy<T, H>(
        &mut self,
        reassembly_buf: &mut crate::transport::ReassemblyBuffer,
        mut handler: H,
    ) -> usize
    where
        T: crate::codec::ZeroCopyCodec,
        H: for<'a> FnMut(u8, T::Archived<'a>),
    {
        let mut delivered = 0usize;
        self.inner
            .process_available_messages(reassembly_buf, |kind, bytes| {
                #[cfg(feature = "dst")]
                dst_fixtures::dst_assertions::assert_sometimes(
                    true,
                    "zero-copy access used",
                    format!("bytes={}", bytes.len()),
                );
                let archived = T::access(bytes).unwrap_or_else(|err| {
                    panic!(
                        "MmapTypedConsumer::process_available_zero_copy access failed for {} bytes: {err}",
                        bytes.len()
                    )
                });
                handler(kind, archived);
                delivered += 1;
            });
        delivered
    }

    /// Access the underlying raw mmap consumer.
    pub fn raw(&mut self) -> &mut MmapFramedTransportConsumer<F> {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ZeroCopyCodec;
    use crate::transport::FixedFrame;
    use crate::RequiredConsumerLivenessConfig;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    /// Trivial codec for test: just passes bytes through.
    #[derive(Debug, PartialEq, Clone)]
    struct TestMessage(Vec<u8>);

    impl Codec for TestMessage {
        type Encoded = Vec<u8>;

        fn encode(&self) -> Result<Vec<u8>, CodecError> {
            Ok(self.0.clone())
        }

        fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
            Ok(TestMessage(bytes.to_vec()))
        }
    }

    fn test_liveness_config() -> RequiredConsumerLivenessConfig {
        RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
            .with_startup_wait_timeout(std::time::Duration::from_millis(100))
            .with_progress_timeout(std::time::Duration::from_millis(20))
            .with_progress_check_interval(std::time::Duration::from_millis(1))
            .with_shutdown_grace_period(std::time::Duration::from_millis(200))
    }

    #[test]
    fn test_typed_roundtrip_through_ring() {
        use std::thread;

        let ring_name = format!("typed_rt_{}", std::process::id());
        let depth = 16;

        type Frame = FixedFrame<256>;

        let mut producer: TypedProducer<Frame> =
            TypedProducer::create(&ring_name, depth).expect("create producer");

        let consumer_name = ring_name.clone();
        let handle = thread::spawn(move || {
            let mut consumer: TypedConsumer<Frame> =
                TypedConsumer::attach(&consumer_name, depth, MyelonWaitStrategy::BusySpin)
                    .expect("attach consumer");
            let (kind, msg): (u8, TestMessage) = consumer.recv().expect("recv");
            (kind, msg)
        });

        // Give consumer time to attach
        thread::sleep(std::time::Duration::from_millis(50));

        let msg = TestMessage(vec![42, 43, 44, 45]);
        producer.publish(&msg, 7).expect("publish");

        let (kind, received) = handle.join().expect("join");
        assert_eq!(kind, 7);
        assert_eq!(received, msg);
    }

    #[test]
    fn test_typed_roundtrip_through_ring_owned_api() {
        use std::thread;

        let ring_name = format!("tor_{}", std::process::id());
        let depth = 16;

        type Frame = FixedFrame<256>;

        let mut producer: TypedProducer<Frame> =
            TypedProducer::create(&ring_name, depth).expect("create producer");

        let consumer_name = ring_name.clone();
        let handle = thread::spawn(move || {
            let mut consumer: TypedConsumer<Frame> =
                TypedConsumer::attach(&consumer_name, depth, MyelonWaitStrategy::BusySpin)
                    .expect("attach consumer");
            let (kind, msg): (u8, TestMessage) = consumer.recv_owned().expect("recv_owned");
            let (raw_kind, raw_bytes) = consumer.recv_raw_owned();
            (kind, msg, raw_kind, raw_bytes)
        });

        thread::sleep(std::time::Duration::from_millis(50));

        let msg = TestMessage(vec![42, 43, 44, 45]);
        producer.publish(&msg, 7).expect("publish");
        producer.publish_raw(&[9, 8, 7], 3);

        let (kind, received, raw_kind, raw_bytes) = handle.join().expect("join");
        assert_eq!(kind, 7);
        assert_eq!(received, msg);
        assert_eq!(raw_kind, 3);
        assert_eq!(raw_bytes, vec![9, 8, 7]);
    }

    #[test]
    fn typed_publish_managed_reports_missing_required_consumer_at_startup() {
        let ring_name = format!("tmsu_{}", std::process::id());
        let depth = 4;
        type Frame = FixedFrame<256>;

        let mut producer: TypedProducer<Frame> =
            TypedProducer::create_with_consumers(&ring_name, depth, 2).expect("create producer");
        producer.enable_required_consumer_liveness(test_liveness_config());

        let _consumer1: TypedConsumer<Frame> = TypedConsumer::attach_with_consumer_id(
            &ring_name,
            depth,
            "c1",
            MyelonWaitStrategy::BusySpin,
        )
        .expect("attach c1");

        let msg = TestMessage(vec![1, 2, 3, 4]);
        let error = producer
            .publish_managed(&msg, 9)
            .expect_err("missing c2 should trip startup timeout");

        assert!(matches!(
            error,
            TypedPublishError::RequiredConsumer(RequiredConsumerError::StartupTimeout { missing })
            if missing == vec!["c2".to_string()]
        ));
    }

    #[test]
    fn typed_publish_managed_recovers_when_same_consumer_id_rejoins() {
        let ring_name = format!("tmrj_{}", std::process::id());
        let depth = 4;
        type Frame = FixedFrame<256>;

        let mut producer: TypedProducer<Frame> =
            TypedProducer::create_with_consumers(&ring_name, depth, 2).expect("create producer");
        producer.enable_required_consumer_liveness(test_liveness_config());

        let stop_consumer1 = Arc::new(AtomicBool::new(false));
        let stop_consumer1_thread = Arc::clone(&stop_consumer1);
        let consumer_name = ring_name.clone();
        let consumer1_thread = std::thread::spawn(move || {
            let mut consumer: TypedConsumer<Frame> = TypedConsumer::attach_with_consumer_id(
                &consumer_name,
                depth,
                "c1",
                MyelonWaitStrategy::BusySpin,
            )
            .expect("attach c1");
            let mut reassembly = crate::transport::ReassemblyBuffer::new(1024);
            while !stop_consumer1_thread.load(Ordering::Acquire) {
                let processed =
                    consumer.process_available::<TestMessage, _>(&mut reassembly, |_kind, _msg| {});
                if processed == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        });

        let mut consumer2: TypedConsumer<Frame> = TypedConsumer::attach_with_consumer_id(
            &ring_name,
            depth,
            "c2",
            MyelonWaitStrategy::BusySpin,
        )
        .expect("attach c2");

        producer
            .publish_managed(&TestMessage(vec![9, 9, 9]), 1)
            .expect("seed publish");
        let (kind, seed): (u8, TestMessage) = consumer2.recv_owned().expect("seed recv");
        assert_eq!(kind, 1);
        assert_eq!(seed, TestMessage(vec![9, 9, 9]));
        drop(consumer2);

        for i in 0..depth {
            producer
                .publish_managed(&TestMessage(vec![i as u8; 8]), 2)
                .expect("fill backlog before rejoin");
        }

        let ring_name_for_rejoin = ring_name.clone();
        let rejoin_thread = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(40));
            let mut consumer: TypedConsumer<Frame> = TypedConsumer::attach_with_consumer_id(
                &ring_name_for_rejoin,
                depth,
                "c2",
                MyelonWaitStrategy::BusySpin,
            )
            .expect("reattach c2");
            let mut reassembly = crate::transport::ReassemblyBuffer::new(1024);
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
            let mut consumed = 0usize;
            while std::time::Instant::now() < deadline && consumed < depth + 2 {
                let processed =
                    consumer.process_available::<TestMessage, _>(&mut reassembly, |_kind, _msg| {
                        consumed += 1;
                    });
                if processed == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
            consumed
        });

        producer
            .publish_managed(&TestMessage(vec![7, 7, 7]), 3)
            .expect("same-id typed rejoin should recover");

        stop_consumer1.store(true, Ordering::Release);
        consumer1_thread.join().expect("join c1");
        let consumed = rejoin_thread.join().expect("join c2");
        assert!(consumed > 0, "rejoined typed consumer should drain backlog");
    }

    // --- ZC-08: Integration test — produce rkyv → consume zero-copy → verify fields ---

    /// A u64 pair with zero-copy access via pointer cast.
    #[derive(Debug, PartialEq, Clone)]
    struct ZcPair {
        a: u64,
        b: u64,
    }

    #[repr(C, packed)]
    struct ArchivedZcPair {
        a: u64,
        b: u64,
    }

    impl Codec for ZcPair {
        type Encoded = Vec<u8>;
        fn encode(&self) -> Result<Vec<u8>, CodecError> {
            let mut buf = Vec::with_capacity(16);
            buf.extend_from_slice(&self.a.to_le_bytes());
            buf.extend_from_slice(&self.b.to_le_bytes());
            Ok(buf)
        }
        fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
            if bytes.len() < 16 {
                return Err(CodecError::decode("too short"));
            }
            Ok(ZcPair {
                a: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                b: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            })
        }
    }

    impl crate::codec::ZeroCopyCodec for ZcPair {
        type Archived<'a> = &'a ArchivedZcPair;
        fn access<'a>(bytes: &'a [u8]) -> Result<Self::Archived<'a>, CodecError> {
            if bytes.len() < 16 {
                return Err(CodecError::decode("too short"));
            }
            // SAFETY: ArchivedZcPair is repr(C, packed) so Rust generates unaligned
            // reads for field access. This handles the 10-byte frame header misalignment.
            let ptr = bytes.as_ptr() as *const ArchivedZcPair;
            Ok(unsafe { &*ptr })
        }
    }

    #[test]
    fn zc08_produce_rkyv_consume_zero_copy_verify_fields() {
        use crate::transport::ReassemblyBuffer;
        use std::thread;

        let ring_name = format!("zc08_{}", std::process::id());
        let depth = 16;
        type Frame = FixedFrame<256>;

        let mut producer: TypedProducer<Frame> =
            TypedProducer::create(&ring_name, depth).expect("create producer");

        let consumer_name = ring_name.clone();
        let handle = thread::spawn(move || {
            let mut consumer: TypedConsumer<Frame> =
                TypedConsumer::attach(&consumer_name, depth, MyelonWaitStrategy::BusySpin)
                    .expect("attach consumer");
            let mut reassembly = ReassemblyBuffer::new(4096);
            let mut results = Vec::new();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while results.len() < 3 {
                consumer.process_available_zero_copy::<ZcPair, _>(
                    &mut reassembly,
                    |kind, archived| {
                        results.push((kind, archived.a, archived.b));
                    },
                );
                if std::time::Instant::now() > deadline {
                    panic!(
                        "ZC-08: timed out waiting for 3 messages (got {})",
                        results.len()
                    );
                }
                std::hint::spin_loop();
            }
            results
        });

        thread::sleep(std::time::Duration::from_millis(50));
        producer.discover_consumers(std::time::Duration::from_secs(3));

        producer
            .publish(&ZcPair { a: 100, b: 200 }, 1)
            .expect("pub 1");
        producer
            .publish(&ZcPair { a: 300, b: 400 }, 2)
            .expect("pub 2");
        producer
            .publish(&ZcPair { a: 500, b: 600 }, 3)
            .expect("pub 3");

        let results = handle.join().expect("join");
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (1, 100, 200));
        assert_eq!(results[1], (2, 300, 400));
        assert_eq!(results[2], (3, 500, 600));
    }

    #[test]
    fn recv_leased_zero_copy_single_frame_verify_fields() {
        use crate::transport::ReassemblyBuffer;
        use std::thread;

        let ring_name = format!("zcl_{}", std::process::id());
        let depth = 16;
        type Frame = FixedFrame<256>;

        let mut producer: TypedProducer<Frame> =
            TypedProducer::create(&ring_name, depth).expect("create producer");

        let consumer_name = ring_name.clone();
        let handle = thread::spawn(move || {
            let mut consumer: TypedConsumer<Frame> =
                TypedConsumer::attach(&consumer_name, depth, MyelonWaitStrategy::BusySpin)
                    .expect("attach consumer");
            let mut reassembly = ReassemblyBuffer::new(4096);
            consumer
                .recv_leased::<ZcPair, _, _>(&mut reassembly, |kind, archived| {
                    (kind, archived.a, archived.b)
                })
                .expect("recv_leased")
        });

        thread::sleep(std::time::Duration::from_millis(50));
        producer.discover_consumers(std::time::Duration::from_secs(3));
        producer
            .publish(&ZcPair { a: 111, b: 222 }, 9)
            .expect("publish");

        let result = handle.join().expect("join");
        assert_eq!(result, (9, 111, 222));
    }

    #[test]
    fn recv_leased_zero_copy_fragmented_message_verify_fields() {
        use crate::transport::ReassemblyBuffer;
        use std::thread;

        let ring_name = format!("zclf_{}", std::process::id());
        let depth = 16;
        type Frame = FixedFrame<8>;

        let mut producer: TypedProducer<Frame> =
            TypedProducer::create(&ring_name, depth).expect("create producer");

        let consumer_name = ring_name.clone();
        let handle = thread::spawn(move || {
            let mut consumer: TypedConsumer<Frame> =
                TypedConsumer::attach(&consumer_name, depth, MyelonWaitStrategy::BusySpin)
                    .expect("attach consumer");
            let mut reassembly = ReassemblyBuffer::new(64);
            consumer
                .recv_leased_with_meta::<ZcPair, _, _>(&mut reassembly, |meta, archived| {
                    (meta.kind, meta.msg_id, archived.a, archived.b)
                })
                .expect("recv_leased_with_meta")
        });

        thread::sleep(std::time::Duration::from_millis(50));
        producer.discover_consumers(std::time::Duration::from_secs(3));
        producer
            .publish(&ZcPair { a: 777, b: 888 }, 4)
            .expect("publish");

        let result = handle.join().expect("join");
        assert_eq!(result.0, 4);
        assert!(result.1 > 0);
        assert_eq!(result.2, 777);
        assert_eq!(result.3, 888);
    }

    // --- ZC-10: Phase timing — access() vs decode() latency ---

    #[test]
    fn zc10_access_is_faster_than_decode() {
        let pair = ZcPair {
            a: 0xDEAD,
            b: 0xBEEF,
        };
        let encoded = pair.encode().unwrap();

        // Warm up
        for _ in 0..1000 {
            let _ = ZcPair::access(&encoded);
            let _ = ZcPair::decode(&encoded);
        }

        let iterations = 100_000;

        // Time access (zero-copy pointer cast)
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let archived = ZcPair::access(&encoded).unwrap();
            std::hint::black_box(archived.a + archived.b);
        }
        let access_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

        // Time decode (full deserialization)
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let decoded = ZcPair::decode(&encoded).unwrap();
            std::hint::black_box(decoded.a + decoded.b);
        }
        let decode_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

        eprintln!(
            "access: {access_ns:.1}ns, decode: {decode_ns:.1}ns, speedup: {:.1}x",
            decode_ns / access_ns
        );

        // access should be at least 2x faster than decode
        assert!(
            access_ns < decode_ns,
            "access ({access_ns:.1}ns) should be faster than decode ({decode_ns:.1}ns)"
        );
    }

    #[test]
    fn typed_layer_counters_record_publish_and_recv() {
        use crate::observability::{ids, CountersFile, COUNTERS_FILE_RESERVED_BYTES};
        use std::ptr::NonNull;
        use std::thread;

        type Frame = FixedFrame<256>;
        let ring_name = format!("typed_obs_{}", std::process::id());
        let depth = 16;

        // Counters file lives on a 64-byte aligned heap region.
        #[repr(C, align(64))]
        struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);
        let mut region = Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES]));
        let region_addr = region.0.as_mut_ptr() as usize;
        let file = unsafe { CountersFile::init(NonNull::new(region.0.as_mut_ptr()).unwrap()) };

        let mut producer: TypedProducer<Frame> =
            TypedProducer::create(&ring_name, depth).expect("create producer");
        producer.attach_counters(&file);

        let consumer_name = ring_name.clone();
        let handle = thread::spawn(move || {
            // Consumer attaches in its own thread; counters file is the
            // same shared region (we re-attach via raw addr to avoid
            // sending CountersFile across threads).
            let mut consumer: TypedConsumer<Frame> =
                TypedConsumer::attach(&consumer_name, depth, MyelonWaitStrategy::BusySpin)
                    .expect("attach consumer");
            let view =
                unsafe { CountersFile::attach(NonNull::new(region_addr as *mut u8).unwrap()) }
                    .expect("attach counters from consumer thread");
            consumer.attach_counters(&view);
            let (kind, _msg): (u8, TestMessage) = consumer.recv_owned().expect("recv");
            kind
        });

        thread::sleep(std::time::Duration::from_millis(50));

        let msg = TestMessage(vec![1, 2, 3, 4, 5]);
        producer.publish(&msg, 9).expect("publish");

        let kind = handle.join().expect("join");
        assert_eq!(kind, 9);

        let snap = file.snapshot();
        let by_id = |id: u32| {
            snap.iter()
                .find(|c| c.id == id)
                .cloned()
                .unwrap_or_else(|| panic!("missing counter id 0x{id:x}"))
        };
        // Producer-side: codec_encode_total + frame_publish_total each
        // fired exactly once.
        assert_eq!(by_id(ids::CODEC_ENCODE_TOTAL).value, 1);
        assert_eq!(by_id(ids::FRAME_PUBLISH_TOTAL).value, 1);
        // Consumer-side: frame_reassemble_count + codec_decode_total
        // each fired exactly once.
        assert_eq!(by_id(ids::FRAME_REASSEMBLE_COUNT).value, 1);
        assert_eq!(by_id(ids::CODEC_DECODE_TOTAL).value, 1);
    }
}
