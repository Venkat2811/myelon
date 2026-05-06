//! Zero-copy message codec for the Myelon transport.
//!
//! The [`Codec`] trait is the contract between the transport layer and
//! the serialization format. Applications implement it for their domain
//! types. The transport is format-agnostic — it moves bytes, not objects.
//!
//! Which format backs the impl (`rkyv`, `flatbuffers`, `bincode`, raw
//! bytes) is the application's choice.

use std::fmt;

/// Error type for codec encode/decode operations.
#[derive(Debug)]
pub enum CodecError {
    /// Serialization failed.
    Encode(String),
    /// Deserialization failed.
    Decode(String),
}

impl CodecError {
    /// Create an encode error from any displayable value.
    pub fn encode(e: impl fmt::Display) -> Self {
        CodecError::Encode(e.to_string())
    }

    /// Create a decode error from any displayable value.
    pub fn decode(e: impl fmt::Display) -> Self {
        CodecError::Decode(e.to_string())
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::Encode(msg) => write!(f, "codec encode: {msg}"),
            CodecError::Decode(msg) => write!(f, "codec decode: {msg}"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Zero-copy message codec for the Myelon transport.
///
/// Implementations serialize domain types into bytes for the ring buffer
/// and deserialize them on the consumer side.
///
/// # Design
///
/// - The trait is intentionally minimal — encode to bytes, decode from bytes.
/// - The transport calls [`encode`](Codec::encode) on publish and
///   [`decode`](Codec::decode) on receive.
/// - The `Encoded` associated type allows zero-alloc encoders (e.g.
///   `rkyv::util::AlignedVec`) without forcing `Vec<u8>`.
///
/// # Example
///
/// ```ignore
/// use myelon::codec::{Codec, CodecError};
///
/// struct MyMessage { id: u64, data: Vec<u8> }
///
/// impl Codec for MyMessage {
///     type Encoded = Vec<u8>;
///
///     fn encode(&self) -> Result<Vec<u8>, CodecError> {
///         // your serialization here
///         Ok(vec![])
///     }
///
///     fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
///         // your deserialization here
///         Ok(MyMessage { id: 0, data: vec![] })
///     }
/// }
/// ```
pub trait Codec: Send + Sized {
    /// The encoded byte representation.
    ///
    /// Typically `Vec<u8>` or `rkyv::util::AlignedVec`.
    type Encoded: AsRef<[u8]>;

    /// Serialize this message into bytes for the ring buffer.
    fn encode(&self) -> Result<Self::Encoded, CodecError>;

    /// Deserialize from bytes received from the ring buffer.
    fn decode(bytes: &[u8]) -> Result<Self, CodecError>;
}

/// Extension of [`Codec`] for formats that support zero-copy access.
///
/// Instead of deserializing into an owned `Self`, the consumer can read
/// archived data directly from the ring buffer without allocation.
///
/// # How it works
///
/// The ring buffer slot contains the serialized bytes. `access()` returns
/// an accessor tied to the input buffer lifetime.
/// The consumer reads fields in-place — zero allocation, zero copy.
///
/// # Supported formats
///
/// - **rkyv**: `Archived` = `rkyv::Archived<Self>`, cost: ~1.4ns (pointer cast + validation)
/// - **flatbuffers**: `Archived` = root table type, cost: ~3ns (vtable lookup)
/// - **bincode**: NOT supported (requires full deserialization)
///
/// # Example
///
/// ```ignore
/// impl ZeroCopyCodec for MyBatch {
///     type Archived<'a> = &'a rkyv::Archived<Vec<MyPayload>>;
///
///     fn access<'a>(bytes: &'a [u8]) -> Result<Self::Archived<'a>, CodecError> {
///         rkyv::access::<rkyv::Archived<Vec<MyPayload>>, rkyv::rancor::Error>(bytes)
///             .map_err(CodecError::decode)
///     }
/// }
///
/// // Consumer:
/// consumer.process_available_zero_copy::<MyBatch, _>(buf, |kind, archived| {
///     // `archived` points directly into ring buffer — zero copy
///     for item in archived.iter() {
///         process(item.token_ids());  // reads in-place from SHM
///     }
/// });
/// ```
pub trait ZeroCopyCodec: Codec {
    /// The zero-copy accessor type returned for a particular input lifetime.
    ///
    /// For rkyv: `&'a rkyv::Archived<Self>` (e.g., `&'a ArchivedVec<ArchivedSequence>`)
    /// For flatbuffers: the root table handle by value (e.g., `PayloadBatch<'a>`)
    type Archived<'a>: 'a
    where
        Self: 'a;

    /// Access archived data in-place from a byte buffer.
    ///
    /// Returns an accessor tied to `bytes` — no allocation, no copy.
    /// The accessor is valid as long as `bytes` is valid.
    ///
    /// Cost: ~1-3ns (pointer cast + optional validation)
    fn access<'a>(bytes: &'a [u8]) -> Result<Self::Archived<'a>, CodecError>;
}

// --- rkyv support ---

#[cfg(feature = "rkyv")]
pub mod rkyv_support {
    //! Re-exports and error helpers for implementing [`Codec`](super::Codec)
    //! with rkyv.
    //!
    //! rkyv's trait bounds are too complex for generic wrapper functions in
    //! stable Rust. Instead, this module re-exports `rkyv` so callers use its
    //! API directly and map rkyv errors into [`CodecError`](super::CodecError)
    //! at their callsites.
    //!
    //! # Usage
    //!
    //! ```ignore
    //! use myelon::codec::{Codec, CodecError};
    //! use myelon::codec::rkyv_support::rkyv;
    //!
    //! impl Codec for MyType {
    //!     type Encoded = rkyv::util::AlignedVec;
    //!
    //!     fn encode(&self) -> Result<Self::Encoded, CodecError> {
    //!         rkyv::to_bytes::<rkyv::rancor::Error>(self)
    //!             .map_err(CodecError::encode)
    //!     }
    //!
    //!     fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
    //!         let archived = rkyv::access::<rkyv::Archived<Self>, rkyv::rancor::Error>(bytes)
    //!             .map_err(CodecError::decode)?;
    //!         rkyv::deserialize::<Self, rkyv::rancor::Error>(archived)
    //!             .map_err(CodecError::decode)
    //!     }
    //! }
    //! ```

    // Re-export rkyv so consumers don't need a direct dependency.
    pub use rkyv;
}

// --- flatbuffers support ---

#[cfg(feature = "flatbuffers")]
pub mod flatbuf_support {
    //! Re-exports and helpers for FlatBuffers-based [`Codec`](super::Codec) impls.

    pub use flatbuffers;

    /// Verify and access a `flatbuffers` root from raw bytes.
    pub fn root<'a, T: flatbuffers::Follow<'a> + flatbuffers::Verifiable + 'a>(
        bytes: &'a [u8],
    ) -> Result<T::Inner, super::CodecError> {
        flatbuffers::root::<T>(bytes)
            .map_err(|e| super::CodecError::Decode(format!("flatbuffers: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trivial codec that passes bytes through unchanged.
    #[derive(Debug, PartialEq)]
    struct RawMessage(Vec<u8>);

    impl Codec for RawMessage {
        type Encoded = Vec<u8>;

        fn encode(&self) -> Result<Vec<u8>, CodecError> {
            Ok(self.0.clone())
        }

        fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
            Ok(RawMessage(bytes.to_vec()))
        }
    }

    #[test]
    fn test_raw_codec_roundtrip() {
        let msg = RawMessage(vec![1, 2, 3, 4, 5]);
        let bytes = msg.encode().unwrap();
        let decoded = RawMessage::decode(bytes.as_ref()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_codec_error_display() {
        let err = CodecError::Encode("test error".to_string());
        assert_eq!(format!("{err}"), "codec encode: test error");
        let err = CodecError::Decode("bad data".to_string());
        assert_eq!(format!("{err}"), "codec decode: bad data");
    }

    #[test]
    fn test_codec_error_is_error() {
        let err: Box<dyn std::error::Error> = Box::new(CodecError::Encode("test".to_string()));
        assert!(err.to_string().contains("test"));
    }

    // --- ZeroCopyCodec tests ---

    /// Test type for zero-copy: a simple u64 pair stored as little-endian bytes.
    #[derive(Debug, PartialEq, Clone)]
    struct SimplePair {
        a: u64,
        b: u64,
    }

    /// The "archived" layout — same struct, but accessed via pointer cast.
    #[repr(C)]
    struct ArchivedSimplePair {
        a: u64,
        b: u64,
    }

    impl Codec for SimplePair {
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
            Ok(SimplePair {
                a: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                b: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            })
        }
    }

    impl ZeroCopyCodec for SimplePair {
        type Archived<'a> = &'a ArchivedSimplePair;

        fn access<'a>(bytes: &'a [u8]) -> Result<Self::Archived<'a>, CodecError> {
            if bytes.len() < 16 {
                return Err(CodecError::decode("too short"));
            }
            // Safety: repr(C) struct with known layout, bytes are properly aligned
            // In production, rkyv handles alignment; this is a simplified test.
            let ptr = bytes.as_ptr() as *const ArchivedSimplePair;
            // Check alignment
            if !(ptr as usize).is_multiple_of(std::mem::align_of::<ArchivedSimplePair>()) {
                return Err(CodecError::decode("unaligned"));
            }
            Ok(unsafe { &*ptr })
        }
    }

    #[test]
    fn zc01_zero_copy_codec_trait_compiles() {
        // ZeroCopyCodec trait exists and SimplePair implements it
        fn assert_zero_copy<T: ZeroCopyCodec>() {}
        assert_zero_copy::<SimplePair>();
    }

    #[test]
    fn zc02_access_returns_valid_reference() {
        let pair = SimplePair { a: 42, b: 99 };
        let encoded = pair.encode().unwrap();
        let archived = SimplePair::access(&encoded).unwrap();
        assert_eq!(archived.a, 42);
        assert_eq!(archived.b, 99);
    }

    #[test]
    fn zc04_access_pointer_is_inside_input_buffer() {
        let pair = SimplePair { a: 1, b: 2 };
        let encoded = pair.encode().unwrap();
        let archived = SimplePair::access(&encoded).unwrap();

        // The archived reference should point INTO the encoded buffer
        let archived_ptr = archived as *const ArchivedSimplePair as usize;
        let buf_start = encoded.as_ptr() as usize;
        let buf_end = buf_start + encoded.len();
        assert!(archived_ptr >= buf_start && archived_ptr < buf_end,
            "access() pointer {archived_ptr:#x} is NOT inside buffer [{buf_start:#x}, {buf_end:#x})");
    }

    #[test]
    fn zc05_decode_pointer_is_not_inside_input_buffer() {
        let pair = SimplePair { a: 1, b: 2 };
        let encoded = pair.encode().unwrap();
        let decoded = SimplePair::decode(&encoded).unwrap();

        // The decoded value should be a NEW allocation, NOT inside the encoded buffer
        let decoded_ptr = &decoded as *const SimplePair as usize;
        let buf_start = encoded.as_ptr() as usize;
        let buf_end = buf_start + encoded.len();
        assert!(
            decoded_ptr < buf_start || decoded_ptr >= buf_end,
            "decode() pointer {decoded_ptr:#x} IS inside buffer — should be a copy!"
        );
    }
}
