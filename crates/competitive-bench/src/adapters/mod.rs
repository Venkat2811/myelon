//! Per-adapter benchmark implementations. One module per IPC library.
//!
//! Each adapter owns its scenario surface (`pingpong`, `broadcast`, ...).
//! `src/bin/<vendor>_<scenario>.rs` are thin wrappers that call into
//! `adapters::<vendor>::<scenario>::main`.

pub mod crossbar;
pub mod iceoryx2;
pub mod internal;
pub mod rusteron;
pub mod shmipc;
pub mod zmq;
