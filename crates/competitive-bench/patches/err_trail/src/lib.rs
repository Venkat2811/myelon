#![cfg_attr(not(test), no_std)]
#![allow(unused_variables)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {{
        #[allow(unused_variables)]
        let args = format_args!($($arg)*);
        #[cfg(feature = "tracing")]
        tracing::error!("{}", args);
        #[cfg(feature = "log")]
        log::error!("{}", args);
        #[cfg(feature = "defmt")]
        defmt::error!("{}", defmt::Display2Format(&args));
    }};
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {{
        #[allow(unused_variables)]
        let args = format_args!($($arg)*);
        #[cfg(feature = "tracing")]
        tracing::warn!("{}", args);
        #[cfg(feature = "log")]
        log::warn!("{}", args);
        #[cfg(feature = "defmt")]
        defmt::warn!("{}", defmt::Display2Format(&args));
    }};
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {{
        #[allow(unused_variables)]
        let args = format_args!($($arg)*);
        #[cfg(feature = "tracing")]
        tracing::info!("{}", args);
        #[cfg(feature = "log")]
        log::info!("{}", args);
        #[cfg(feature = "defmt")]
        defmt::info!("{}", defmt::Display2Format(&args));
    }};
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {{
        #[allow(unused_variables)]
        let args = format_args!($($arg)*);
        #[cfg(feature = "tracing")]
        tracing::debug!("{}", args);
        #[cfg(feature = "log")]
        log::debug!("{}", args);
        #[cfg(feature = "defmt")]
        defmt::debug!("{}", defmt::Display2Format(&args));
    }};
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {{
        #[allow(unused_variables)]
        let args = format_args!($($arg)*);
        #[cfg(feature = "tracing")]
        tracing::trace!("{}", args);
        #[cfg(feature = "log")]
        log::trace!("{}", args);
        #[cfg(feature = "defmt")]
        defmt::trace!("{}", defmt::Display2Format(&args));
    }};
}

use core::fmt::Display;

mod sealed {
    /// A sealed trait to prevent external implementations.
    pub trait Sealed {}
}


pub trait ErrLog<E> {
    fn error(self, error: &E);
    fn warn(self, error: &E);
    fn info(self, error: &E);
    fn debug(self, error: &E);
    fn trace(self, error: &E);
}

impl<'a, E> ErrLog<E> for ()
where
    E: Display,
{
    #[inline]
    fn error(self, error: &E) {
        error!("{}", error)
    }
    #[inline]
    fn warn(self, error: &E) {
        warn!("{}", error)
    }
    #[inline]
    fn info(self, error: &E) {
        info!("{}", error)
    }
    #[inline]
    fn debug(self, error: &E) {
        debug!("{}", error)
    }
    #[inline]
    fn trace(self, error: &E) {
        trace!("{}", error)
    }
}

impl<E> ErrLog<E> for &str {
    #[inline]
    fn error(self, error: &E) {
        error!("{}", self)
    }
    #[inline]
    fn warn(self, error: &E) {
        warn!("{}", self)
    }
    #[inline]
    fn info(self, error: &E) {
        info!("{}", self)
    }
    #[inline]
    fn debug(self, error: &E) {
        debug!("{}", self)
    }
    #[inline]
    fn trace(self, error: &E) {
        trace!("{}", self)
    }
}

impl<F, E, D> ErrLog<E> for F
where
    F: FnOnce(&E) -> D,
    D: Display,
{
    #[inline]
    fn error(self, error: &E) {
        let message = self(error);
        error!("{}", message)
    }
    #[inline]
    fn warn(self, error: &E) {
        let message = self(error);
        warn!("{}", message)
    }
    #[inline]
    fn info(self, error: &E) {
        let message = self(error);
        info!("{}", message)
    }
    #[inline]
    fn debug(self, error: &E) {
        let message = self(error);
        debug!("{}", message)
    }
    #[inline]
    fn trace(self, error: &E) {
        let message = self(error);
        trace!("{}", message)
    }
}

pub trait NoneLog {
    fn error(self);
    fn warn(self);
    fn info(self);
    fn debug(self);
    fn trace(self);
}

impl NoneLog for &str {
    #[inline]
    fn error(self) {
        error!("{}", self)
    }
    #[inline]
    fn warn(self) {
        warn!("{}", self)
    }
    #[inline]
    fn info(self) {
        info!("{}", self)
    }
    #[inline]
    fn debug(self) {
        debug!("{}", self)
    }
    #[inline]
    fn trace(self) {
        trace!("{}", self)
    }
}

impl<F, D> NoneLog for F
where
    F: FnOnce() -> D,
    D: Display,
{
    #[inline]
    fn error(self) {
        let message = self();
        error!("{}", message)
    }
    #[inline]
    fn warn(self) {
        let message = self();
        warn!("{}", message)
    }
    #[inline]
    fn info(self) {
        let message = self();
        info!("{}", message)
    }
    #[inline]
    fn debug(self) {
        let message = self();
        debug!("{}", message)
    }
    #[inline]
    fn trace(self) {
        let message = self();
        trace!("{}", message)
    }
}

/// For logging a [`Result`] when [`Result::Err`] is encountered.
pub trait ErrContext<T, E>: sealed::Sealed {
    /// If [`Result::Err`], logging as "error".
    fn error(self, input: impl ErrLog<E>) -> Result<T, E>;
    /// If [`Result::Err`], logging as "warn".
    fn warn(self, input: impl ErrLog<E>) -> Result<T, E>;
    /// If [`Result::Err`], logging as "info".
    fn info(self, input: impl ErrLog<E>) -> Result<T, E>;
    /// If [`Result::Err`], logging as "debug".
    fn debug(self, input: impl ErrLog<E>) -> Result<T, E>;
    /// If [`Result::Err`], logging as "trace".
    fn trace(self, input: impl ErrLog<E>) -> Result<T, E>;
}

/// For logging a [`Option`] when [`Option::None`] is encountered.
pub trait NoneContext<T>: sealed::Sealed {
    /// If [`Option::None`], logging as "error".
    fn error(self, input: impl NoneLog) -> Option<T>;
    /// If [`Option::None`], logging as "warn".
    fn warn(self, input: impl NoneLog) -> Option<T>;
    /// If [`Option::None`], logging as "info".
    fn info(self, input: impl NoneLog) -> Option<T>;
    /// If [`Option::None`], logging as "debug".
    fn debug(self, input: impl NoneLog) -> Option<T>;
    /// If [`Option::None`], logging as "trace".
    fn trace(self, input: impl NoneLog) -> Option<T>;
}

impl<T, E> sealed::Sealed for Result<T, E> {}

impl<T, E> ErrContext<T, E> for Result<T, E> {
    #[inline]
    fn error(self, input: impl ErrLog<E>) -> Result<T, E> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.error(&err);
                Err(err)
            }
        }
    }

    #[inline]
    fn warn(self, input: impl ErrLog<E>) -> Result<T, E> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.warn(&err);
                Err(err)
            }
        }
    }

    #[inline]
    fn info(self, input: impl ErrLog<E>) -> Result<T, E> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.info(&err);
                Err(err)
            }
        }
    }

    #[inline]
    fn debug(self, input: impl ErrLog<E>) -> Result<T, E> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.debug(&err);
                Err(err)
            }
        }
    }

    #[inline]
    fn trace(self, input: impl ErrLog<E>) -> Result<T, E> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.trace(&err);
                Err(err)
            }
        }
    }
}

impl<T> sealed::Sealed for Option<T> {}

impl<T> NoneContext<T> for Option<T> {
    #[inline]
    fn error(self, input: impl NoneLog) -> Option<T> {
        match self {
            Some(value) => Some(value),
            None => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.error();
                None
            }
        }
    }

    #[inline]
    fn warn(self, input: impl NoneLog) -> Option<T> {
        match self {
            Some(value) => Some(value),
            None => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.warn();
                None
            }
        }
    }

    #[inline]
    fn info(self, input: impl NoneLog) -> Option<T> {
        match self {
            Some(value) => Some(value),
            None => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.info();
                None
            }
        }
    }

    #[inline]
    fn debug(self, input: impl NoneLog) -> Option<T> {
        match self {
            Some(value) => Some(value),
            None => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.debug();
                None
            }
        }
    }

    #[inline]
    fn trace(self, input: impl NoneLog) -> Option<T> {
        match self {
            Some(value) => Some(value),
            None => {
                #[cfg(any(feature = "tracing", feature = "log", feature = "defmt"))]
                input.trace();
                None
            }
        }
    }
}
