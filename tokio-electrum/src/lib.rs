//! Tokio Electrum client

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::large_futures)]
#![warn(rustdoc::bare_urls)]

pub mod address;
pub mod builder;
pub mod client;
mod constant;
pub mod notification;
pub mod prelude;
#[cfg(feature = "socks")]
mod socks;
pub mod status;
pub mod types;
