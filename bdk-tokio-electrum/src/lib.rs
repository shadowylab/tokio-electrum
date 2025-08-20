//! (Tokio) Electrum client for Bitcoin Dev Kit

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::large_futures)]
#![warn(rustdoc::bare_urls)]

mod bdk_electrum_client;
mod util;

pub use bdk_electrum_client::*;
