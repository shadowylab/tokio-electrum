//! (Tokio) Electrum client for Bitcoin Dev Kit

#![warn(missing_docs)]
#![warn(clippy::large_futures)]
#![warn(rustdoc::bare_urls)]

mod client;
mod util;

pub use self::client::*;
