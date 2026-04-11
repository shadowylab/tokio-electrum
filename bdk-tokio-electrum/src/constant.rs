use std::num::NonZeroU32;
use std::time::Duration;

/// We include a chain suffix of a certain length for the purpose of robustness.
pub(crate) const CHAIN_SUFFIX_LENGTH: u32 = 8;
pub(crate) const DEFAULT_STOP_GAP: NonZeroU32 = NonZeroU32::new(20).unwrap();
pub(crate) const DEFAULT_BATCH_SIZE: NonZeroU32 = NonZeroU32::new(20).unwrap();
pub(crate) const DEFAULT_BATCH_WINDOW: Duration = Duration::from_millis(1500);
pub(crate) const DEFAULT_WALLET_LABEL: &str = "unlabeled";
pub(crate) const LIVE_SYNC_HISTORY_BATCH_SIZE: usize = 64;
