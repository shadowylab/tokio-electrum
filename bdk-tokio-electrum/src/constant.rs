use std::time::Duration;

/// We include a chain suffix of a certain length for the purpose of robustness.
pub(crate) const CHAIN_SUFFIX_LENGTH: u32 = 8;
pub(crate) const DEFAULT_STOP_GAP: usize = 20;
pub(crate) const DEFAULT_BATCH_SIZE: usize = 20;
pub(crate) const DEFAULT_BATCH_WINDOW: Duration = Duration::from_millis(1500);
pub(crate) const DEFAULT_WALLET_LABEL: &str = "unlabeled";
