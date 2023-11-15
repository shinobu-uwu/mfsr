use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[inline]
pub fn timestamp_to_system_time(timestamp: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(timestamp)
}
