use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::TimeOrNow;

#[inline(always)]
pub fn timestamp_to_system_time(timestamp: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(timestamp)
}

#[inline(always)]
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[inline(always)]
pub fn system_time_to_timestamp(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs()
}

#[inline(always)]
pub fn time_or_now_to_timestamp(time_or_now: TimeOrNow) -> u64 {
    match time_or_now {
        TimeOrNow::SpecificTime(t) => system_time_to_timestamp(t),
        TimeOrNow::Now => current_timestamp(),
    }
}
