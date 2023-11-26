use std::{
    mem::size_of,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fuser::TimeOrNow;

use crate::types::inode::Inode;

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

#[inline(always)]
pub fn get_block_group_size(block_size: u32) -> u64 {
    let result = block_size as u64 // super block
    + block_size as u64 // data bitmap
    + block_size as u64 // inode bitmap
    + get_inode_table_size(block_size)
    + get_data_block_size(block_size);

    result as u64
}

#[inline(always)]
pub fn get_inode_table_size(block_size: u32) -> u64 {
    block_size as u64 * 8 * size_of::<Inode>() as u64
}

#[inline(always)]
pub fn get_data_block_size(block_size: u32) -> u64 {
    block_size as u64 * 8 * block_size as u64
}
