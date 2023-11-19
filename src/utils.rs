use std::{
    mem::size_of,
    process::Command,
    str::from_utf8,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
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
    let inode_table_size = block_size * 8 * size_of::<Inode>() as u32;
    let result = block_size // super block
    + block_size // data bitmap
    + block_size // inode bitmap
    + inode_table_size
    + get_data_block_size(block_size);

    result as u64
}

#[inline(always)]
pub fn get_data_block_size(block_size: u32) -> u32 {
    block_size * 8 * block_size
}
