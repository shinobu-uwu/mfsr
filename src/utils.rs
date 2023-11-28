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

#[inline(always)]
pub fn bytes_to_pointers(chunk: &[u8]) -> u32 {
    let mut result = 0u32;

    for (i, &byte) in chunk.iter().enumerate() {
        result |= (byte as u32) << (i * 8);
    }
    result
}

#[inline(always)]
pub fn pointer_to_bytes(pointer: u32) -> [u8; 4] {
    let mut bytes = [0u8; 4];

    for i in 0..4 {
        bytes[i] = ((pointer >> (8 * i)) & 0xFF) as u8;
    }

    bytes
}

#[inline(always)]
pub fn bytes_to_u64(bytes: [u8; 8]) -> u64 {
    let mut result = 0u64;

    for (i, &byte) in bytes.iter().enumerate() {
        result |= (byte as u64) << (8 * i);
    }

    result
}

#[inline(always)]
pub fn u64_to_bytes(value: u64) -> [u8; 8] {
    let mut result = [0u8; 8];

    for i in 0..8 {
        result[i] = ((value >> (8 * i)) & 0xFF) as u8;
    }

    result
}
