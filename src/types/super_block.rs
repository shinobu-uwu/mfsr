use std::{
    io::{Read, Write},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};

pub const SB_MAGIC_NUMBER: u32 = 0x4D534653;

#[derive(Debug, Serialize, Deserialize)]
pub struct SuperBlock {
    pub magic: u32,
    pub block_size: u32,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub last_mounted_at: SystemTime,
    pub block_count: u64,
    pub inode_count: u64,
    pub free_blocks: u64,
    pub free_inodes: u64,
    pub groups: u64,
    pub data_blocks_per_group: u64,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
    pub checksum: u64,
}

impl SuperBlock {
    pub fn serialize_into<W>(&mut self, w: W) -> Result<(), Box<dyn std::error::Error>>
    where
        W: Write,
    {
        bincode::serialize_into(w, self).map_err(|e| e.into())
    }

    pub fn deserialize_from<R>(r: R) -> Result<Self, Box<dyn std::error::Error>>
    where
        R: Read,
    {
        let sb: Self = bincode::deserialize_from(r)?;
        Ok(sb)
    }
}

