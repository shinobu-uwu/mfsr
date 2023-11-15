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
    pub uid: u64,
    pub gid: u64,
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

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            magic: SB_MAGIC_NUMBER,
            block_size: 512,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            last_mounted_at: SystemTime::now(),
            block_count: 0,
            inode_count: 0,
            free_blocks: 0,
            free_inodes: 0,
            groups: 0,
            data_blocks_per_group: 0,
            uid: 0,
            gid: 0,
            checksum: 0,
        }
    }
}
