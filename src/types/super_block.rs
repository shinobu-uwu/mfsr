use std::{
    io::{Read, Write},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use crc32fast::Hasher;
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
    pub checksum: u32,
}

impl SuperBlock {
    pub fn serialize_into<W>(&mut self, w: W) -> Result<()>
    where
        W: Write,
    {
        self.checksum();
        bincode::serialize_into(w, self).map_err(|e| e.into())
    }

    pub fn deserialize_from<R>(r: R) -> Result<Self>
    where
        R: Read,
    {
        let mut sb: Self = bincode::deserialize_from(r)?;

        if !sb.verify_checksum() {
            Err(anyhow!("Invalid superblock checksum"))
        } else {
            Ok(sb)
        }
    }

    pub fn checksum(&mut self) {
        self.checksum = self.calculate_checksum();
    }

    pub fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&bincode::serialize(&self).unwrap());
        hasher.finalize()
    }

    pub fn verify_checksum(&mut self) -> bool {
        let checksum = self.checksum;
        self.checksum = 0;
        let ok = checksum == self.calculate_checksum();
        self.checksum = checksum;

        ok
    }

    pub fn update_last_mounted(&mut self) {
        self.last_mounted_at = SystemTime::now();
    }
}

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            magic: Default::default(),
            block_size: Default::default(),
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            last_mounted_at: SystemTime::now(),
            block_count: Default::default(),
            inode_count: Default::default(),
            free_blocks: Default::default(),
            free_inodes: Default::default(),
            groups: Default::default(),
            data_blocks_per_group: Default::default(),
            uid: Default::default(),
            gid: Default::default(),
            checksum: Default::default(),
        }
    }
}
