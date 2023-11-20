use std::{
    io::{Read, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use crc32fast::Hasher;
use libc::{gid_t, uid_t};
use serde::{Deserialize, Serialize};

const MAGIC_NUMBER: u32 = 0x4D534653;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
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
    pub block_group_count: u64,
    pub data_blocks_per_group: u64,
    pub uid: uid_t,
    pub gid: gid_t,
    pub checksum: u32,
}

impl SuperBlock {
    pub fn new(
        block_size: u32,
        block_group_count: u64,
        data_blocks_per_group: u64,
        uid: uid_t,
        gid: gid_t,
    ) -> Self {
        let block_count = block_size as u64 * 8 * block_group_count;

        Self {
            magic: MAGIC_NUMBER,
            block_size,
            created_at: SystemTime::now(),
            modified_at: UNIX_EPOCH,
            last_mounted_at: UNIX_EPOCH,
            block_count,
            inode_count: block_count,
            free_blocks: block_count,
            free_inodes: block_count,
            block_group_count,
            data_blocks_per_group,
            uid,
            gid,
            checksum: 0,
        }
    }

    pub fn serialize_into<W>(&mut self, w: W) -> Result<()>
    where
        W: Write,
    {
        bincode::serialize_into(w, self).map_err(|e| e.into())
    }

    pub fn deserialize_from<R>(r: R) -> Result<Self>
    where
        R: Read,
    {
        let sb: Self = bincode::deserialize_from(r)?;

        // if !sb.verify_checksum() {
        // Err(anyhow!("Invalid superblock checksum"))
        // } else {
        Ok(sb)
        // }
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
