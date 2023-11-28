use std::{
    collections::BTreeMap,
    io::{Read, Seek, Write},
};

use anyhow::{anyhow, Result};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::utils::u64_to_bytes;

#[derive(Debug, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub inode_id: u64,
    pub entries: BTreeMap<String, u64>,
    pub checksum: u32,
}

impl DirectoryEntry {
    pub fn new(inode_id: u64) -> Self {
        Self {
            inode_id,
            entries: BTreeMap::new(),
            checksum: 0,
        }
    }

    pub fn serialize_into<W>(&mut self, mut w: W) -> Result<()>
    where
        W: Write + Seek,
    {
        self.checksum();
        let len = bincode::serialized_size(self)?;
        let buf = u64_to_bytes(len);
        w.write_all(&buf)?;
        bincode::serialize_into(w, self).map_err(|e| e.into())
    }

    pub fn deserialize_from<R>(r: R) -> Result<Self>
    where
        R: Read,
    {
        let mut dentry: Self = bincode::deserialize_from(r)?;

        if !dentry.verify_checksum() {
            Err(anyhow!("Invalid superblock checksum"))
        } else {
            Ok(dentry)
        }
    }

    pub fn checksum(&mut self) {
        self.checksum = self.calculate_checksum();
    }

    pub fn calculate_checksum(&mut self) -> u32 {
        self.checksum = 0;
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
}
