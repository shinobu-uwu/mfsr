use std::{
    collections::BTreeMap,
    io::{Read, Write},
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub inode_id: u64,
    pub entries: BTreeMap<String, u64>,
}

impl DirectoryEntry {
    pub fn new(inode_id: u64) -> Self {
        Self {
            inode_id,
            entries: BTreeMap::new(),
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
        let dentry: Self = bincode::deserialize_from(r)?;

        // if !sb.verify_checksum() {
        // Err(anyhow!("Invalid superblock checksum"))
        // } else {
        Ok(dentry)
        // }
    }
}
