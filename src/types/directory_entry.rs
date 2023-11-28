use std::{
    borrow::BorrowMut,
    collections::BTreeMap,
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::utils::u64_to_bytes;

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

    pub fn serialize_into<W>(&mut self, mut w: W) -> Result<()>
    where
        W: Write + Seek,
    {
        let len = bincode::serialized_size(self)?;
        let buf = u64_to_bytes(len);
        w.write_all(&buf)?;
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
