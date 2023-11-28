use std::{io::Read, io::Write};

use anyhow::Result;
use crc32fast::Hasher;
use fuser::{FileAttr, FileType};
use libc::{gid_t, mode_t, uid_t};
use serde::{Deserialize, Serialize};

use crate::utils::{current_timestamp, timestamp_to_system_time};

use super::super_block::SuperBlock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Inode {
    pub id: u64,
    pub size: u64,
    pub creation_time: u64,
    pub last_accessed: u64,
    pub last_modified: u64,
    pub last_metadata_changed: u64,
    pub kind: FileType,
    pub mode: libc::mode_t,
    pub hard_links: u32,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
    pub block_count: u64,
    pub rdev: u32,
    pub flags: u32,
    pub direct_pointers: [u32; 12],
    pub indirect_pointer: u32,
    pub doubly_indirect_pointer: u32,
    pub triply_indirect_pointer: u32,
    pub checksum: u32,
}

impl Inode {
    pub fn new(id: u64, kind: FileType, mode: mode_t, uid: uid_t, gid: gid_t, flags: u32) -> Self {
        Self {
            id,
            kind,
            mode,
            uid,
            gid,
            flags,
            size: 0,
            creation_time: current_timestamp(),
            last_accessed: current_timestamp(),
            last_modified: current_timestamp(),
            last_metadata_changed: current_timestamp(),
            hard_links: 1,
            block_count: 0,
            rdev: 0,
            direct_pointers: [0; 12],
            indirect_pointer: 0,
            doubly_indirect_pointer: 0,
            triply_indirect_pointer: 0,
            checksum: 0,
        }
    }

    pub fn to_file_attr(&self, super_block: &SuperBlock) -> FileAttr {
        FileAttr {
            ino: self.id,
            size: self.size,
            blocks: self.block_count,
            atime: timestamp_to_system_time(self.last_accessed),
            mtime: timestamp_to_system_time(self.last_modified),
            ctime: timestamp_to_system_time(self.last_metadata_changed),
            crtime: timestamp_to_system_time(self.creation_time),
            kind: self.kind,
            perm: self.mode as u16,
            nlink: self.hard_links,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: super_block.block_size,
            flags: self.flags,
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
        let inode: Self = bincode::deserialize_from(r)?;

        // if !sb.verify_checksum() {
        // Err(anyhow!("Invalid superblock checksum"))
        // } else {
        Ok(inode)
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

    pub fn clear_suid_sgid(&mut self) {
        self.mode &= !libc::S_ISUID;

        if self.mode & libc::S_IXGRP != 0 {
            self.mode &= !libc::S_ISGID;
        }
    }
}
