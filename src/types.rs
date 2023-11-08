use std::time::SystemTime;

use fuser::{FileAttr, FileType};
use serde::{Deserialize, Serialize};

use crate::utils::current_timestamp;

const MAGIC_NUMBER: u32 = 0x4D534653;

#[derive(Serialize, Deserialize)]
pub struct SuperBlock {
    pub magic: u32,
    pub block_size: u32,
    pub created_at: u64,
    pub modified_at: Option<u64>,
    pub last_mounted_at: Option<u64>,
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

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            magic: MAGIC_NUMBER,
            block_size: 512,
            created_at: current_timestamp(),
            modified_at: None,
            last_mounted_at: None,
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

#[derive(Debug, Clone)]
pub struct Inode {
    pub id: u64,
    pub name: String,
    pub parent: Option<u64>,
    pub open_file_handles: u64,
    pub size: u64,
    pub last_accessed: Option<u64>,
    pub last_modified: Option<u64>,
    pub last_metadata_changed: Option<u64>,
    pub kind: FileType,
    pub mode: libc::mode_t,
    pub hardlinks: u32,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
}

impl From<Inode> for FileAttr {
    fn from(inode: Inode) -> Self {
        FileAttr {
            ino: inode.id,
            size: inode.size,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: inode.kind,
            perm: inode.mode as u16,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            flags: 0,
        }
    }
}
