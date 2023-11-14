use std::{collections::BTreeMap, ffi::OsString, time::SystemTime};

use fuser::{FileAttr, FileType};

use super::super_block::SuperBlock;

#[derive(Debug, Clone)]
pub struct Inode {
    pub id: u64,
    pub directory_entries: BTreeMap<OsString, u64>,
    pub open_file_handles: u64,
    pub size: u64,
    pub creation_time: SystemTime,
    pub last_accessed: SystemTime,
    pub last_modified: SystemTime,
    pub last_metadata_changed: SystemTime,
    pub kind: FileType,
    pub mode: u16,
    pub hard_links: u32,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
    pub block_count: u64,
    pub rdev: u32,
    pub flags: u32,
    pub extended_attributes: BTreeMap<OsString, OsString>,
}

impl Inode {
    pub fn to_file_attr(&self, super_block: &SuperBlock) -> FileAttr {
        FileAttr {
            ino: self.id,
            size: self.size,
            blocks: self.block_count,
            atime: self.last_accessed,
            mtime: self.last_modified,
            ctime: self.last_metadata_changed,
            crtime: self.creation_time,
            kind: self.kind,
            perm: self.mode,
            nlink: self.hard_links,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: super_block.block_size,
            flags: self.flags,
        }
    }
}
