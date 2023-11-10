use std::{collections::BTreeMap, ffi::OsString, time::SystemTime};

use fuser::{FileAttr, FileType};

#[derive(Debug, Clone)]
pub struct Inode {
    pub id: u64,
    pub directory_entries: BTreeMap<OsString, u64>,
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

impl From<&Inode> for FileAttr {
    fn from(inode: &Inode) -> Self {
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
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            blksize: 0,
            flags: 0,
        }
    }
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
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            blksize: 0,
            flags: 0,
        }
    }
}
