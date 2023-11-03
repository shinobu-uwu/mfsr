use std::{
    collections::HashMap,
    io::Cursor,
    time::{Duration, SystemTime},
};

use fuser::{FileAttr, FileType, Filesystem, ReplyOpen, Request};

use crate::{
    types::{Inode, SuperBlock},
    utils::current_timestamp,
};

pub struct Mfsr {
    super_block: SuperBlock,
    inodes: HashMap<u64, Inode>,
    current_id: u64,
}

impl Mfsr {
    pub fn new() -> Self {
        let root = Inode {
            id: 1,
            file_name: "".to_owned(),
            parent: None,
            open_file_handles: 0,
            size: 0,
            last_accessed: None,
            last_modified: None,
            last_metadata_changed: None,
            kind: FileType::Directory,
            mode: 777,
            hardlinks: 0,
            uid: 0,
            gid: 0,
        };
        let mut inodes = HashMap::new();
        inodes.insert(1, root);
        let tmp = Inode {
            id: 2,
            file_name: "tmp".to_owned(),
            parent: Some(1),
            open_file_handles: 0,
            size: 0,
            last_accessed: None,
            last_modified: None,
            last_metadata_changed: None,
            kind: FileType::Directory,
            mode: 777,
            hardlinks: 0,
            uid: 0,
            gid: 0,
        };
        inodes.insert(2, tmp);

        let home = Inode {
            id: 3,
            file_name: "home".to_owned(),
            parent: Some(1),
            open_file_handles: 0,
            size: 0,
            last_accessed: None,
            last_modified: None,
            last_metadata_changed: None,
            kind: FileType::Directory,
            mode: 777,
            hardlinks: 0,
            uid: 0,
            gid: 0,
        };
        inodes.insert(3, home);

        Self {
            inodes,
            super_block: SuperBlock::default(),
            current_id: 1,
        }
    }
}

impl Filesystem for Mfsr {
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(
            self.super_block.free_blocks + self.super_block.block_count,
            self.super_block.block_count,
            self.super_block.free_blocks,
            0,
            0,
            self.super_block.block_size,
            512,
            self.super_block.block_size,
        )
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let inode = self.inodes.get(&ino);

        match inode {
            Some(i) => reply.attr(
                &Duration::from_secs(2),
                &FileAttr {
                    ino: i.id,
                    size: i.size,
                    blocks: 0,
                    atime: SystemTime::now(),
                    mtime: SystemTime::now(),
                    ctime: SystemTime::now(),
                    crtime: SystemTime::now(),
                    blksize: self.super_block.block_size,
                    kind: i.kind,
                    perm: i.mode,
                    nlink: 0,
                    uid: i.uid,
                    gid: i.gid,
                    rdev: 0,
                    flags: 0,
                },
            ),
            None => reply.error(libc::ENOENT),
        }
    }
}
