use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use fuser::{FileAttr, FileType, Filesystem, ReplyDirectory, ReplyOpen, Request, ReplyEmpty};

use crate::types::{Inode, SuperBlock};

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

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(self.current_id, 0);
        self.current_id += 1;
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let home = self.inodes.get(&2).unwrap();
        let tmp = self.inodes.get(&3).unwrap();
        let _ = reply.add(home.id, 1, home.kind, &home.file_name);
        let _ = reply.add(tmp.id, 2, tmp.kind, &tmp.file_name);
        self.inodes.entry(2).and_modify(|inode| inode.open_file_handles += 1);
        self.inodes.entry(3).and_modify(|inode| inode.open_file_handles += 1);
        reply.ok();
    }

    fn releasedir(
            &mut self,
            _req: &Request<'_>,
            _ino: u64,
            _fh: u64,
            _flags: i32,
            reply: ReplyEmpty,
        ) {
        self.inodes.entry(2).and_modify(|inode| inode.open_file_handles -= 1);
        self.inodes.entry(3).and_modify(|inode| inode.open_file_handles -= 1);
        reply.ok()
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
