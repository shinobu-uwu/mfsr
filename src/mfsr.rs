use std::{
    collections::HashMap,
    ffi::OsStr,
    time::{Duration, SystemTime},
};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request,
};
use libc::ENOENT;

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
            name: "".to_owned(),
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
            name: "tmp".to_owned(),
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
            name: "home".to_owned(),
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
            current_id: 4,
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
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if let Some(_) = self.inodes.get(&ino) {
            let directory_inodes: Vec<_> = self
                .inodes
                .values()
                .filter(|i| i.parent == Some(ino))
                .cloned()
                .collect();

            for (i, inode) in directory_inodes.into_iter().enumerate() {
                if offset == 0 {
                    let _ = reply.add(inode.id, (i as i64) + 1, inode.kind, &inode.name);
                    if let Some(inode) = self.inodes.get_mut(&inode.id) {
                        inode.open_file_handles += 1;
                    }
                }
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        if ino == 1 {
            self.inodes
                .entry(2)
                .and_modify(|inode| inode.open_file_handles -= 1);
            self.inodes
                .entry(3)
                .and_modify(|inode| inode.open_file_handles -= 1);
        }

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

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let inode = Inode {
            id: self.current_id,
            name: name.to_str().unwrap().to_owned(),
            parent: Some(parent),
            open_file_handles: 0,
            size: 0,
            last_accessed: None,
            last_modified: None,
            last_metadata_changed: None,
            kind: FileType::RegularFile,
            mode: mode.try_into().unwrap(),
            hardlinks: 0,
            uid: 0,
            gid: 0,
        };

        reply.created(
            &Duration::from_secs(2),
            &inode.into_fileattr(),
            0,
            2,
            flags.try_into().unwrap(),
        );
        self.inodes.insert(inode.id, inode);
        self.current_id += 1
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let lookup = self
            .inodes
            .values()
            .find(|inode| inode.parent == Some(parent) && inode.name.as_str() == name);

        match lookup {
            Some(i) => reply.entry(&Duration::new(0, 0), &i.into_fileattr(), 0),
            None => reply.error(ENOENT),
        }
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        let inode = self.inodes.get(&ino);

        match inode {
            Some(i) => reply.attr(&Duration::new(0, 0), &i.into_fileattr()),
            None => reply.error(ENOENT),
        }
    }
}
