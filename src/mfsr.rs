use std::{
    collections::BTreeMap,
    ffi::OsStr,
    time::{Duration, SystemTime},
};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request,
};
use libc::{EEXIST, ENOENT};

use crate::types::{Inode, SuperBlock};

pub struct Mfsr {
    super_block: SuperBlock,
    inodes: BTreeMap<u64, Inode>,
    next_id: u64,
    next_fh: u64,
}

impl Mfsr {
    pub fn new() -> Self {
        Self {
            inodes: BTreeMap::new(),
            super_block: SuperBlock::default(),
            next_id: 1,
            next_fh: 1,
        }
    }

    pub fn get_inode(&self, inode_id: u64) -> Option<&Inode> {
        self.inodes.get(&inode_id)
    }

    pub fn get_children_inodes(&self, parent_id: u64) -> Vec<&Inode> {
        self.inodes
            .values()
            .filter(|i| i.parent == Some(parent_id))
            .collect()
    }

    pub fn insert_inode(&mut self, inode: Inode) {
        self.inodes.insert(inode.id, inode);
        self.next_id += 1
    }

    pub fn lookup_inode(&self, parent_id: u64, name: &str) -> Option<&Inode> {
        self.inodes
            .values()
            .find(|inode| inode.parent == Some(parent_id) && inode.name == name)
    }

    pub fn close_inode(&mut self, inode_id: u64) {
        // self.inodes.entry(inode_id).and_modify(|i| i.open_file_handles -= 1);
    }
}

impl Filesystem for Mfsr {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        self.insert_inode(Inode {
            id: 1,
            name: "".to_string(),
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
        });

        Ok(())
    }
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

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.get_inode(ino) {
            Some(_) => {
                reply.opened(self.next_fh, 0);
                self.next_fh += 1;
            }
            None => reply.error(ENOENT),
        }
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
        self.close_inode(ino);
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
                    perm: i.mode as u16,
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
            id: self.next_id,
            name: name.to_str().unwrap().to_owned(),
            parent: Some(parent),
            open_file_handles: 1,
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
        self.next_fh += 1;
        reply.created(
            &Duration::from_secs(2),
            &inode.clone().into(),
            0,
            2,
            flags.try_into().unwrap(),
        );
        self.insert_inode(inode);
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup_inode(parent, name.to_str().unwrap()) {
            Some(i) => reply.entry(&Duration::new(0, 0), &i.clone().into(), 0),
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
            Some(i) => reply.attr(&Duration::new(0, 0), &i.clone().into()),
            None => reply.error(ENOENT),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        match self.get_inode(parent) {
            Some(_) => {
                let inode = Inode {
                    id: self.next_id,
                    name: name.to_str().unwrap().to_string(),
                    parent: Some(parent),
                    open_file_handles: 0,
                    size: 0,
                    last_accessed: None,
                    last_modified: None,
                    last_metadata_changed: None,
                    kind: FileType::Directory,
                    mode,
                    hardlinks: 2,
                    uid: 0,
                    gid: 0,
                };
                self.insert_inode(inode.clone());
                reply.entry(&Duration::new(0, 0), &inode.into(), 0)
            }
            None => reply.error(ENOENT),
        };
    }

    fn write(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            fh: u64,
            offset: i64,
            data: &[u8],
            write_flags: u32,
            flags: i32,
            lock_owner: Option<u64>,
            reply: fuser::ReplyWrite,
        ) {
        
    }
}
