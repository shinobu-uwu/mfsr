use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs::File,
    io::{Cursor, Write},
    time::{Duration, SystemTime},
};

use anyhow::Result;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request,
};
use libc::{EEXIST, EINVAL, ENOENT};
use memmap2::MmapMut;

use crate::{
    types::{inode::Inode, super_block::SuperBlock},
    utils::{system_time_to_timestamp, time_or_now_to_timestamp},
};

#[derive(Debug)]
pub struct Mfsr {
    super_block: SuperBlock,
    io_map: MmapMut,
    inodes: BTreeMap<u64, Inode>,
    next_id: u64,
    next_fh: u64,
}

impl Mfsr {
    pub fn new(super_block: SuperBlock, device: File) -> Result<Self> {
        Ok(Self {
            inodes: BTreeMap::new(),
            io_map: unsafe { MmapMut::map_mut(&device)? },
            super_block,
            next_id: 1,
            next_fh: 1,
        })
    }

    pub fn get_inode(&self, inode_id: u64) -> Option<&Inode> {
        self.inodes.get(&inode_id)
    }

    pub fn get_inode_mut(&mut self, inode_id: u64) -> Option<&mut Inode> {
        self.inodes.get_mut(&inode_id)
    }

    pub fn insert_inode(&mut self, inode: Inode) {
        self.inodes.insert(inode.id, inode);
        self.next_id += 1
    }

    pub fn lookup_inode(&self, parent_id: u64, name: &OsStr) -> Option<&Inode> {
        match self.get_inode(parent_id) {
            Some(inode) => match inode.directory_entries.get(name) {
                Some(id) => self.get_inode(*id),
                None => None,
            },
            None => None,
        }
    }

    pub fn close_inode(&mut self, inode_id: u64) {
        // self.inodes.entry(inode_id).and_modify(|i| i.open_file_handles -= 1);
    }

    fn update_inode(
        &self,
        inode: &mut Inode,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        crtime: Option<SystemTime>,
        flags: Option<u32>,
    ) {
    }
}

impl Filesystem for Mfsr {
    fn init(
        &mut self,
        req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        self.super_block.update_last_mounted();
        self.super_block.uid = req.uid();
        self.super_block.gid = req.gid();

        Ok(())
    }

    fn destroy(&mut self) {
        let buf = self.io_map.as_mut();
        let mut cursor = Cursor::new(buf);

        self.super_block.serialize_into(&mut cursor).unwrap();
        cursor.flush().unwrap();
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
        if let Some(parent_inode) = self.inodes.get(&ino) {
            for (i, (name, id)) in parent_inode.directory_entries.iter().enumerate() {
                if offset == 0 {
                    let inode = self.get_inode(*id).unwrap();
                    let _ = reply.add(*id, (i as i64) + 1, inode.kind, &name);
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
        request: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        if self.lookup_inode(parent, name).is_some() {
            reply.error(EEXIST);
            return;
        }

        let (read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            _ => {
                reply.error(EINVAL);
                return;
            }
        };

        let parent_inode = match self.get_inode(parent) {
            Some(parent_inode) => parent_inode,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup_inode(parent, name) {
            Some(i) => reply.entry(&Duration::new(0, 0), &i.to_file_attr(&self.super_block), 0),
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
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        let inode = self.get_inode(ino);
        if inode.is_none() {
            reply.error(ENOENT);
            return;
        }

        self.inodes.entry(ino).and_modify(|inode| {
            if mode.is_some() {
                inode.mode = mode.unwrap();
            }

            if uid.is_some() {
                inode.uid = uid.unwrap();
            }

            if gid.is_some() {
                inode.gid = gid.unwrap();
            }

            if size.is_some() {
                inode.size = size.unwrap();
            }

            if atime.is_some() {
                inode.last_accessed = time_or_now_to_timestamp(atime.unwrap());
            }

            if ctime.is_some() {
                inode.last_modified = system_time_to_timestamp(ctime.unwrap());
            }

            if crtime.is_some() {
                inode.creation_time = system_time_to_timestamp(crtime.unwrap());
            }

            if flags.is_some() {
                inode.flags = flags.unwrap();
            }
        });
    }

    fn mkdir(
        &mut self,
        request: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        match self.get_inode(parent) {
            Some(_) => {
                let id = self.next_id;
                let inode = Inode {
                    id,
                    open_file_handles: 0,
                    size: 0,
                    block_count: 0,
                    creation_time: 0,
                    last_accessed: 0,
                    last_modified: 0,
                    last_metadata_changed: 0,
                    kind: FileType::Directory,
                    mode,
                    hard_links: 2,
                    uid: request.uid(),
                    gid: request.gid(),
                    directory_entries: BTreeMap::new(),
                    extended_attributes: BTreeMap::new(),
                    flags: 0,
                    rdev: 0,
                    direct_blocks: [0; 12],
                    indirect_block: 0,
                    double_indirect_block: 0,
                    checksum: 0,
                };
                self.insert_inode(inode.clone());
                reply.entry(
                    &Duration::new(0, 0),
                    &inode.to_file_attr(&self.super_block),
                    0,
                );
                self.get_inode_mut(parent)
                    .unwrap()
                    .directory_entries
                    .insert(name.to_owned(), id);
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
        dbg!(self);
    }
}
