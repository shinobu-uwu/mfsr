use std::{
    mem::size_of,
    collections::BTreeMap,
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{BufWriter, Write, Cursor},
    path::Path,
    time::{Duration, SystemTime}, mem::size_of,
};

use anyhow::Result;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request,
};
use libc::{EEXIST, EINVAL, ENOENT};
use memmap2::MmapMut;

use crate::{
    types::{block_group::BlockGroup, super_block::SuperBlock, inode::Inode},
};

#[derive(Debug)]
pub struct Mfsr {
    super_block: SuperBlock,
    io_map: MmapMut,
    block_groups: Vec<BlockGroup>,
    next_fh: u64,
}

impl Mfsr {
    pub fn new<P>(mount_point: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let device = OpenOptions::new().write(true).read(true).open(&mount_point)?;
        let io_map = unsafe { MmapMut::map_mut(&device)? };
        let mut cursor = Cursor::new(&io_map);
        let super_block = SuperBlock::deserialize_from(&mut cursor)?;
        let block_groups = BlockGroup::deserialize_from(cursor, super_block.block_size, super_block.block_group_count as usize)?;

        Ok(Self {
            io_map,
            block_groups,
            super_block,
            next_fh: 1,
        })
    }

    fn create_root(&mut self) -> anyhow::Result<()> {
        if self.inode_exists(1) {
            Ok(())
        } else {
            self.create_inode(1, todo!())
        }
    }

    fn inode_exists(&mut self, inode_id: u64) -> bool {
        let group_id = (inode_id / self.super_block.block_size as u64 - 1) as usize;
        let group = self.block_groups[group_id];
    }

    fn get_inode(&mut self, inode_id: u64) -> Option<Inode> {

        todo!()
    }

    fn create_inode(&self, arg: i32, inode: Inode) -> std::result::Result<(), anyhow::Error> {
        todo!()
    }

    // fn next_inode_id(&self) -> Option<u64> {
    //     for (group_index, inode_group) in self.inode_groups.iter().enumerate() {
    //         for bit_index in 0..64 {
    //             let inode_mask = 1 << bit_index;
    //             if (inode_group.inode_bitmap & inode_mask) == 0 {
    //                 return Some((group_index as u64) * 64 + bit_index as u64);
    //             }
    //         }
    //     }
    //     None
    // }

    // fn write_inode(&self, inode: Inode) -> Result<()> {
    //     Ok(())
    // }
    //
    // pub fn get_inode(&self, inode_id: u64) -> Option<&Inode> {
    //     self.inodes.get(&inode_id)
    // }
    //
    // pub fn get_inode_mut(&mut self, inode_id: u64) -> Option<&mut Inode> {
    //     self.inodes.get_mut(&inode_id)
    // }
    //
    // pub fn insert_inode(&mut self, inode: Inode) {
    //     self.inodes.insert(inode.id, inode);
    //     self.next_id += 1
    // }
    //
    // pub fn lookup_inode(&self, parent_id: u64, name: &OsStr) -> Option<&Inode> {
    //     match self.get_inode(parent_id) {
    //         Some(inode) => match inode.directory_entries.get(name) {
    //             Some(id) => self.get_inode(*id),
    //             None => None,
    //         },
    //         None => None,
    //     }
    // }
    //
    // pub fn close_inode(&mut self, _inode_id: u64) {
    //     // self.inodes.entry(inode_id).and_modify(|i| i.open_file_handles -= 1);
    // }
}

impl Filesystem for Mfsr {
    // fn init(
    //     &mut self,
    //     req: &Request<'_>,
    //     _config: &mut fuser::KernelConfig,
    // ) -> Result<(), libc::c_int> {
    //     self.super_block.update_last_mounted();
    //     self.super_block.uid = req.uid();
    //     self.super_block.gid = req.gid();
    //
    //     Ok(())
    // }
    //
    // fn destroy(&mut self) {
    //     let buf = self.io_map.as_mut();
    //     let mut writer = BufWriter::new(buf);
    //
    //     self.super_block.serialize_into(&mut writer).unwrap();
    //     writer.flush().unwrap();
    // }
    //
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(
            self.super_block.block_count,
            self.super_block.free_blocks,
            self.super_block.free_blocks,
            self.super_block.inode_count - self.super_block.free_inodes,
            self.super_block.free_inodes,
            self.super_block.block_size,
            255,
            self.super_block.block_size,
        )
    }
    //
    // fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
    //     match self.get_inode(ino) {
    //         Some(_) => {
    //             reply.opened(self.next_fh, 0);
    //             self.next_fh += 1;
    //         }
    //         None => reply.error(ENOENT),
    //     }
    // }
    //
    // fn readdir(
    //     &mut self,
    //     _req: &Request<'_>,
    //     ino: u64,
    //     _fh: u64,
    //     offset: i64,
    //     mut reply: ReplyDirectory,
    // ) {
    //     if let Some(parent_inode) = self.inodes.get(&ino) {
    //         for (i, (name, id)) in parent_inode.directory_entries.iter().enumerate() {
    //             if offset == 0 {
    //                 let inode = self.get_inode(*id).unwrap();
    //                 let _ = reply.add(*id, (i as i64) + 1, inode.kind, name);
    //             }
    //         }
    //         reply.ok();
    //     } else {
    //         reply.error(ENOENT);
    //     }
    // }
    //
    // fn releasedir(
    //     &mut self,
    //     _req: &Request<'_>,
    //     ino: u64,
    //     _fh: u64,
    //     _flags: i32,
    //     _reply: ReplyEmpty,
    // ) {
    //     self.close_inode(ino);
    // }
    //
    // fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
    //     let inode = self.inodes.get(&ino);
    //
    //     match inode {
    //         Some(i) => reply.attr(
    //             &Duration::from_secs(2),
    //             &FileAttr {
    //                 ino: i.id,
    //                 size: i.size,
    //                 blocks: 0,
    //                 atime: SystemTime::now(),
    //                 mtime: SystemTime::now(),
    //                 ctime: SystemTime::now(),
    //                 crtime: SystemTime::now(),
    //                 blksize: self.super_block.block_size,
    //                 kind: i.kind,
    //                 perm: i.mode as u16,
    //                 nlink: 0,
    //                 uid: i.uid,
    //                 gid: i.gid,
    //                 rdev: 0,
    //                 flags: 0,
    //             },
    //         ),
    //         None => reply.error(libc::ENOENT),
    //     }
    // }
    //
    // fn create(
    //     &mut self,
    //     _request: &Request<'_>,
    //     parent: u64,
    //     name: &OsStr,
    //     _mode: u32,
    //     _umask: u32,
    //     flags: i32,
    //     reply: fuser::ReplyCreate,
    // ) {
    //     if self.lookup_inode(parent, name).is_some() {
    //         reply.error(EEXIST);
    //         return;
    //     }
    //
    //     let (_read, _write) = match flags & libc::O_ACCMODE {
    //         libc::O_RDONLY => (true, false),
    //         libc::O_WRONLY => (false, true),
    //         libc::O_RDWR => (true, true),
    //         _ => {
    //             reply.error(EINVAL);
    //             return;
    //         }
    //     };
    //
    //     let _parent_inode = match self.get_inode(parent) {
    //         Some(parent_inode) => parent_inode,
    //         None => {
    //             reply.error(ENOENT);
    //             return;
    //         }
    //     };
    // }
    //
    // fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
    //     match self.lookup_inode(parent, name) {
    //         Some(i) => reply.entry(&Duration::new(0, 0), &i.to_file_attr(&self.super_block), 0),
    //         None => reply.error(ENOENT),
    //     }
    // }
    //
    // fn flush(
    //     &mut self,
    //     _req: &Request<'_>,
    //     _ino: u64,
    //     _fh: u64,
    //     _lock_owner: u64,
    //     reply: ReplyEmpty,
    // ) {
    //     reply.ok();
    // }
    //
    // fn setattr(
    //     &mut self,
    //     _req: &Request<'_>,
    //     ino: u64,
    //     mode: Option<u32>,
    //     uid: Option<u32>,
    //     gid: Option<u32>,
    //     size: Option<u64>,
    //     atime: Option<fuser::TimeOrNow>,
    //     _mtime: Option<fuser::TimeOrNow>,
    //     ctime: Option<SystemTime>,
    //     _fh: Option<u64>,
    //     crtime: Option<SystemTime>,
    //     _chgtime: Option<SystemTime>,
    //     _bkuptime: Option<SystemTime>,
    //     flags: Option<u32>,
    //     reply: fuser::ReplyAttr,
    // ) {
    //     let inode = self.get_inode(ino);
    //     if inode.is_none() {
    //         reply.error(ENOENT);
    //         return;
    //     }
    //
    //     self.inodes.entry(ino).and_modify(|inode| {
    //         if let Some(m) = mode {
    //             inode.mode = m;
    //         }
    //
    //         if let Some(u) = uid {
    //             inode.uid = u
    //         }
    //
    //         if let Some(g) = gid {
    //             inode.gid = g;
    //         }
    //
    //         if let Some(s) = size {
    //             inode.size = s;
    //         }
    //
    //         if let Some(a) = atime {
    //             inode.last_accessed = time_or_now_to_timestamp(a);
    //         }
    //
    //         if let Some(c) = ctime {
    //             inode.last_modified = system_time_to_timestamp(c);
    //         }
    //
    //         if let Some(cr) = crtime {
    //             inode.creation_time = system_time_to_timestamp(cr);
    //         }
    //
    //         if let Some(f) = flags {
    //             inode.flags = f;
    //         }
    //     });
    // }
    //
    // fn mkdir(
    //     &mut self,
    //     request: &Request<'_>,
    //     parent: u64,
    //     name: &OsStr,
    //     mode: u32,
    //     _umask: u32,
    //     reply: ReplyEntry,
    // ) {
    //     match self.get_inode(parent) {
    //         Some(_) => {
    //             let id = self.next_id;
    //             let inode = Inode {
    //                 id,
    //                 open_file_handles: 0,
    //                 size: 0,
    //                 block_count: 0,
    //                 creation_time: 0,
    //                 last_accessed: 0,
    //                 last_modified: 0,
    //                 last_metadata_changed: 0,
    //                 kind: FileType::Directory,
    //                 mode,
    //                 hard_links: 2,
    //                 uid: request.uid(),
    //                 gid: request.gid(),
    //                 directory_entries: BTreeMap::new(),
    //                 extended_attributes: BTreeMap::new(),
    //                 flags: 0,
    //                 rdev: 0,
    //                 direct_blocks: [0; 12],
    //                 indirect_block: 0,
    //                 double_indirect_block: 0,
    //                 checksum: 0,
    //             };
    //             self.insert_inode(inode.clone());
    //             reply.entry(
    //                 &Duration::new(0, 0),
    //                 &inode.to_file_attr(&self.super_block),
    //                 0,
    //             );
    //             self.get_inode_mut(parent)
    //                 .unwrap()
    //                 .directory_entries
    //                 .insert(name.to_owned(), id);
    //         }
    //         None => reply.error(ENOENT),
    //     };
    // }
    //
    // fn write(
    //     &mut self,
    //     _req: &Request<'_>,
    //     _ino: u64,
    //     _fh: u64,
    //     _offset: i64,
    //     _data: &[u8],
    //     _write_flags: u32,
    //     _flags: i32,
    //     _lock_owner: Option<u64>,
    //     _reply: fuser::ReplyWrite,
    // ) {
    //     dbg!(self);
    // }
}
