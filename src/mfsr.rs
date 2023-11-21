use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Cursor, Read, Seek},
    mem::size_of,
    path::Path,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use fuser::{FileType, Filesystem, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request, TimeOrNow, FUSE_ROOT_ID};

use libc::{c_int, EACCES, EEXIST, ENAMETOOLONG, ENOENT, W_OK, EIO, EFBIG, S_ISUID, S_ISGID};
use memmap2::{MmapMut, MmapOptions};

use crate::{
    types::{block_group::BlockGroup, inode::Inode, super_block::SuperBlock},
    utils::{
        current_timestamp, get_block_group_size, time_or_now_to_timestamp, system_time_to_timestamp,
    },
};

const FILE_ATTR_TTL: Duration = Duration::from_secs(1);
const MAX_NAME_LENGTH: usize = 255;
const FMODE_EXEC: i32 = 0x20;
const FILE_HANDLE_READ_BIT: u64 = 1 << 63;
const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;
// with triple indirect pointers we can have file sizes up to 4 TiB
const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024 * 1024 * 1024;

#[derive(Debug)]
pub struct Mfsr {
    super_block: SuperBlock,
    io_map: MmapMut,
    block_groups: Vec<BlockGroup>,
    next_fh: u64,
}

impl Mfsr {
    pub fn new<P>(source: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(source)?;
        let mut buf = [0; size_of::<SuperBlock>()];
        file.read_exact(&mut buf)?;
        let cursor = Cursor::new(&buf);
        let super_block = SuperBlock::deserialize_from(cursor)?;
        let size = get_block_group_size(super_block.block_size) * super_block.block_group_count;
        file.rewind()?;
        let io_map = unsafe { MmapOptions::new().len(size as usize).map_mut(&file)? };
        let mut cursor = Cursor::new(&io_map);
        let block_groups = BlockGroup::deserialize_from(
            &mut cursor,
            super_block.block_size,
            super_block.block_group_count as usize,
        )?;

        let mut fs = Self {
            super_block,
            block_groups,
            io_map,
            next_fh: 1,
        };

        fs.create_root()?;

        Ok(fs)
    }

    pub fn check_access(
        &self,
        inode_uid: u32,
        inode_gid: u32,
        file_mode: u16,
        uid: u32,
        gid: u32,
        mut access_mask: i32,
    ) -> bool {
        // F_OK tests for existence of file
        if access_mask == libc::F_OK {
            return true;
        }
        let file_mode = i32::from(file_mode);

        // root is allowed to read & write anything
        if uid == 0 {
            // root only allowed to exec if one of the X bits is set
            access_mask &= libc::X_OK;
            access_mask -= access_mask & (file_mode >> 6);
            access_mask -= access_mask & (file_mode >> 3);
            access_mask -= access_mask & file_mode;
            return access_mask == 0;
        }

        if uid == inode_uid {
            access_mask -= access_mask & (file_mode >> 6);
        } else if gid == inode_gid {
            access_mask -= access_mask & (file_mode >> 3);
        } else {
            access_mask -= access_mask & file_mode;
        }

        return access_mask == 0;
    }

    fn parse_flags(&self, flags: i32) -> Result<(c_int, bool, bool), c_int> {
        match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                if flags & libc::O_TRUNC != 0 {
                    return Err(EACCES);
                }

                Ok((libc::R_OK, true, false))
            }
            libc::O_WRONLY => Ok((libc::W_OK, false, true)),
            libc::O_RDWR => Ok((libc::R_OK | libc::W_OK, true, true)),
            _ => Err(libc::EINVAL),
        }
    }

    fn create_root(&mut self) -> anyhow::Result<()> {
        if self.inode_exists(1) {
            Ok(())
        } else {
            let mut inode = Inode::new(FUSE_ROOT_ID, FileType::Directory, 0o777, 0, 0, 0);
            inode.hard_links = 2;
            self.write_inode(&mut inode)?;
            Ok(())
        }
    }

    fn inode_exists(&self, inode_id: u64) -> bool {
        let (group_id, bitmap_byte_index, bitmap_bit_index) = self.inode_bitmap_offset(inode_id);
        let group = &self.block_groups[group_id];

        if bitmap_byte_index >= group.inode_bitmap.len() {
            return false;
        }

        let bitmap_byte = group.inode_bitmap[bitmap_byte_index as usize];
        let mask = 1 << bitmap_bit_index;

        (bitmap_byte & mask) != 0
    }

    fn get_inode(&mut self, inode_id: u64) -> Option<Inode> {
        if !self.inode_exists(inode_id) {
            return None;
        }

        let offset = self.inode_table_offset(inode_id);
        let mmap = self.io_map.as_mut();
        let mut cursor = Cursor::new(mmap);
        cursor
            .seek(std::io::SeekFrom::Start(offset as u64))
            .unwrap();

        Some(Inode::deserialize_from(&mut cursor).unwrap())
    }

    fn write_inode(&mut self, inode: &mut Inode) -> anyhow::Result<()> {
        let (group_id, bitmap_byte_index, bitmap_bit_index) = self.inode_bitmap_offset(inode.id);
        let group = &mut self.block_groups[group_id as usize];
        let creation = group.inode_bitmap[bitmap_byte_index as usize] & 1 << bitmap_bit_index == 0;
        group.inode_bitmap[bitmap_byte_index as usize] |= 1 << bitmap_bit_index;

        let offset = self.inode_table_offset(inode.id);
        let mmap = self.io_map.as_mut();
        let mut cursor = Cursor::new(mmap);
        cursor.seek(std::io::SeekFrom::Start(offset as u64))?;
        inode.serialize_into(&mut cursor)?;

        if creation {
            self.super_block.free_inodes -= 1;
        }

        Ok(())
    }

    fn inode_bitmap_offset(&self, inode_id: u64) -> (usize, usize, usize) {
        let group_offset = inode_id / self.super_block.block_size as u64;
        let byte_offset = (inode_id - 1) + group_offset * self.super_block.block_size as u64 / 8;
        let bit_offset = (inode_id - 1) % 8;

        (
            group_offset as usize,
            byte_offset as usize,
            bit_offset as usize
        )
    }

    fn inode_table_offset(&self, inode_id: u64) -> u64 {
        let group_id = inode_id / self.super_block.block_size as u64;
        let block_size = self.super_block.block_size;
        get_block_group_size(block_size) * group_id
            + block_size as u64 * 3
            + (inode_id - 1) * size_of::<Inode>() as u64
    }

    fn lookup_inode(&mut self, parent_id: u64, name: &OsStr) -> Option<Inode> {
        match self.get_inode(parent_id) {
            Some(inode) => match inode.directory_entries.get(name) {
                Some(id) => self.get_inode(*id),
                None => None,
            },
            None => None,
        }
    }
    fn next_inode_id(&self) -> u64 {
        for (group_id, group) in self.block_groups.iter().enumerate() {
            for (byte_index, byte) in group.inode_bitmap.iter().enumerate() {
                for bit_index in 0..8 {
                    let mask = 1 << bit_index;

                    if byte & mask == 0 {
                        return (group_id as u64 * self.super_block.block_size as u64) + (byte_index as u64 * 8) + bit_index as u64 + 1;
                    }
                }
            }
        }

        0
    }

    fn get_next_file_handle(&mut self, read: bool, write: bool) -> u64 {
        self.next_fh += 1;
        let mut fh = self.next_fh;
        // panic in case we ran out of bits
        assert!(fh < FILE_HANDLE_WRITE_BIT && fh < FILE_HANDLE_READ_BIT);

        if read {
            fh |= FILE_HANDLE_READ_BIT;
        }
        if write {
            fh |= FILE_HANDLE_WRITE_BIT;
        }

        fh
    }

    fn get_groups(&self, pid: u32) -> Vec<u32> {
        #[cfg(not(target_os = "macos"))]
        {
            let path = format!("/proc/{pid}/task/{pid}/status");
            let file = File::open(path).unwrap();
            for line in BufReader::new(file).lines() {
                let line = line.unwrap();
                if line.starts_with("Groups:") {
                    return line["Groups: ".len()..]
                        .split(' ')
                        .filter(|x| !x.trim().is_empty())
                        .map(|x| x.parse::<u32>().unwrap())
                        .collect();
                }
            }
        }

        vec![]
    }

    fn check_file_handle_write(&self, fh: u64) -> bool {
        (fh & FILE_HANDLE_WRITE_BIT ) != 0
    }

    fn check_file_handle_read(&self, fh: u64) -> bool {
        (fh & FILE_HANDLE_READ_BIT ) != 0
    }

    fn truncate_inode(&mut self, inode: &mut Inode, size: u64, uid: u32, gid: u32) -> std::result::Result<(), c_int> {
         if size > MAX_FILE_SIZE {
            return Err(EFBIG);
        }

        if !self.check_access(inode.uid, inode.gid, inode.mode as u16, uid, gid, libc::W_OK) {
            return Err(EACCES);
        }

        inode.size = size;
        inode.last_metadata_changed = current_timestamp();
        inode.last_modified = current_timestamp();
        inode.clear_suid_sgid();
        self.write_inode(inode).map_err(|_| EIO)?;

        Ok(())
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
        BlockGroup::serialize_into(&mut cursor, &self.block_groups, &mut self.super_block).unwrap();
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if name.len() > MAX_NAME_LENGTH as usize {
            reply.error(ENAMETOOLONG);
            return;
        }

        match self.lookup_inode(parent, name) {
            Some(i) => reply.entry(&FILE_ATTR_TTL, &i.to_file_attr(&self.super_block), 0),
            None => reply.error(ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        match self.get_inode(ino) {
            Some(i) => reply.attr(&FILE_ATTR_TTL, &i.to_file_attr(&self.super_block)),
            None => reply.error(ENOENT),
        }
    }

    fn setattr(
        &mut self,
        req: &Request<'_>,
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
        let mut inode = match self.get_inode(ino) {
            Some(attrs) => attrs,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if let Some(mode) = mode {
            if req.uid() != 0 && req.uid() != inode.uid {
                reply.error(libc::EPERM);
                return;
            }
            if req.uid() != 0 && req.gid() != inode.gid {
                // if SGID is set and the file belongs to a group that the caller is not part of
                // then the SGID bit is suppose to be cleared during chmod
                inode.mode = mode & !S_ISGID as u32;
            } else {
                inode.mode = mode;
            }
            inode.last_metadata_changed = current_timestamp();
            match self.write_inode(&mut inode) {
                Ok(()) => {},
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            }
            reply.attr(&Duration::new(0, 0), &inode.to_file_attr(&self.super_block));
            return;
        }

        if uid.is_some() || gid.is_some() {
            if let Some(gid) = gid {
                // Non-root users can only change gid to a group they're in
                if req.uid() != 0 && !self.get_groups(req.pid()).contains(&gid) {
                    reply.error(libc::EPERM);
                    return;
                }
            }
            if let Some(uid) = uid {
                if req.uid() != 0 && !(uid == inode.uid && req.uid() == inode.uid) {
                    reply.error(libc::EPERM);
                    return;
                }
            }
            // Only owner may change the group
            if gid.is_some() && req.uid() != 0 && req.uid() != inode.uid {
                reply.error(libc::EPERM);
                return;
            }

            if inode.mode & (libc::S_IXUSR | libc::S_IXGRP | libc::S_IXOTH) != 0 {
                // SUID & SGID are cleared when chown'ing an executable file
                inode.clear_suid_sgid();
            }

            if let Some(uid) = uid {
                inode.uid = uid;
                // Clear SETUID on owner change
                inode.mode &= !S_ISUID;
            }
            if let Some(gid) = gid {
                inode.gid = gid;
                // Clear SETGID unless user is root
                if req.uid() != 0 {
                    inode.mode &= !S_ISGID;
                }
            }
            inode.last_metadata_changed = current_timestamp();
            match self.write_inode(&mut inode) {
                Ok(()) => {},
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            }
            reply.attr(&Duration::new(0, 0), &inode.to_file_attr(&self.super_block));
            return;
        }

        if let Some(size) = size {
            if let Some(handle) = fh {
                // If the file handle is available, check access locally.
                // This is important as it preserves the semantic that a file handle opened
                // with W_OK will never fail to truncate, even if the file has been subsequently
                // chmod'ed
                if self.check_file_handle_write(handle) {
                    if let Err(error_code) = self.truncate_inode(&mut inode, size, 0, 0) {
                        reply.error(error_code);
                        return;
                    }
                } else {
                    reply.error(EACCES);
                    return;
                }
            } else if let Err(error_code) = self.truncate_inode(&mut inode, size, req.uid(), req.gid()) {
                reply.error(error_code);
                return;
            }
        }

        if let Some(atime) = atime {
            if inode.uid != req.uid() && req.uid() != 0 && atime != TimeOrNow::Now {
                reply.error(libc::EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    libc::W_OK,
                )
            {
                reply.error(EACCES);
                return;
            }

            inode.last_accessed = time_or_now_to_timestamp(atime);
            inode.last_metadata_changed = current_timestamp();
        }

        if let Some(ctime) = ctime {
            if inode.uid != req.uid() && req.uid() != 0 {
                reply.error(libc::EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    libc::W_OK,
                )
            {
                reply.error(EACCES);
                return;
            }

            inode.last_modified = system_time_to_timestamp(ctime);
            inode.last_metadata_changed = current_timestamp();
        }

        if let Some(crtime) = crtime {
            if inode.uid != req.uid() && req.uid() != 0 {
                reply.error(libc::EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    libc::W_OK,
                )
            {
                reply.error(EACCES);
                return;
            }

            inode.creation_time = system_time_to_timestamp(crtime);
            inode.last_metadata_changed = current_timestamp();
        }

        if let Some(mtime) = mtime {
            if inode.uid != req.uid() && req.uid() != 0 && mtime != TimeOrNow::Now {
                reply.error(libc::EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    libc::W_OK,
                )
            {
                reply.error(EACCES);
                return;
            }

            inode.last_modified = time_or_now_to_timestamp(mtime);
            inode.last_metadata_changed = current_timestamp();
        }

        if let Some(flags) = flags {
            if inode.uid != req.uid() && req.uid() != 0 {
                reply.error(libc::EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    libc::W_OK,
                )
            {
                reply.error(EACCES);
                return;
            }

            inode.flags = flags;
            inode.last_metadata_changed = current_timestamp();
        }

        match self.write_inode(&mut inode) {
            Ok(_) => {},
            Err(_) => {
                reply.error(EIO);
                return;
            }
        }

        let inode = self.get_inode(ino).unwrap();
        reply.attr(&Duration::new(0, 0), &inode.to_file_attr(&self.super_block));
        return;
    }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let (access_mask, read, write) = match self.parse_flags(flags) {
            Ok(result) => result,
            Err(code) => {
                reply.error(code);
                return;
            }
        };

        match self.get_inode(ino) {
            Some(i) => {
                if !self.check_access(
                    i.uid,
                    i.uid,
                    i.mode as u16,
                    req.uid(),
                    req.gid(),
                    access_mask,
                ) {
                    reply.error(EACCES);
                    return;
                }

                self.next_fh += 1;
                reply.opened(self.get_next_file_handle(read, write), access_mask as u32);
            }
            None => reply.error(ENOENT),
        }
    }

    fn mkdir(
            &mut self,
            req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            mut mode: u32,
            _umask: u32,
            reply: ReplyEntry,
        ) {
        if !self.inode_exists(parent) {
            reply.error(ENOENT);
            return;
        }

        if self.lookup_inode(parent, name).is_some() {
            reply.error(EEXIST);
            return;
        }

        let mut parent_inode = self.get_inode(parent).unwrap();

        if !self.check_access(parent_inode.uid, parent_inode.gid, parent_inode.mode as u16, req.uid(), req.gid(), W_OK) {
            reply.error(EACCES);
            return;
        }


        if req.uid() != 0 {
            mode &= !(S_ISUID | S_ISGID) as u32;
        }
        if parent_inode.mode & S_ISGID != 0 {
            mode |= S_ISGID as u32;
        }

        let mut new_inode = Inode::new(self.next_inode_id(), FileType::Directory, mode, req.uid(), req.gid(), 0);
        parent_inode.last_modified = current_timestamp();
        parent_inode.last_metadata_changed = current_timestamp();
        parent_inode.directory_entries.insert(name.into(), new_inode.id);

        match self.write_inode(&mut parent_inode) {
            Ok(_) => {},
            Err(_) => {
                reply.error(EIO);
                return;
            }
        }

        match self.write_inode(&mut new_inode) {
            Ok(()) => {},
            Err(_) => {
                reply.error(EIO);
                return;
            }
        }

        reply.entry(&FILE_ATTR_TTL, &new_inode.to_file_attr(&self.super_block), 0);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        dbg!(self);
        reply.written(100);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.get_inode(ino) {
            Some(i) => {
                let (access_mask, read, write) = match self.parse_flags(flags) {
                    Ok(result) => result,
                    Err(code) => {
                        reply.error(code);
                        return;
                    }
                };

                if !self.check_access(
                    i.uid,
                    i.gid,
                    i.mode as u16,
                    _req.uid(),
                    _req.gid(),
                    access_mask,
                ) {
                    reply.error(EACCES);
                    return;
                }

                reply.opened(self.get_next_file_handle(read, write), flags as u32);
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
        if let Some(parent_inode) = self.get_inode(ino) {
            for (i, (name, id)) in parent_inode.directory_entries.iter().enumerate() {
                if offset == 0 {
                    let inode = self.get_inode(*id).unwrap();
                    let _ = reply.add(*id, (i as i64) + 1, inode.kind, name);
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
        match self.get_inode(ino) {
            Some(_) => reply.ok(),
            None => reply.error(ENOENT),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(
            self.super_block.block_count,
            self.super_block.free_blocks,
            self.super_block.free_blocks,
            self.super_block.inode_count - self.super_block.free_inodes,
            self.super_block.free_inodes,
            self.super_block.block_size,
            MAX_NAME_LENGTH as u32,
            self.super_block.block_size,
        )
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mut mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        if !self.inode_exists(parent) {
            reply.error(ENOENT);
            return;
        }

        if self.lookup_inode(parent, name).is_some() {
            reply.error(EEXIST);
            return;
        }

        let (read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let mut parent_inode = self.get_inode(parent).unwrap();

        if !self.check_access(
            parent_inode.uid,
            parent_inode.gid,
            parent_inode.mode as u16,
            req.uid(),
            req.gid(),
            W_OK,
        ) {
            reply.error(EACCES);
            return;
        }

        if req.uid() != 0 {
            mode &= !(S_ISUID | S_ISGID) as u32;
        }

        let mut new_inode = Inode::new(
            self.next_inode_id(),
            FileType::RegularFile,
            mode,
            req.uid(),
            req.gid(),
            flags as u32,
        );
        parent_inode.directory_entries.insert(name.into(), new_inode.id);
        parent_inode.last_modified = current_timestamp();
        parent_inode.last_metadata_changed = current_timestamp();

        match self.write_inode(&mut parent_inode) {
            Ok(()) => {},
            Err(_) => {
                reply.error(EIO);
                return;
            }
        }

        match self.write_inode(&mut new_inode) {
            Ok(_) => {
                reply.created(
                    &FILE_ATTR_TTL,
                    &new_inode.to_file_attr(&self.super_block),
                    0,
                    self.get_next_file_handle(read, write),
                    flags as u32,
                );
            }
            Err(_) => reply.error(EIO),
        }
    }
}
