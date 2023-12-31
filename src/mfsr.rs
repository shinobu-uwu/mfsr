use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom, Write},
    mem::{size_of, size_of_val},
    path::Path,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use fuser::{
    FileType, Filesystem, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request, TimeOrNow,
    FUSE_ROOT_ID,
};
use libc::{
    c_int, EACCES, EEXIST, EFBIG, EINVAL, EIO, ENAMETOOLONG, ENOENT, ENOTEMPTY, EPERM, F_OK,
    O_ACCMODE, O_RDONLY, O_RDWR, O_TRUNC, O_WRONLY, RENAME_EXCHANGE, R_OK, S_ISGID, S_ISUID,
    S_ISVTX, S_IXGRP, S_IXOTH, S_IXUSR, W_OK, X_OK,
};
use memmap2::{MmapMut, MmapOptions};

use crate::{
    types::{
        block_group::BlockGroup, directory_entry::DirectoryEntry, inode::Inode,
        super_block::SuperBlock,
    },
    utils::{
        bytes_to_pointer, bytes_to_u64, current_timestamp, get_block_group_size,
        get_inode_table_size, pointer_to_bytes, system_time_to_timestamp, time_or_now_to_timestamp,
    },
};

const FILE_ATTR_TTL: Duration = Duration::new(0, 0);
const MAX_NAME_LENGTH: usize = 255;
const FMODE_EXEC: i32 = 0x20;
const FILE_HANDLE_READ_BIT: u64 = 1 << 63;
const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;
// with doubly indirect pointers we can have file sizes up to 4 GiB
const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024 * 1024;

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
        if access_mask == F_OK {
            return true;
        }
        let file_mode = i32::from(file_mode);

        // root is allowed to read & write anything
        if uid == 0 {
            // root only allowed to exec if one of the X bits is set
            access_mask &= X_OK;
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

        access_mask == 0
    }

    fn parse_flags(&self, flags: i32) -> Result<(c_int, bool, bool), c_int> {
        match flags & O_ACCMODE {
            O_RDONLY => {
                if flags & O_TRUNC != 0 {
                    return Err(EACCES);
                }

                if flags & FMODE_EXEC != 0 {
                    return Ok((X_OK, true, false));
                }

                Ok((R_OK, true, false))
            }
            O_WRONLY => Ok((W_OK, false, true)),
            O_RDWR => Ok((R_OK | W_OK, true, true)),
            _ => Err(EINVAL),
        }
    }

    fn create_root(&mut self) -> anyhow::Result<()> {
        if self.inode_exists(1) {
            Ok(())
        } else {
            let mut inode = Inode::new(FUSE_ROOT_ID, FileType::Directory, 0o777, 0, 0, 0);
            inode.hard_links = 2;
            let mut dentry = DirectoryEntry::new(FUSE_ROOT_ID);
            self.write_dentry(&mut inode, &mut dentry)?;
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

        let bitmap_byte = group.inode_bitmap[bitmap_byte_index];
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
        cursor.seek(std::io::SeekFrom::Start(offset)).unwrap();

        Some(Inode::deserialize_from(&mut cursor).unwrap())
    }

    fn write_inode(&mut self, inode: &mut Inode) -> anyhow::Result<()> {
        let (group_id, bitmap_byte_index, bitmap_bit_index) = self.inode_bitmap_offset(inode.id);
        let group = &mut self.block_groups[group_id];
        let creation = group.inode_bitmap[bitmap_byte_index] & 1 << bitmap_bit_index == 0;
        group.inode_bitmap[bitmap_byte_index] |= 1 << bitmap_bit_index;

        let offset = self.inode_table_offset(inode.id);
        let mmap = self.io_map.as_mut();
        let mut cursor = Cursor::new(mmap);
        cursor.seek(std::io::SeekFrom::Start(offset))?;
        inode.serialize_into(&mut cursor)?;

        if creation {
            self.super_block.free_inodes -= 1;
        }

        Ok(())
    }

    fn delete_inode(&mut self, inode_id: u64) {
        let inode = self.get_inode(inode_id).unwrap();
        let (group_id, bitmap_byte_index, bitmap_bit_index) = self.inode_bitmap_offset(inode_id);
        let group = &mut self.block_groups[group_id];
        group.inode_bitmap[bitmap_byte_index] &= !(1 << bitmap_bit_index);
        self.super_block.free_inodes += 1;

        for pointer in inode.direct_pointers {
            if pointer == 0 {
                continue;
            }

            let (group_id, bitmap_byte_index, bitmap_bit_index) =
                self.data_block_bitmap_offset(pointer);
            let group = &mut self.block_groups[group_id];
            group.data_bitmap[bitmap_byte_index] &= !(1 << bitmap_bit_index);
        }

        if inode.indirect_pointer != 0 {
            let pointers = self.read_indirect_pointer(inode.indirect_pointer).unwrap();

            for pointer in pointers {
                if pointer != 0 {
                    let (group_index, byte_index, bit_index) = self.inode_bitmap_offset(inode_id);
                    self.block_groups[group_index].data_bitmap[byte_index] &= !(1 << bit_index);
                }
            }
        }
    }

    fn inode_bitmap_offset(&self, inode_id: u64) -> (usize, usize, usize) {
        let group_offset = inode_id / self.super_block.block_size as u64;
        let byte_offset = ((inode_id - 1) - group_offset * self.super_block.block_size as u64) / 8;
        let bit_offset = (inode_id - 1) % 8;

        (
            group_offset as usize,
            byte_offset as usize,
            bit_offset as usize,
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
        let inode = self.get_inode(parent_id)?;
        let dentry = match self.get_dentry(&inode) {
            Ok(d) => d,
            Err(_) => return None,
        };

        match dentry.entries.get(name.to_str().unwrap()) {
            Some(&i) => self.get_inode(i),
            None => None,
        }
    }

    fn next_inode_id(&self) -> u64 {
        for (group_id, group) in self.block_groups.iter().enumerate() {
            for (byte_index, byte) in group.inode_bitmap.iter().enumerate() {
                for bit_index in 0..8 {
                    if byte >> bit_index & 1 == 0 {
                        return (group_id as u64 * self.super_block.block_size as u64)
                            + (byte_index as u64 * 8)
                            + bit_index as u64
                            + 1;
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

    #[inline(always)]
    fn data_block_id_to_address(&self, block_id: u32) -> u32 {
        let cluster_size = self.super_block.block_size;
        let group_id = block_id / self.super_block.block_size;
        let offset = (block_id - 1) - group_id * cluster_size;

        group_id * get_block_group_size(self.super_block.block_size) as u32
            + cluster_size * 3 // super block + data bitmap + inode bitmap
            + get_inode_table_size(cluster_size) as u32
            + offset * cluster_size
    }

    fn next_free_data_block(&self) -> u32 {
        for (group_id, group) in self.block_groups.iter().enumerate() {
            for (byte_index, byte) in group.data_bitmap.iter().enumerate() {
                for bit_index in 0..8 {
                    if byte >> bit_index & 1 == 0 {
                        return (group_id as u32 * self.super_block.block_size)
                            + (byte_index as u32 * 8)
                            + bit_index as u32
                            + 1;
                    }
                }
            }
        }

        0
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
        (fh & FILE_HANDLE_WRITE_BIT) != 0
    }

    fn check_file_handle_read(&self, fh: u64) -> bool {
        (fh & FILE_HANDLE_READ_BIT) != 0
    }

    fn truncate_inode(
        &mut self,
        inode: &mut Inode,
        size: u64,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<(), c_int> {
        if size > MAX_FILE_SIZE {
            return Err(EFBIG);
        }

        if !self.check_access(inode.uid, inode.gid, inode.mode as u16, uid, gid, W_OK) {
            return Err(EACCES);
        }

        inode.size = size;
        inode.last_metadata_changed = current_timestamp();
        inode.last_modified = current_timestamp();
        inode.clear_suid_sgid();
        self.write_inode(inode).map_err(|_| EIO)?;

        Ok(())
    }

    fn clear_suid_gid(&self, inode: &mut Inode) {
        inode.mode &= !S_ISUID;

        // SGID is only suppose to be cleared if XGRP is set
        if inode.mode & S_IXGRP != 0 {
            inode.mode &= !S_ISGID;
        }
    }

    fn data_block_bitmap_offset(&self, block_id: u32) -> (usize, usize, usize) {
        let group_offset = block_id / self.super_block.block_size;
        let byte_offset = ((block_id - 1) - group_offset * self.super_block.block_size) / 8;
        let bit_offset = (block_id - 1) % 8;

        (
            group_offset as usize,
            byte_offset as usize,
            bit_offset as usize,
        )
    }

    #[inline(always)]
    fn write_data(&mut self, block_id: u32, data: &[u8]) -> Result<usize> {
        let address = self.data_block_id_to_address(block_id);
        let mut cursor = Cursor::new(self.io_map.as_mut());
        cursor.seek(SeekFrom::Start(address as u64))?;
        cursor.write_all(data)?;
        let (group_index, byte_index, bit_index) = self.data_block_bitmap_offset(block_id);
        self.block_groups[group_index].data_bitmap[byte_index] |= 1 << bit_index;

        Ok(data.len())
    }

    #[inline(always)]
    fn read_data(&mut self, block_id: u32, buf: &mut [u8]) -> Result<()> {
        if block_id == 0 {
            println!();
        }
        let address = self.data_block_id_to_address(block_id);
        let mut cursor = Cursor::new(self.io_map.as_ref());
        cursor.seek(SeekFrom::Start(address as u64))?;
        cursor.read_exact(buf)?;

        Ok(())
    }

    fn read_indirect_pointer(&mut self, indirect_pointer: u32) -> Result<Vec<u32>> {
        let mut buf = vec![0; self.super_block.block_size as usize];
        self.read_data(indirect_pointer, &mut buf)?;

        Ok(buf
            .chunks_exact(size_of_val(&indirect_pointer))
            .map(bytes_to_pointer)
            .collect())
    }

    fn write_indirect_pointer(
        &mut self,
        inode: &mut Inode,
        offset: usize,
        data: &[u8],
    ) -> Result<usize, c_int> {
        if inode.indirect_pointer == 0 {
            let pointers = vec![0; self.super_block.block_size as usize];
            let indirect_block_id = self.next_free_data_block();

            if self.write_data(indirect_block_id, &pointers).is_err() {
                return Err(EIO);
            }

            inode.indirect_pointer = indirect_block_id;

            if self.write_inode(inode).is_err() {
                return Err(EIO);
            }

            inode.block_count += 1;
            self.super_block.block_count += 1;
            self.super_block.free_blocks -= 1
        }

        let mut pointers = self
            .read_indirect_pointer(inode.indirect_pointer)
            .map_err(|_| EIO)?;
        let pointer = pointers[offset];
        let block_id = if pointer == 0 {
            inode.block_count += 1;
            self.super_block.block_count += 1;
            self.super_block.free_blocks -= 1;
            self.next_free_data_block()
        } else {
            pointer
        };

        if self.write_data(block_id, data).is_err() {
            return Err(EIO);
        }

        pointers[offset] = block_id;
        let mut buf = vec![];

        for pointer in pointers {
            let slice = pointer_to_bytes(pointer);
            buf.extend_from_slice(&slice);
        }

        if self
            .write_data(inode.indirect_pointer, buf.as_slice())
            .is_err()
        {
            return Err(EIO);
        }

        Ok(data.len())
    }

    fn get_dentry(&mut self, parent_inode: &Inode) -> Result<DirectoryEntry> {
        let address = self.data_block_id_to_address(parent_inode.direct_pointers[0]) as u64;
        let mut cursor = Cursor::new(self.io_map.as_ref());
        cursor.seek(SeekFrom::Start(address))?;
        let mut buf = [0; size_of::<u64>()];
        cursor.read_exact(&mut buf)?;
        let len = bytes_to_u64(buf);
        let mut result_buf = vec![0; len as usize];

        for direct_pointer in parent_inode.direct_pointers {
            if direct_pointer == 0 {
                continue;
            }

            cursor.read_exact(&mut result_buf)?;
        }

        let result = DirectoryEntry::deserialize_from(Cursor::new(result_buf)).unwrap();
        Ok(result)
    }

    fn write_dentry(
        &mut self,
        parent_inode: &mut Inode,
        dentry: &mut DirectoryEntry,
    ) -> Result<()> {
        let mut buf = vec![];
        dentry.serialize_into(Cursor::new(&mut buf))?;

        for (i, chunk) in buf.chunks(self.super_block.block_size as usize).enumerate() {
            let block_id = if parent_inode.direct_pointers[i] == 0 {
                parent_inode.block_count += 1;
                self.super_block.block_count += 1;
                self.next_free_data_block()
            } else {
                parent_inode.direct_pointers[i]
            };

            parent_inode.direct_pointers[i] = block_id;
            self.write_data(block_id, chunk)?;
        }

        Ok(())
    }
}

impl Filesystem for Mfsr {
    fn init(&mut self, req: &Request<'_>, _config: &mut fuser::KernelConfig) -> Result<(), c_int> {
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
        if name.len() > MAX_NAME_LENGTH {
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
                reply.error(EPERM);
                return;
            }
            if req.uid() != 0 && req.gid() != inode.gid {
                // if SGID is set and the file belongs to a group that the caller is not part of
                // then the SGID bit is suppose to be cleared during chmod
                inode.mode = mode & !S_ISGID;
            } else {
                inode.mode = mode;
            }
            inode.last_metadata_changed = current_timestamp();
            match self.write_inode(&mut inode) {
                Ok(()) => {}
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
                    reply.error(EPERM);
                    return;
                }
            }
            if let Some(uid) = uid {
                if req.uid() != 0 && !(uid == inode.uid && req.uid() == inode.uid) {
                    reply.error(EPERM);
                    return;
                }
            }
            // Only owner may change the group
            if gid.is_some() && req.uid() != 0 && req.uid() != inode.uid {
                reply.error(EPERM);
                return;
            }

            if inode.mode & (S_IXUSR | S_IXGRP | S_IXOTH) != 0 {
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
                Ok(()) => {}
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
            } else if let Err(error_code) =
                self.truncate_inode(&mut inode, size, req.uid(), req.gid())
            {
                reply.error(error_code);
                return;
            }
        }

        if let Some(atime) = atime {
            if inode.uid != req.uid() && req.uid() != 0 && atime != TimeOrNow::Now {
                reply.error(EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    W_OK,
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
                reply.error(EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    W_OK,
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
                reply.error(EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    W_OK,
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
                reply.error(EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    W_OK,
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
                reply.error(EPERM);
                return;
            }

            if inode.uid != req.uid()
                && !self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    W_OK,
                )
            {
                reply.error(EACCES);
                return;
            }

            inode.flags = flags;
            inode.last_metadata_changed = current_timestamp();
        }

        match self.write_inode(&mut inode) {
            Ok(_) => {}
            Err(_) => {
                reply.error(EIO);
                return;
            }
        }

        let inode = self.get_inode(ino).unwrap();
        reply.attr(&Duration::new(0, 0), &inode.to_file_attr(&self.super_block));
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
            mode &= !(S_ISUID | S_ISGID);
        }
        if parent_inode.mode & S_ISGID != 0 {
            mode |= S_ISGID;
        }

        let mut new_inode = Inode::new(
            self.next_inode_id(),
            FileType::Directory,
            mode,
            req.uid(),
            req.gid(),
            0,
        );
        new_inode.hard_links = 2;
        let mut parenty_dentry = match self.get_dentry(&parent_inode) {
            Ok(d) => d,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let mut dentry = DirectoryEntry::new(new_inode.id);

        parenty_dentry
            .entries
            .insert(name.to_str().unwrap().to_string(), new_inode.id);
        parent_inode.last_modified = current_timestamp();
        parent_inode.last_metadata_changed = current_timestamp();

        if self.write_dentry(&mut new_inode, &mut dentry).is_err() {
            reply.error(EIO);
            return;
        }

        if self
            .write_dentry(&mut parent_inode, &mut parenty_dentry)
            .is_err()
        {
            reply.error(EIO);
            return;
        }

        match self.write_inode(&mut parent_inode) {
            Ok(_) => {}
            Err(_) => {
                reply.error(EIO);
                return;
            }
        }

        match self.write_inode(&mut new_inode) {
            Ok(()) => {}
            Err(_) => {
                reply.error(EIO);
                return;
            }
        }

        reply.entry(
            &FILE_ATTR_TTL,
            &new_inode.to_file_attr(&self.super_block),
            0,
        );
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        if !self.check_file_handle_write(fh) {
            reply.error(EACCES);
            return;
        }

        let mut inode = match self.get_inode(ino) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut written = 0;
        let start_block = (offset / self.super_block.block_size as i64) as usize;
        let pointers_per_indirect_block =
            self.super_block.block_size as usize / size_of_val(&inode.indirect_pointer);

        for (i, chunk) in data
            .chunks(self.super_block.block_size as usize)
            .enumerate()
        {
            let is_direct_pointer = i + start_block < 12;
            let is_indirect_pointer =
                i + start_block >= 12 && i + start_block < pointers_per_indirect_block + 12;

            if is_direct_pointer {
                let is_new_block = inode.direct_pointers[i + start_block] == 0;

                let block_id = if is_new_block {
                    inode.block_count += 1;
                    self.super_block.block_count += 1;
                    self.super_block.free_blocks -= 1;
                    self.next_free_data_block()
                } else {
                    inode.direct_pointers[i + start_block]
                };

                if self.write_data(block_id, chunk).is_err() {
                    reply.error(EIO);
                    return;
                }

                inode.direct_pointers[i + start_block] = block_id;
                written += chunk.len();
            } else if is_indirect_pointer {
                match self.write_indirect_pointer(&mut inode, i + start_block - 12, chunk) {
                    Ok(w) => written += w,
                    Err(c) => {
                        reply.error(c);
                        return;
                    }
                }
            } else {
                reply.error(EFBIG);
                return;
            }
        }

        let new_size = (offset + written as i64) as u64;
        if new_size > inode.size {
            inode.size = new_size;
        }

        inode.last_metadata_changed = current_timestamp();
        inode.last_modified = current_timestamp();
        self.clear_suid_gid(&mut inode);
        self.write_inode(&mut inode).unwrap();

        reply.written(written as u32);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        if !self.check_file_handle_read(fh) {
            reply.error(EACCES);
            return;
        }

        let mut inode = match self.get_inode(ino) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut result_buf = Vec::with_capacity(size as usize);
        let start_block = (offset / self.super_block.block_size as i64) as usize;
        let end_block = ((offset + size as i64 - 1) / self.super_block.block_size as i64) as usize;

        for i in start_block..=end_block {
            if i < 12 {
                let direct_pointer = inode.direct_pointers[i];
                let mut buf = vec![0; self.super_block.block_size as usize];

                if self.read_data(direct_pointer, &mut buf).is_err() {
                    reply.error(EIO);
                    return;
                }

                result_buf.extend_from_slice(buf.as_slice());
            } else if i >= 12 && i < self.super_block.block_size as usize {
                let mut buf = vec![0; self.super_block.block_size as usize];

                if self.read_data(inode.indirect_pointer, &mut buf).is_err() {
                    reply.error(EIO);
                    return;
                }

                let pointers: Vec<u32> = buf.chunks_exact(4).map(bytes_to_pointer).collect();
                let block_id = pointers[i - 12];

                if self.read_data(block_id, &mut buf).is_err() {
                    reply.error(EIO);
                    return;
                }

                result_buf.extend_from_slice(&buf);
            }
        }

        inode.last_accessed = current_timestamp();

        if self.write_inode(&mut inode).is_err() {
            reply.error(EIO);
            return;
        }

        reply.data(&result_buf);
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
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let inode = match self.get_inode(ino) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if !self.check_access(
            inode.uid,
            inode.gid,
            inode.mode as u16,
            req.uid(),
            req.gid(),
            R_OK,
        ) {
            reply.error(EACCES);
            return;
        }

        if offset < 0 {
            reply.error(EINVAL);
            return;
        }

        let dentry = match self.get_dentry(&inode) {
            Ok(d) => d,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        for (index, entry) in dentry.entries.iter().skip(offset as usize).enumerate() {
            let (name, inode_id) = entry;
            let child_inode = match self.get_inode(*inode_id) {
                Some(i) => i,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            };

            let buffer_full =
                reply.add(*inode_id, offset + index as i64 + 1, child_inode.kind, name);

            if buffer_full {
                break;
            }
        }

        reply.ok();
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

        let (read, write) = match flags & O_ACCMODE {
            O_RDONLY => (true, false),
            O_WRONLY => (false, true),
            O_RDWR => (true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(EINVAL);
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
            mode &= !(S_ISUID | S_ISGID);
        }

        let mut new_inode = Inode::new(
            self.next_inode_id(),
            FileType::RegularFile,
            mode,
            req.uid(),
            req.gid(),
            flags as u32,
        );

        let mut parent_dentry = match self.get_dentry(&parent_inode) {
            Ok(d) => d,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        parent_dentry
            .entries
            .insert(name.to_str().unwrap().to_string(), new_inode.id);
        parent_inode.last_modified = current_timestamp();
        parent_inode.last_metadata_changed = current_timestamp();

        if self
            .write_dentry(&mut parent_inode, &mut parent_dentry)
            .is_err()
        {
            reply.error(EIO);
            return;
        }

        if self.write_inode(&mut parent_inode).is_err() {
            reply.error(EIO);
            return;
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

    fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let mut parent_inode = match self.get_inode(parent) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

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

        let uid = req.uid();

        if parent_inode.mode & S_ISVTX != 0 // sticky bit
            && uid != 0
            && uid != parent_inode.uid
            && uid != parent_inode.uid
        {
            reply.error(EACCES);
            return;
        }

        let inode_id = match self.lookup_inode(parent, name) {
            Some(i) => i.id,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        self.delete_inode(inode_id);
        parent_inode.last_metadata_changed = current_timestamp();
        parent_inode.last_modified = current_timestamp();
        let mut parent_dentry = match self.get_dentry(&parent_inode) {
            Ok(d) => d,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        parent_dentry.entries.remove(name.to_str().unwrap());

        if self
            .write_dentry(&mut parent_inode, &mut parent_dentry)
            .is_err()
        {
            reply.error(EIO);
            return;
        }

        if self.write_inode(&mut parent_inode).is_err() {
            reply.error(EIO);
            return;
        }

        reply.ok();
    }

    fn rename(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        let mut inode = match self.lookup_inode(parent, name) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut parent_inode = match self.get_inode(parent) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

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

        // "Sticky bit" handling
        if parent_inode.mode & S_ISVTX != 0
            && req.uid() != 0
            && req.uid() != parent_inode.uid
            && req.uid() != inode.uid
        {
            reply.error(EACCES);
            return;
        }

        let mut new_parent_inode = match self.get_inode(new_parent) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if !self.check_access(
            new_parent_inode.uid,
            new_parent_inode.gid,
            new_parent_inode.mode as u16,
            req.uid(),
            req.gid(),
            W_OK,
        ) {
            reply.error(EACCES);
            return;
        }

        // "Sticky bit" handling in new_parent
        if new_parent_inode.mode & S_ISVTX != 0 {
            if let Some(existing_inode) = self.lookup_inode(new_parent, new_name) {
                if req.uid() != 0
                    && req.uid() != new_parent_inode.uid
                    && req.uid() != existing_inode.uid
                {
                    reply.error(EACCES);
                    return;
                }
            }
        }

        #[cfg(target_os = "linux")]
        if flags & RENAME_EXCHANGE != 0 {
            let mut new_inode = match self.lookup_inode(new_parent, new_name) {
                Some(i) => i,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            };

            let mut new_inode_dentry = match self.get_dentry(&new_inode) {
                Ok(d) => d,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };

            new_inode_dentry
                .entries
                .insert(new_name.to_str().unwrap().to_string(), inode.id);

            if self
                .write_dentry(&mut new_inode, &mut new_inode_dentry)
                .is_err()
            {
                reply.error(EIO);
                return;
            }

            let mut parent_dentry = match self.get_dentry(&parent_inode) {
                Ok(d) => d,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            parent_dentry
                .entries
                .insert(name.to_str().unwrap().to_string(), new_inode.id);

            parent_inode.last_metadata_changed = current_timestamp();
            parent_inode.last_modified = current_timestamp();

            if self.write_inode(&mut parent_inode).is_err() {
                reply.error(EIO);
                return;
            }

            new_parent_inode.last_metadata_changed = current_timestamp();
            new_parent_inode.last_modified = current_timestamp();

            if self.write_inode(&mut new_parent_inode).is_err() {
                reply.error(EIO);
                return;
            }

            inode.last_metadata_changed = current_timestamp();

            if self.write_inode(&mut inode).is_err() {
                reply.error(EIO);
                return;
            }

            new_inode.last_metadata_changed = current_timestamp();

            if self.write_inode(&mut new_inode).is_err() {
                reply.error(EIO);
                return;
            }

            if inode.kind == FileType::Directory {
                let mut dentry = self.get_dentry(&inode).unwrap();
                dentry.entries.insert("..".to_string(), new_parent);

                if self.write_dentry(&mut inode, &mut dentry).is_err() {
                    reply.error(EIO);
                    return;
                }

                if self.write_inode(&mut inode).is_err() {
                    reply.error(EIO);
                    return;
                }
            }

            if new_inode.kind == FileType::Directory {
                let mut dentry = self.get_dentry(&new_inode).unwrap();
                dentry.entries.insert("..".to_string(), parent);

                if self.write_dentry(&mut new_inode, &mut dentry).is_err() {
                    reply.error(EIO);
                    return;
                }

                if self.write_inode(&mut new_inode).is_err() {
                    reply.error(EIO);
                    return;
                }
            }

            reply.ok();
            return;
        }

        // Only overwrite an existing directory if it's empty
        if let Some(new_name_inode) = self.lookup_inode(new_parent, new_name) {
            if new_name_inode.kind == FileType::Directory
                && self.get_dentry(&new_name_inode).unwrap().entries.len() > 2
            {
                reply.error(ENOTEMPTY);
                return;
            }
        }

        if inode.kind == FileType::Directory
            && parent != new_parent
            && !self.check_access(
                inode.uid,
                inode.gid,
                inode.mode as u16,
                req.uid(),
                req.gid(),
                W_OK,
            )
        {
            reply.error(EACCES);
            return;
        }

        // If target already exists decrement its hardlink count
        if let Some(mut existing_inode) = self.lookup_inode(new_parent, new_name) {
            let mut dentry = self.get_dentry(&parent_inode).unwrap();
            dentry.entries.remove(new_name.to_str().unwrap());

            if self.write_dentry(&mut parent_inode, &mut dentry).is_err() {
                reply.error(EIO);
                return;
            }

            if existing_inode.kind == FileType::Directory {
                existing_inode.hard_links = 0;
            } else {
                existing_inode.hard_links -= 1;
            }

            existing_inode.last_metadata_changed = current_timestamp();

            if self.write_inode(&mut existing_inode).is_err() {
                reply.error(EIO);
                return;
            }

            if existing_inode.hard_links == 0 {
                self.delete_inode(existing_inode.id);
            }
        }

        let mut dentry = self.get_dentry(&parent_inode).unwrap();
        dentry.entries.remove(name.to_str().unwrap());

        if self.write_dentry(&mut parent_inode, &mut dentry).is_err() {
            reply.error(EIO);
            return;
        }

        let mut dentry = self.get_dentry(&new_parent_inode).unwrap();
        dentry
            .entries
            .insert(new_name.to_str().unwrap().to_string(), inode.id);

        if self
            .write_dentry(&mut new_parent_inode, &mut dentry)
            .is_err()
        {
            reply.error(EIO);
            return;
        }

        parent_inode.last_metadata_changed = current_timestamp();
        parent_inode.last_modified = current_timestamp();
        self.write_inode(&mut parent_inode).unwrap();
        new_parent_inode.last_metadata_changed = current_timestamp();
        new_parent_inode.last_modified = current_timestamp();
        self.write_inode(&mut new_parent_inode).unwrap();
        inode.last_metadata_changed = current_timestamp();
        self.write_inode(&mut inode).unwrap();

        if inode.kind == FileType::Directory {
            let mut dentry = self.get_dentry(&inode).unwrap();
            dentry.entries.insert("..".to_string(), new_parent);

            if self.write_dentry(&mut inode, &mut dentry).is_err() {
                reply.error(EIO);
                return;
            }

            if self.write_inode(&mut inode).is_err() {
                reply.error(EIO);
                return;
            }
        }

        reply.ok();
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let mut inode = match self.lookup_inode(parent, name) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut parent_inode = match self.get_inode(parent) {
            Some(i) => i,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if self.get_dentry(&inode).unwrap().entries.len() > 2 {
            reply.error(ENOTEMPTY);
            return;
        }

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

        if parent_inode.mode & S_ISVTX != 0
            && req.uid() != 0
            && req.uid() != parent_inode.uid
            && req.uid() != inode.uid
        {
            reply.error(EACCES);
            return;
        }

        parent_inode.last_metadata_changed = current_timestamp();
        parent_inode.last_modified = current_timestamp();

        if self.write_inode(&mut parent_inode).is_err() {
            reply.error(EIO);
            return;
        }

        inode.hard_links = 0;
        inode.last_metadata_changed = current_timestamp();

        if self.write_inode(&mut inode).is_err() {
            reply.error(EIO);
            return;
        }

        self.delete_inode(inode.id);

        let mut dentry = self.get_dentry(&parent_inode).unwrap();
        dentry.entries.remove(name.to_str().unwrap());

        if self.write_dentry(&mut parent_inode, &mut dentry).is_err() {
            reply.error(EIO);
            return;
        }

        reply.ok();
    }

    fn access(&mut self, req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        match self.get_inode(ino) {
            Some(inode) => {
                if self.check_access(
                    inode.uid,
                    inode.gid,
                    inode.mode as u16,
                    req.uid(),
                    req.gid(),
                    mask,
                ) {
                    reply.ok();
                } else {
                    reply.error(EACCES);
                }
            }
            None => reply.error(ENOENT),
        }
    }
}
