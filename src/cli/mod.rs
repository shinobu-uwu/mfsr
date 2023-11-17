use std::{
    fs::OpenOptions,
    io::{BufReader, BufWriter, Cursor, Read, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use block_utils::get_device_info;

use crate::{
    mfsr::Mfsr,
    types::super_block::{SuperBlock, SB_MAGIC_NUMBER},
};

pub mod args;

pub fn mkfs(path: PathBuf, block_size: u32) -> Result<()> {
    let device = get_device_info(&path)?;
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let now = SystemTime::now();
    let mut sb = SuperBlock {
        magic: SB_MAGIC_NUMBER,
        block_size,
        created_at: now,
        modified_at: now,
        last_mounted_at: UNIX_EPOCH,
        block_count: device.capacity,
        inode_count: device.capacity,
        free_blocks: device.capacity,
        free_inodes: device.capacity,
        groups: device.capacity / 64,
        data_blocks_per_group: 64,
        uid,
        gid,
        checksum: 0,
    };
    let file = OpenOptions::new().write(true).open(&path)?;
    let mut buf = BufWriter::new(&file);
    sb.serialize_into(&mut buf)?;
    buf.flush()?;

    Ok(())
}

pub fn mount(source: PathBuf, directory: PathBuf) -> Result<()> {
    let disk = OpenOptions::new().read(true).write(true).open(&source)?;
    let buf = BufReader::new(&disk);
    let sb = SuperBlock::deserialize_from(buf)?;
    let fs = Mfsr::new(sb, disk)?;
    fuser::mount2(fs, directory, &[])?;

    Ok(())
}

pub fn debug_disk(path: PathBuf) -> Result<()> {
    let mut file = OpenOptions::new().read(true).open(&path)?;
    const SB_SIZE: usize = std::mem::size_of::<SuperBlock>();
    let mut buf = [0; SB_SIZE];
    file.read_exact(&mut buf)?;
    let cursor = Cursor::new(buf);
    let sb = SuperBlock::deserialize_from(cursor)?;
    dbg!(sb);
    Ok(())
}
