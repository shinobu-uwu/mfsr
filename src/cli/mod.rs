use std::{
    fs::OpenOptions,
    io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write, BufRead},
    path::PathBuf, mem::size_of,
};

use anyhow::{Result, anyhow};
use libparted::Device;

use crate::{
    types::{super_block::SuperBlock, block_group::BlockGroup}, utils::get_block_group_size,
};

pub mod args;

pub fn mkfs(path: PathBuf, block_size: u32) -> Result<()> {
    let device = Device::new(&path)?;
    let device_size = device.length() * device.phys_sector_size();

    if device.phys_sector_size() > block_size as u64 {
        return Err(anyhow!("The specified block size must be bigger than the device's cluster (physical block size), block size: {}, cluster size: {}", block_size, device.phys_sector_size()));
    }

    let block_group_count = device_size / get_block_group_size(block_size);
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let data_blocks_per_group = block_size as u64 * 8;
    let mut sb = SuperBlock::new(
        block_size,
        block_group_count,
        data_blocks_per_group,
        uid,
        gid,
    );

    let file = OpenOptions::new().write(true).open(&path)?;
    let mut groups: Vec<BlockGroup> = Vec::with_capacity(block_group_count as usize);
    let empty_bitmap = vec![1; block_size as usize];

    for _ in 0..block_group_count {
        groups.push(BlockGroup::new(empty_bitmap.clone(), empty_bitmap.clone()));
    }

    let mut buf = BufWriter::new(&file);
    BlockGroup::serialize_into(&mut buf, &groups, &mut sb)?;
    buf.flush()?;

    Ok(())
}

pub fn mount(source: PathBuf, directory: PathBuf) -> Result<()> {
    let disk = OpenOptions::new().read(true).write(true).open(source)?;
    let buf = BufReader::new(&disk);
    let sb = SuperBlock::deserialize_from(buf)?;
    // let fs = Mfsr::new(sb, disk)?;
    // fuser::mount2(fs, directory, &[])?;

    Ok(())
}

pub fn debug_disk(path: PathBuf) -> Result<()> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    const SB_SIZE: usize = size_of::<SuperBlock>();
    let mut buf = [0; SB_SIZE];
    file.read_exact(&mut buf)?;
    let cursor = Cursor::new(buf);
    let sb = SuperBlock::deserialize_from(cursor)?; // the first group superblock
    dbg!(&sb);

    let size = sb.block_group_count as usize * size_of::<BlockGroup>();
    let mut group_buf = vec![0; size];
    file.rewind()?;
    file.read_exact(&mut group_buf)?;
    let cursor = Cursor::new(&mut group_buf);
    let groups = BlockGroup::deserialize_from(cursor, sb.block_size, sb.block_group_count as usize)?;
    dbg!(groups);

    Ok(())
}
