use std::{
    fs::OpenOptions,
    io::{BufReader, BufWriter, Cursor, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use libparted::Device;

use crate::{
    mfsr::Mfsr,
    types::{block_group::BlockGroup, super_block::SuperBlock},
    utils::get_block_group_size,
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
    let empty_bitmap = vec![0; block_size as usize];

    for _ in 0..block_group_count {
        groups.push(BlockGroup::new(empty_bitmap.clone(), empty_bitmap.clone()));
    }

    let mut buf = BufWriter::new(&file);
    BlockGroup::serialize_into(&mut buf, &groups, &mut sb)?;
    buf.flush()?;

    Ok(())
}

pub fn mount<P>(source: PathBuf, mount_point: PathBuf) -> Result<()>
where
    P: AsRef<Path>,
{
    let mut buf = 
    let fs = Mfsr::new(mount_point);
    // let fs = Mfsr::new(sb, disk)?;
    // fuser::mount2(fs, directory, &[])?;

    Ok(())
}

pub fn debug_disk(path: PathBuf) -> Result<()> {
    let mut disk = OpenOptions::new().read(true).open(path)?;
    const SB_SIZE: usize = size_of::<SuperBlock>();
    let mut buf = [0; SB_SIZE];
    disk.read_exact(&mut buf)?;
    let cursor = Cursor::new(buf);
    let sb = SuperBlock::deserialize_from(cursor)?; // the first group superblock
    dbg!(sb);

    Ok(())
}
