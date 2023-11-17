#![allow(unused)]
mod cli;
mod mfsr;
mod types;
mod utils;

use std::{
    fs::OpenOptions,
    io::{BufReader, BufWriter, Cursor, Read, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use block_utils::get_device_info;
use clap::Parser;
use cli::args::{Args, Commands};
use types::super_block::{SuperBlock, SB_MAGIC_NUMBER};

use crate::mfsr::Mfsr;

    match args.command {
        Commands::Mkfs {
            disk_path,
            block_size,
        } => mkfs(disk_path, block_size),
        Commands::Debug { disk_path } => debug_disk(disk_path),
        _ => todo!(),
    }
}

fn mkfs(path: PathBuf, block_size: u32) {
    let device = get_device_info(&path).expect("Invalid device");
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
        checksum: 1052,
    };
    let file = OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("Invalid device");
    let mut buf = BufWriter::new(&file);
    sb.serialize_into(&mut buf)
        .expect("Failed to serialize superblock");
    buf.flush().expect("Failed to flush superblock");
}

fn mount(source: PathBuf, directory: PathBuf) {
    let disk = OpenOptions::new()
        .read(true)
        .open(&source)
        .expect("Invalid device");
    let buf = BufReader::new(&disk);
    let sb = SuperBlock::deserialize_from(buf).expect("Failed to mount: bad superblock");
    dbg!(&sb);
    let fs = Mfsr::new(sb);
    fuser::mount2(fs, directory, &[]).expect("Failed to mount filesystem");
}

fn debug_disk(path: PathBuf) {
    let mut file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("Invalid device");
    const SB_SIZE: usize = std::mem::size_of::<SuperBlock>();
    let mut buf = [0; SB_SIZE];
    file.read_exact(&mut buf).expect("Failed to read device");
    let cursor = Cursor::new(buf);
    let sb = SuperBlock::deserialize_from(cursor).expect("Failed to deserialize, bad superblock");
    dbg!(sb);
}
