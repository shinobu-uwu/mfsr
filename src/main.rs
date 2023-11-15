mod cli;
mod mfsr;
mod types;
mod utils;

use std::{path::PathBuf, fs::OpenOptions, time::{SystemTime, UNIX_EPOCH}, io::{BufWriter, Write, Cursor, Read}};

use block_utils::{get_all_device_info, get_device_info};
use clap::Parser;
use cli::args::{Args, Commands};
use types::super_block::{SuperBlock, SB_MAGIC_NUMBER};

fn main() {
    let mut file = OpenOptions::new().read(true).open("/dev/sdb4").unwrap();
    let mut buf = [0; 1024];
    file.read_exact(&mut buf).unwrap();
    let mut cursor = Cursor::new(&buf);
    let sb = SuperBlock::deserialize_from(&mut cursor).unwrap();
    dbg!(sb);
    // let args = Args::parse();
    //
    // match args.command {
    //     Commands::Mkfs { path, block_size } => mkfs(path, block_size),
    //     _ => todo!(),
    // }
}

#[allow(unused_variables)]
fn mkfs(path: PathBuf, block_size: u32) {
    let device = get_device_info(&path).expect("Invalid device");
    let uid = nix::unistd::geteuid().as_raw() as u64;
    let gid = nix::unistd::getegid().as_raw() as u64;
    let mut sb = SuperBlock{
        magic: SB_MAGIC_NUMBER,
        block_size,
        created_at: SystemTime::now(),
        modified_at: SystemTime::now(),
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
    let file = OpenOptions::new().write(true).open(&path).expect("Invalid device");
    let mut buf = BufWriter::new(&file);
    sb.serialize_into(&mut buf).expect("Failed to serialize superblock");
    buf.flush().expect("Failed to flush superblock");
}
