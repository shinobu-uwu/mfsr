mod cli;
mod mfsr;
mod types;
mod utils;

use anyhow::Result;
use clap::Parser;
use cli::{
    args::{Args, Commands},
    debug_disk, mkfs, mount,
};

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Mkfs {
            disk_path,
            block_size,
        } => mkfs(disk_path, block_size),
        Commands::Debug { disk_path } => debug_disk(disk_path),
        Commands::Mount { source, directory } => mount(source, directory),
    }
}
