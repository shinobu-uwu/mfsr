use std::path::PathBuf;

use clap::command;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author="Matheus Filipe dos Santos Reinert", version="0.1.0", about="Utilities for the MFSR filesystem", long_about = None)]
#[command(propagate_version = true)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Mkfs {
        path: PathBuf,
        #[arg(default_value = "4096", short, long)]
        block_size: u32,
    },
    Mount {
        source: PathBuf,
        directory: PathBuf,
    },
}
