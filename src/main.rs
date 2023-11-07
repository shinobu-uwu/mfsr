use std::env::args;

use mfsr::Mfsr;

use crate::types::Inode;

mod mfsr;
mod types;
mod utils;

fn main() {
    let path = args()
        .nth(1)
        .expect("You need specify a path for the file system to be mounted");

    fuser::mount2(Mfsr::new(), path.clone(), &[]).unwrap();
}
