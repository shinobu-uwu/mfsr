#![allow(unused_variables)]
use std::env::args;

use mfsr::Mfsr;

mod mfsr;
mod types;
mod utils;

fn main() {
    let path = args().nth(1).expect("Select a partition to be formatted");

    fuser::mount2(Mfsr::new(), path.clone(), &[]).unwrap();
}
