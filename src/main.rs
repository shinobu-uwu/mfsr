mod mfsr;
mod types;

use std::{fs::File, io::{Cursor, Read}};

use types::super_block::SuperBlock;


fn main() {
    let mut file = File::options().read(true).open("/dev/nvme0n1p4").unwrap();
    let mut buf = [0; 1024];
    file.read(&mut buf).unwrap();
    let cursor = Cursor::new(buf);
    let sb = SuperBlock::deserialize_from(cursor).unwrap();
    dbg!(sb);
}
