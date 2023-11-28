use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{types::super_block::SuperBlock, utils::get_block_group_size};

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockGroup {
    pub data_bitmap: Vec<u8>,
    pub inode_bitmap: Vec<u8>,
}

impl BlockGroup {
    pub fn new(data_bitmap: Vec<u8>, inode_bitmap: Vec<u8>) -> Self {
        Self {
            data_bitmap,
            inode_bitmap,
        }
    }

    pub fn serialize_into<W>(mut w: W, groups: &[Self], super_block: &mut SuperBlock) -> Result<()>
    where
        W: Write + Seek,
    {
        assert!(!groups.is_empty());
        let block_size = super_block.block_size;

        for (i, g) in groups.iter().enumerate() {
            let offset = get_block_group_size(block_size) * i as u64;
            let mut w_ref = w.by_ref();
            w_ref.seek(SeekFrom::Start(offset))?;
            super_block.serialize_into(&mut w_ref)?;
            // first block of the group will always be the super block
            w_ref.seek(SeekFrom::Start(offset + block_size as u64))?;
            w_ref.write_all(&g.data_bitmap)?;
            w_ref.write_all(&g.inode_bitmap)?;
        }

        Ok(())
    }

    pub fn deserialize_from<R>(mut r: R, block_size: u32, count: usize) -> Result<Vec<Self>>
    where
        R: Read + Seek,
    {
        let mut groups = Vec::with_capacity(count);

        for i in 0..count {
            let offset = get_block_group_size(block_size) * i as u64 + block_size as u64;
            r.seek(SeekFrom::Start(offset))?;
            let mut data_bitmap = vec![0; block_size as usize];
            let mut inode_bitmap = vec![0; block_size as usize];
            r.read_exact(&mut data_bitmap)?;
            r.read_exact(&mut inode_bitmap)?;
            groups.push(BlockGroup::new(data_bitmap, inode_bitmap));
        }

        Ok(groups)
    }
}
