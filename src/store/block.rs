use bitcoin::{hashes::Hash, BlockHash};

use crate::store::{Store, Batch};

use super::BlockHeight;

pub trait BlockStoreRead {
  fn get_tip_block(&self) -> anyhow::Result<Option<(BlockHeight, BlockHash)>>;
}

pub trait BlockStoreWrite {
  fn insert_block(&mut self, hash: &BlockHash, height: BlockHeight);
}

impl BlockStoreRead for Store {
  fn get_tip_block(&self) -> anyhow::Result<Option<(BlockHeight, BlockHash)>> {
    let Some((key, value)) = self.height_to_block_hash.last_key_value()? else {
      return Ok(None);
    };
    Ok(Some((
      BlockHeight::from_be_bytes(key.as_ref().try_into()?),
      BlockHash::from_byte_array(value.as_ref().try_into()?),
    )))
  }
}

impl BlockStoreWrite for Batch<'_> {
  fn insert_block(&mut self, hash: &BlockHash, height: BlockHeight) {
    self.batch.insert(&self.store.block_hash_to_height, hash.as_byte_array(), height.to_be_bytes());
    self.batch.insert(&self.store.height_to_block_hash, height.to_be_bytes(), hash.as_byte_array());
  }
}
