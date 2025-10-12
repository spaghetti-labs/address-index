use crate::store::{TxRead, WriteTx};

use super::{common::{BlockHash, BlockHeight}};

pub trait BlockStoreRead {
  fn get_tip_block(&self) -> anyhow::Result<Option<(BlockHeight, BlockHash)>>;
}

pub trait BlockStoreWrite {
  fn insert_block(&mut self, hash: &BlockHash, height: &BlockHeight);
}

impl<T: TxRead> BlockStoreRead for T {
  fn get_tip_block(&self) -> anyhow::Result<Option<(BlockHeight, BlockHash)>> {
    Ok(self.last_key_value(&self.store().height_to_block_hash)?.and_then(
      |(key, value)| Some((BlockHeight::from(key), BlockHash::from(value)))
    ))
  }
}

impl BlockStoreWrite for WriteTx<'_> {
  fn insert_block(&mut self, hash: &BlockHash, height: &BlockHeight) {
    self.tx.insert(&self.store.block_hash_to_height, hash, height);
    self.tx.insert(&self.store.height_to_block_hash, height, hash);
  }
}
