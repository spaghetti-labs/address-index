use bitcoin::{hashes::Hash, BlockHash};

use crate::store::{TxRead, WriteTx};

use super::BlockHeight;

pub trait BlockStoreRead {
  fn get_tip_block(&self) -> anyhow::Result<Option<(BlockHeight, BlockHash)>>;
}

pub trait BlockStoreWrite {
  fn insert_block(&mut self, hash: &BlockHash, height: BlockHeight);
}

impl<T: TxRead> BlockStoreRead for T {
  fn get_tip_block(&self) -> anyhow::Result<Option<(BlockHeight, BlockHash)>> {
    let Some((key, value)) = self.last_key_value(&self.store().height_to_block_hash)? else {
      return Ok(None);
    };
    Ok(Some((
      BlockHeight::from_be_bytes(key.as_ref().try_into()?),
      BlockHash::from_byte_array(value.as_ref().try_into()?),
    )))
  }
}

impl BlockStoreWrite for WriteTx<'_> {
  fn insert_block(&mut self, hash: &BlockHash, height: BlockHeight) {
    self.tx.insert(&self.store.block_hash_to_height, hash.as_byte_array(), height.to_be_bytes());
    self.tx.insert(&self.store.height_to_block_hash, height.to_be_bytes(), hash.as_byte_array());
  }
}
