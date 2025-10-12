use crate::impl_bincode_conversion;
use super::Batch;

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct Block {
  pub hash: super::common::BlockHash,
  pub height: super::common::BlockHeight,
}
impl_bincode_conversion!(Block);

pub struct BlockStore {
  pub(super) partition: fjall::Partition,
}

impl BlockStore {
  pub fn last_block(&self) -> anyhow::Result<Option<Block>> {
    Ok(
      self.partition.last_key_value()?
        .map(|(_, value)| value)
        .map(Block::from)
    )
  }

  pub fn insert_block(&self, block: &Block, batch: &mut Batch) {
    batch.batch.insert(&self.partition, &block.height, block);
  }
}
