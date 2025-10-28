use bitcoin::{hashes::Hash, BlockHash};

use crate::store::{Store, Batch};

use super::BlockHeight;

pub trait BlockStoreRead {
  fn get_tip_block(&self) -> anyhow::Result<Option<(BlockHeight, BlockHash)>>;
}

pub trait BlockStoreWrite {
  fn insert_blocks<'a>(&mut self, entries: impl Iterator<Item = (&'a BlockHash, BlockHeight)>);
}

impl BlockStoreRead for Store {
  fn get_tip_block(&self) -> anyhow::Result<Option<(BlockHeight, BlockHash)>> {
    let cf = self.db.cf_handle("height_to_block_hash").unwrap();
    let Some((key, value)) = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End).next().transpose()? else {
      return Ok(None);
    };
    Ok(Some((
      BlockHeight::from_be_bytes(key.as_ref().try_into()?),
      BlockHash::from_byte_array(value.as_ref().try_into()?),
    )))
  }
}

impl BlockStoreWrite for Batch<'_> {
  fn insert_blocks<'a>(&mut self, entries: impl Iterator<Item = (&'a BlockHash, BlockHeight)>) {
    let cf_hash_to_height = self.store.db.cf_handle("block_hash_to_height").unwrap();
    let cf_height_to_hash = self.store.db.cf_handle("height_to_block_hash").unwrap();

    for (hash, height) in entries {
      self.batch.put_cf(&cf_hash_to_height, hash.as_byte_array(), height.to_be_bytes());
      self.batch.put_cf(&cf_height_to_hash, height.to_be_bytes(), hash.as_byte_array());
    }
  }
}

pub fn cf_descriptors(common_opts: &rocksdb::Options) -> Vec<rocksdb::ColumnFamilyDescriptor> {
  vec![
    rocksdb::ColumnFamilyDescriptor::new("block_hash_to_height", common_opts.clone()),
    rocksdb::ColumnFamilyDescriptor::new("height_to_block_hash", common_opts.clone()),
  ]
}
