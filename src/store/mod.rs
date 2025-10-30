use std::iter;
use rocksdb::WaitForCompactOptions;

pub mod block;
pub mod txo;
pub mod codec;

pub type BlockHeight = u32;

pub struct Store {
  pub(self) db: rocksdb::DB,
}

impl Store {
  pub fn open(path: &str) -> anyhow::Result<Self> {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, true);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_partition_filters(true);
    block_opts.set_whole_key_filtering(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    // block_opts.set_index_type(BlockBasedIndexType::HashSearch);
    block_opts.set_block_size(4 * 1024); // 4 KB

    let cache = rocksdb::Cache::new_lru_cache(2 * 1024 * 1024 * 1024); // 2 GB
    block_opts.set_block_cache(&cache);

    opts.set_block_based_table_factory(&block_opts);

    let db = rocksdb::DB::open_cf_descriptors(
      &opts,
      path,
      iter::empty().chain(
        block::cf_descriptors(&opts),
      ).chain(
        txo::cf_descriptors(&opts),
      ),
    )?;

    db.compact_range::<Vec<u8>, Vec<u8>>(None, None);
    db.wait_for_compact(&WaitForCompactOptions::default())?;

    Ok(Self {
      db,
    })
  }
}

pub struct Batch<'a> {
  pub(crate) store: &'a Store,
  pub(crate) batch: rocksdb::WriteBatch,
}

impl <'a> Batch<'a> {
  pub fn commit(self) -> anyhow::Result<()> {
    self.store.db.write(self.batch)?;
    Ok(())
  }
}
