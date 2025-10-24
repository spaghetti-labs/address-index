use fjall::PartitionCreateOptions;

pub mod block;
pub mod tx;
pub mod account;

pub type BlockHeight = u32;

pub struct Store {
  pub(crate) keyspace: fjall::Keyspace,

  pub(self) block_hash_to_height: fjall::Partition,
  pub(self) height_to_block_hash: fjall::Partition,

  pub(self) txid_to_tx_state: fjall::Partition,

  pub(self) locker_script_hash_to_account_state: fjall::Partition,
}

impl Store {
  pub fn open(path: &str) -> anyhow::Result<Self> {
    let keyspace = fjall::Config::new(path).cache_size(1024 * 1024 * 1024 /* 1 GB */).open()?;

    Ok(Self {
      block_hash_to_height: keyspace.open_partition("block_hash_to_height", PartitionCreateOptions::default())?,
      height_to_block_hash: keyspace.open_partition("height_to_block_hash", PartitionCreateOptions::default())?,

      txid_to_tx_state: keyspace.open_partition("txid_to_tx_state", PartitionCreateOptions::default())?,

      locker_script_hash_to_account_state: keyspace.open_partition("locker_script_hash_to_account_state", PartitionCreateOptions::default())?,

      keyspace,
    })
  }
}

pub struct Batch<'a> {
  pub(crate) store: &'a Store,
  pub(crate) batch: fjall::Batch,
}
