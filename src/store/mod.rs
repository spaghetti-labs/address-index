use fjall::{PartitionCreateOptions};

use crate::store::id::IDGenerator;

pub mod block;
pub mod txo;
pub mod account;
pub mod script;
pub mod id;

pub type BlockHeight = u32;

pub struct Store {
  pub(crate) keyspace: fjall::Keyspace,
  id_gen: IDGenerator,

  pub(self) block_hash_to_height: fjall::Partition,
  pub(self) height_to_block_hash: fjall::Partition,

  pub(self) txoid_to_utxo: fjall::Partition,

  pub(self) locker_script_id_to_balance: fjall::Partition,
  pub(self) locker_script_id_and_height_to_balance: fjall::Partition,
  pub(self) height_and_locker_script_id: fjall::Partition,

  pub(self) script_to_script_id: fjall::Partition,
  pub(self) script_id_to_script: fjall::Partition,
}

impl Store {
  pub fn open(path: &str) -> anyhow::Result<Self> {
    let keyspace = fjall::Config::new(path).cache_size(1024 * 1024 * 1024 /* 1 GB */).open()?;

    Ok(Self {
      block_hash_to_height: keyspace.open_partition("block_hash_to_height", PartitionCreateOptions::default())?,
      height_to_block_hash: keyspace.open_partition("height_to_block_hash", PartitionCreateOptions::default())?,

      txoid_to_utxo: keyspace.open_partition("txoid_to_utxo", PartitionCreateOptions::default())?,

      locker_script_id_to_balance: keyspace.open_partition("locker_script_id_to_balance", PartitionCreateOptions::default())?,
      locker_script_id_and_height_to_balance: keyspace.open_partition("locker_script_id_and_height_to_balance", PartitionCreateOptions::default())?,
      height_and_locker_script_id: keyspace.open_partition("height_and_locker_script_id", PartitionCreateOptions::default())?,

      script_to_script_id: keyspace.open_partition("script_to_script_id", PartitionCreateOptions::default())?,
      script_id_to_script: keyspace.open_partition("script_id_to_script", PartitionCreateOptions::default())?,

      keyspace,
      id_gen: IDGenerator::new(),
    })
  }
}

pub struct Batch<'a> {
  pub(crate) store: &'a Store,
  pub(crate) batch: fjall::Batch,
}
