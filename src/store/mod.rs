use fjall::{KvPair, PartitionCreateOptions, TxPartitionHandle, UserValue};

pub mod common;
pub mod block;
pub mod txo;
pub mod account;

pub struct Store {
  keyspace: fjall::TxKeyspace,

  pub(self) block_hash_to_height: fjall::TxPartition,
  pub(self) height_to_block_hash: fjall::TxPartition,

  pub(self) txoid_to_txo: fjall::TxPartition,
  pub(self) height_and_txoid: fjall::TxPartition,

  pub(self) locker_script_and_height_to_balance: fjall::TxPartition,
  pub(self) height_and_locker_script: fjall::TxPartition,
}

impl Store {
  pub fn open(path: &str) -> anyhow::Result<Self> {
    let keyspace = fjall::Config::new(path).open_transactional()?;

    Ok(Self {
      block_hash_to_height: keyspace.open_partition("block_hash_to_height", PartitionCreateOptions::default())?,
      height_to_block_hash: keyspace.open_partition("height_to_block_hash", PartitionCreateOptions::default())?,

      txoid_to_txo: keyspace.open_partition("txoid_to_txo", PartitionCreateOptions::default())?,
      height_and_txoid: keyspace.open_partition("height_and_txoid", PartitionCreateOptions::default())?,

      locker_script_and_height_to_balance: keyspace.open_partition("locker_script_and_height_to_balance", PartitionCreateOptions::default())?,
      height_and_locker_script: keyspace.open_partition("height_and_locker_script", PartitionCreateOptions::default())?,

      keyspace,
    })
  }

  pub fn read_tx(&self) -> ReadTx {
    let tx = self.keyspace.read_tx();
    ReadTx {
      store: self,
      tx,
    }
  }

  pub fn write_tx(&self) -> WriteTx {
    let tx = self.keyspace.write_tx();
    WriteTx {
      store: self,
      tx,
    }
  }
}

pub struct ReadTx<'a> {
  pub(self) store: &'a Store,
  pub(self) tx: fjall::ReadTransaction,
}

pub struct WriteTx<'a> {
  pub(self) store: &'a Store,
  pub(self) tx: fjall::WriteTransaction<'a>,
}

impl WriteTx<'_> {
  pub fn commit(self) -> anyhow::Result<()> {
    self.tx.commit()?;
    Ok(())
  }
}

// See: https://github.com/fjall-rs/fjall/issues/188
pub(self) trait TxRead {
  fn store(&self) -> &Store;
  fn last_key_value(&self, partition: &TxPartitionHandle) -> fjall::Result<Option<KvPair>>;
  fn get<K: AsRef<[u8]>>(&self, partition: &TxPartitionHandle, key: K) -> fjall::Result<Option<UserValue>>;
  fn prefix<'b, K: AsRef<[u8]> + 'b>(
    &'b self,
    partition: &'b TxPartitionHandle,
    prefix: K,
  ) -> impl DoubleEndedIterator<Item = fjall::Result<KvPair>> + 'b;
}

macro_rules! impl_tx_read {
  ($type:ty, $tx_field:ident) => {
    impl TxRead for $type {
      fn store(&self) -> &Store {
        self.store
      }

      fn last_key_value(&self, partition: &TxPartitionHandle) -> fjall::Result<Option<KvPair>> {
        self.$tx_field.last_key_value(partition)
      }

      fn get<K: AsRef<[u8]>>(&self, partition: &TxPartitionHandle, key: K) -> fjall::Result<Option<UserValue>> {
        self.$tx_field.get(partition, key)
      }

      fn prefix<'b, K: AsRef<[u8]> + 'b>(
        &'b self,
        partition: &'b TxPartitionHandle,
        prefix: K,
      ) -> impl DoubleEndedIterator<Item = fjall::Result<KvPair>> + 'b {
        self.$tx_field.prefix(partition, prefix)
      }
    }
  };
}

impl_tx_read!(ReadTx<'_>, tx);
impl_tx_read!(WriteTx<'_>, tx);
