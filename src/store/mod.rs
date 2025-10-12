pub mod block;
pub mod utxo;
pub mod common;

pub struct Store {
  keyspace: fjall::Keyspace,
}

impl Store {
  pub fn open(path: &str) -> anyhow::Result<Self> {
    let keyspace = fjall::Keyspace::open(fjall::Config::new(path))?;
    Ok(Self { keyspace })
  }

  pub fn block_store(&self) -> anyhow::Result<block::BlockStore> {
    Ok(block::BlockStore {
      partition: self.keyspace.open_partition("block", fjall::PartitionCreateOptions::default())?,
    })
  }

  pub fn utxo_store(&self) -> anyhow::Result<utxo::UTXOStore> {
    Ok(utxo::UTXOStore {
      partition: self.keyspace.open_partition("utxo", fjall::PartitionCreateOptions::default())?,
    })
  }

  pub fn batch(&self) -> anyhow::Result<Batch> {
    let batch = self.keyspace.batch();
    Ok(Batch { batch })
  }
}

pub struct Batch {
  batch: fjall::Batch,
}

impl Batch {
  pub fn commit(self) -> anyhow::Result<()> {
    self.batch.commit()?;
    Ok(())
  }
}

