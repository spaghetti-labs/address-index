pub mod block;
pub mod txo;
pub mod common;
pub mod address;

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

  pub fn txo_store(&self) -> anyhow::Result<txo::TXOStore> {
    Ok(txo::TXOStore {
      partition: self.keyspace.open_partition("txo", fjall::PartitionCreateOptions::default())?,
    })
  }

  pub fn address_store(&self) -> anyhow::Result<address::AddressStore> {
    Ok(address::AddressStore {
      partition: self.keyspace.open_partition("address", fjall::PartitionCreateOptions::default())?,
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

