pub struct Store {
  keyspace: fjall::Keyspace,
}

impl Store {
  pub fn open(path: &str) -> anyhow::Result<Self> {
    let keyspace = fjall::Keyspace::open(fjall::Config::new(path))?;
    Ok(Self { keyspace })
  }

  pub fn block_store(&self) -> anyhow::Result<BlockStore> {
    Ok(BlockStore {
      partition: self.keyspace.open_partition("blocks", fjall::PartitionCreateOptions::default())?,
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

pub struct BlockStore {
  partition: fjall::Partition,
}

#[derive(Clone, Copy, Debug)]
pub struct BlockHeight(pub u64);

impl TryFrom<fjall::Slice> for BlockHeight {
  type Error = anyhow::Error;
  fn try_from(value: fjall::Slice) -> Result<Self, Self::Error> {
    let bytes = value[0..8].try_into()?;
    Ok(BlockHeight(u64::from_be_bytes(bytes)))
  }
}

impl Into<fjall::Slice> for BlockHeight {
  fn into(self) -> fjall::Slice {
    self.0.to_be_bytes().to_vec().into()
  }
}

#[derive(Debug)]
pub struct Block {
  pub height: BlockHeight,
  pub hash: [u8; 32],
}

impl TryFrom<fjall::Slice> for Block {
  type Error = anyhow::Error;
  fn try_from(value: fjall::Slice) -> Result<Self, Self::Error> {
    if value.len() != 40 {
      return Err(anyhow::anyhow!("Invalid block encoding length"));
    }
    let height = value.slice(0..8).try_into()?;
    let hash = value[8..40].try_into()?;
    Ok(Block { height, hash })
  }
}

impl Into<fjall::Slice> for Block {
  fn into(self) -> fjall::Slice {
    let mut v = Vec::with_capacity(40);
    v.extend_from_slice(Into::<fjall::Slice>::into(self.height).as_ref());
    v.extend_from_slice(&self.hash);
    v.into()
  }
}

impl BlockStore {
  pub fn last_block(&self) -> anyhow::Result<Option<Block>> {
    self.partition.last_key_value()?
      .map(|(_, value)| value)
      .map(Block::try_from)
      .transpose()
      .into()
  }

  pub fn insert_block(&self, block: Block, batch: &mut Batch) {
    batch.batch.insert(&self.partition, block.height, block);
  }
}
