use std::time::Duration;

use tokio::time::sleep;

use crate::{bitcoin, store};

pub struct Scanner {
  bitcoin_rpc: bitcoin::BitcoinRpcClient,
  store: store::Store,
  block_store: store::BlockStore,
}

#[derive(Debug)]
enum BlockIdentifier {
  ByHeight(u64),
  ByHash(bitcoin::BlockHash),
}

impl Scanner {
  pub fn open(
    bitcoin_rpc: bitcoin::BitcoinRpcClient,
    store: store::Store,
  ) -> anyhow::Result<Self> {
    let block_store = store.block_store()?;
    Ok(Self {
      bitcoin_rpc,
      store,
      block_store,
    })
  }

  async fn identify_next_block(&self) -> anyhow::Result<BlockIdentifier> {
    let Some(store::Block {
      height: store::BlockHeight(last_stored_block),
      ..
    }) = self.block_store.last_block()? else {
      return Ok(BlockIdentifier::ByHeight(0));
    };
    return Ok(BlockIdentifier::ByHeight(last_stored_block + 1));
  }

  async fn get_block(&self, identifier: BlockIdentifier) -> anyhow::Result<bitcoin::Block> {
    let hash = match &identifier {
      BlockIdentifier::ByHeight(height) => self.bitcoin_rpc.getblockhash(*height).await?,
      BlockIdentifier::ByHash(hash) => hash.clone(),
    };
    let block = self.bitcoin_rpc.getblock(hash).await?;
    match &identifier {
      BlockIdentifier::ByHeight(height) if block.height != *height => {
        anyhow::bail!("Expected block at height {}, but got block at height {}", height, block.height);
      }
      BlockIdentifier::ByHash(hash) if block.hash.0 != hash.0 => {
        anyhow::bail!("Expected block with hash {:?}, but got block with hash {:?}", hash, block.hash);
      }
      _ => {}
    }
    Ok(block)
  }

  pub async fn scan(&self) -> anyhow::Result<()> {
    let mut next_block = self.identify_next_block().await?;
    println!("Starting scan from {:?}", next_block);

    loop {
      println!("Fetching block: {:?}", next_block);

      let block = self.get_block(next_block).await?;
      println!("Fetched block: {:?}", block);

      let mut batch = self.store.batch()?;
      self.block_store.insert_block(store::Block{
        hash: block.hash.0,
        height: store::BlockHeight(block.height),
      }, &mut batch);
      batch.commit()?;
      println!("Stored block at height {}", block.height);

      if let Some(next_hash) = block.nextblockhash {
        next_block = BlockIdentifier::ByHash(next_hash);
      } else {
        next_block = BlockIdentifier::ByHeight(block.height + 1);
        println!("Reached the tip of the blockchain at block height {}", block.height);
        sleep(Duration::from_secs(5)).await;
      }
    }
  }
}
