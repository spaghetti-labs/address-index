use std::time::Duration;

use bitcoin_hashes::{Hash160, Sha256};
use tokio::time::sleep;

use crate::{bitcoin::{self, ScriptPubKey, TxOutput}, store};

pub struct Scanner {
  bitcoin_rpc: bitcoin::BitcoinRpcClient,
  store: store::Store,
  block_store: store::block::BlockStore,
  utxo_store: store::utxo::UTXOStore,
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
    let utxo_store = store.utxo_store()?;
    Ok(Self {
      bitcoin_rpc,
      store,
      block_store,
      utxo_store,
    })
  }

  async fn identify_next_block(&self, start_height: u64) -> anyhow::Result<BlockIdentifier> {
    let Some(store::block::Block {
      height: last_stored_block_height,
      ..
    }) = self.block_store.last_block()? else {
      return Ok(BlockIdentifier::ByHeight(start_height));
    };
    return Ok(BlockIdentifier::ByHeight(last_stored_block_height.height + 1));
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

  pub async fn scan(&self, start_height: u64) -> anyhow::Result<()> {
    let mut next_block = self.identify_next_block(start_height).await?;
    println!("Starting scan from {:?}", next_block);

    loop {
      println!("Fetching block: {:?}", next_block);

      let block = self.get_block(next_block).await?;
      println!("Fetched block: {:?}", block);

      let mut batch = self.store.batch()?;
      self.block_store.insert_block(&store::block::Block{
        hash: block.hash.0.into(),
        height: block.height.into(),
      }, &mut batch);

      self.scan_transaction(&block, &mut batch).await?;
      
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

  pub async fn scan_transaction(&self, block: &bitcoin::Block, batch: &mut store::Batch) -> anyhow::Result<()> {
    for tx in &block.tx {
      for vout in tx.vout.iter() {
        let Some(address) = &vout.scriptPubKey.address else {
          continue;
        };

        let utxo = store::utxo::UTXO {
          id: store::utxo::UTXOID {
            txid: tx.txid.0.into(),
            vout: vout.n,
          },
          value: vout.value.satoshis.into(),
          address: address.clone(),
        };

        println!("Inserting UTXO: {:?}", utxo);

        self.utxo_store.insert_utxo(&utxo, batch);
      }
    }
    Ok(())
  }
}
