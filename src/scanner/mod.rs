use std::{collections::btree_map, time::Duration};

use bitcoin_hashes::{Hash160, Sha256};
use tokio::time::sleep;

use crate::{bitcoin::{self, ScriptPubKey, TxOutput}, store::{self, address::AddressState, txo}};

pub struct Scanner {
  bitcoin_rpc: bitcoin::BitcoinRpcClient,
  store: store::Store,
  block_store: store::block::BlockStore,
  txo_store: store::txo::TXOStore,
  address_store: store::address::AddressStore,
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
    let txo_store = store.txo_store()?;
    let address_store = store.address_store()?;
    Ok(Self {
      bitcoin_rpc,
      store,
      block_store,
      txo_store,
      address_store,
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

  pub async fn scan_blocks(&self, start_height: u64) -> anyhow::Result<()> {
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

      self.scan_block(&block, &mut batch).await?;
      
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

  async fn scan_block(&self, block: &bitcoin::Block, batch: &mut store::Batch) -> anyhow::Result<()> {
    let mut utxo_balance_changes = btree_map::BTreeMap::<String, i128>::new();

    let mut block_txos = btree_map::BTreeMap::<store::txo::TXOID, store::txo::TXO>::new();

    for tx in &block.tx {
      for vin in &tx.vin {
        let txoid = match vin {
          bitcoin::TxInput::Coinbase { .. } => continue,
          bitcoin::TxInput::Normal { txid, vout } => store::txo::TXOID {
            txid: txid.0.into(),
            vout: *vout,
          },
        };

        let txo = match block_txos.get(&txoid) {
          Some(txo) => txo.clone(),
          None => match self.txo_store.get_txo(&txoid)? {
            Some(txo) => txo,
            None => anyhow::bail!("TXO not found for input: {:?}", vin),
          },
        };

        let Some(address) = &txo.address else {
          continue;
        };

        *utxo_balance_changes.entry(address.clone()).or_default() -= txo.value.satoshis as i128;
      }

      for vout in tx.vout.iter() {
        let txo = store::txo::TXO {
          id: store::txo::TXOID {
            txid: tx.txid.0.into(),
            vout: vout.n,
          },
          value: vout.value.satoshis.into(),
          address: vout.scriptPubKey.address.clone(),
        };

        println!("Inserting TXO: {:?}", txo);
        self.txo_store.insert_txo(&txo, batch);
        block_txos.insert(txo.id.clone(), txo);

        if let Some(address) = &vout.scriptPubKey.address {
          *utxo_balance_changes.entry(address.clone()).or_default() += vout.value.satoshis as i128;
        }
      }
    }

    for (address, balance_change) in utxo_balance_changes {
      let balance_change: i64 = balance_change.try_into()?;

      if balance_change == 0 {
        continue;
      }

      let mut address_state = self.address_store.get_address(&address)?.unwrap_or(AddressState {
        address: address.clone(),
        utxo_balance: store::common::Amount { satoshis: 0 },
      });

      address_state.utxo_balance.satoshis = (i64::try_from(address_state.utxo_balance.satoshis)? + balance_change) as u64;

      self.address_store.insert_address(&address_state, batch);
      println!("Updated address state: {:?}", address_state);
    }

    Ok(())
  }
}
