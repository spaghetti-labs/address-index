use bitcoin::hashes::Hash;

use crate::{bitcoin_rpc::BitcoinRpcClient, store::{self, account::{AccountStoreRead as _, AccountStoreWrite as _}, block::{BlockStoreRead as _, BlockStoreWrite}, common::{Amount, BlockHash, BlockHeight, Script}, txo::{self, TXOStoreRead as _, TXOStoreWrite, TXO, TXOID}, Store, WriteTx}};


pub struct Scanner<'a> {
  bitcoin_rpc: BitcoinRpcClient,
  store: &'a Store,
}

pub enum ScanResult {
  ProcessedBlock {
    block_hash: bitcoin::BlockHash,
  },
  NoNewBlock,
}

impl<'a> Scanner<'a> {
  pub fn open(
    bitcoin_rpc: BitcoinRpcClient,
    store: &'a Store,
  ) -> anyhow::Result<Self> {
    Ok(Self {
      bitcoin_rpc,
      store,
    })
  }

  pub async fn scan_next_block(&self) -> anyhow::Result<ScanResult> {
    let mut tx = self.store.write_tx();

    let (prev_block_hash, next_block_height) = match tx.get_tip_block()? {
      Some((height, hash)) => (Some(hash), height.height + 1),
      None => (None, 0),
    };

    let blockchain_info = self.bitcoin_rpc.getblockchaininfo().await?;
    if next_block_height > blockchain_info.blocks {
      return Ok(ScanResult::NoNewBlock);
    }

    let next_block_hash = self.bitcoin_rpc.getblockhash(next_block_height).await?;

    println!("Fetching block: {:?} @ {:?}", next_block_hash, next_block_height);
    let next_block = self.bitcoin_rpc.getblock(next_block_hash).await?;

    if let Some(prev_block_hash) = prev_block_hash {
      if prev_block_hash.bytes != next_block.header.prev_blockhash.to_byte_array() {
        anyhow::bail!(
          "Reorg detected at height {}: expected previous block hash {:?}, got {:?}",
          next_block_height,
          prev_block_hash,
          next_block.header.prev_blockhash
        );
      }
    }

    self.scan_block_transactions(&next_block, next_block_height.into(), &mut tx).await?;

    tx.insert_block(&next_block.block_hash().to_raw_hash().to_byte_array().into(), &next_block_height.into());

    tx.commit()?;

    Ok(ScanResult::ProcessedBlock {
      block_hash: next_block.block_hash(),
    })
  }

  async fn scan_block_transactions(&self, block: &bitcoin::Block, height: BlockHeight, tx: &mut WriteTx<'_>) -> anyhow::Result<()> {
    for transaction in &block.txdata {
      for vin in &transaction.input {
        if vin.previous_output.is_null() {
          continue;
        }
        let txoid = TXOID {
          txid: vin.previous_output.txid.into(),
          vout: vin.previous_output.vout,
        };

        let Some(txo) = tx.get_txo(&txoid)? else {
          anyhow::bail!("TXO not found for input: {:?}, {:?}", vin, txoid);
        };

        let prev_balance = tx.get_recent_balance(&txo.locker_script)?;
        let new_balance = prev_balance.satoshis.checked_sub(txo.value.into()).ok_or_else(|| anyhow::anyhow!(
          "Negative balance for script {:?}: {:?} - {:?} @ {:?}",
          txo.locker_script, prev_balance, txo.value, transaction.compute_txid(),
        ))?.into();
        tx.insert_balance(&txo.locker_script, &height, &new_balance);
      }

      for (vout_index, vout) in transaction.output.iter().enumerate() {
        let txoid = TXOID {
          txid: transaction.compute_txid().into(),
          vout: vout_index as u32,
        };

        tx.insert_txo(&height, &txoid, &TXO {
          locker_script: vout.script_pubkey.to_bytes().into(),
          value: vout.value.to_sat().into(),
        });

        let prev_balance = tx.get_recent_balance(&vout.script_pubkey.to_bytes().into())?;
        let new_balance = prev_balance.satoshis.checked_add(vout.value.to_sat()).ok_or_else(|| anyhow::anyhow!(
          "Overflow balance for script {:?}: {:?} + {:?} @ {:?}#{:?}",
          vout.script_pubkey, prev_balance, vout.value, height, transaction.compute_txid(),
        ))?.into();
        tx.insert_balance(&vout.script_pubkey.to_bytes().into(), &height, &new_balance);
      }
    }

    Ok(())
  }
}
