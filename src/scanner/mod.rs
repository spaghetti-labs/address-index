use crate::{bitcoin, store::{self, account::{AccountStoreRead as _, AccountStoreWrite as _}, block::{BlockStoreRead as _, BlockStoreWrite}, common::{Amount, BlockHash, Script}, txo::{self, TXOStoreRead as _, TXOStoreWrite, TXO, TXOID}, Store, WriteTx}};


pub struct Scanner<'a> {
  bitcoin_rpc: bitcoin::BitcoinRpcClient,
  store: &'a Store,
}

#[derive(Debug)]
enum BlockIdentifier {
  ByHeight(u64),
  ByHash(bitcoin::BlockHash),
}

pub struct ScanNextBlockHint {
  next_block_hash: Option<bitcoin::BlockHash>,
}

impl Default for ScanNextBlockHint {
  fn default() -> Self {
    Self { next_block_hash: None }
  }
}

pub enum ScanResult {
  ProcessedBlock {
    scan_next_block_hint: ScanNextBlockHint,
  },
  NoNewBlock,
}

impl<'a> Scanner<'a> {
  pub fn open(
    bitcoin_rpc: bitcoin::BitcoinRpcClient,
    store: &'a Store,
  ) -> anyhow::Result<Self> {
    Ok(Self {
      bitcoin_rpc,
      store,
    })
  }

  pub async fn scan_next_block(&self, hint: &ScanNextBlockHint) -> anyhow::Result<ScanResult> {
    let mut tx = self.store.write_tx();

    let (prev_block_hash, next_block_height) = match tx.get_tip_block()? {
      Some((height, hash)) => (Some(hash), height.height + 1),
      None => (None, 0),
    };

    let next_block_hash = match &hint.next_block_hash {
      Some(hash) => hash.clone(),
      None => {
        let blockchain_info = self.bitcoin_rpc.getblockchaininfo().await?;
        if next_block_height > blockchain_info.blocks {
          return Ok(ScanResult::NoNewBlock);
        }

        self.bitcoin_rpc.getblockhash(next_block_height).await?
      }
    };

    println!("Fetching block: {:?} @ {:?}", next_block_hash, next_block_height);
    let next_block = self.bitcoin_rpc.getblock(next_block_hash).await?;

    if next_block.height != next_block_height {
      anyhow::bail!(
        "Unexpected block height: expected {}, got {}",
        next_block_height,
        next_block.height
      );
    }

    if let Some(prev_block_hash) = prev_block_hash {
      let Some(received_previousblockhash) = &next_block.previousblockhash else {
        anyhow::bail!("Block at height {} has no `previousblockhash`", next_block.height);
      };
      if prev_block_hash.bytes != received_previousblockhash.0 {
        anyhow::bail!(
          "Reorg detected at height {}: expected previous block hash {:?}, got {:?}",
          next_block.height,
          prev_block_hash,
          received_previousblockhash.0
        );
      }
    }

    self.scan_block_transactions(&next_block, &mut tx).await?;

    tx.insert_block(&next_block.hash.0.into(), &next_block.height.into());

    tx.commit()?;

    Ok(ScanResult::ProcessedBlock {
      scan_next_block_hint: ScanNextBlockHint {
        next_block_hash: next_block.nextblockhash,
      }
    })
  }

  async fn scan_block_transactions(&self, block: &bitcoin::Block, tx: &mut WriteTx<'_>) -> anyhow::Result<()> {
    for transaction in &block.tx {
      for vin in &transaction.vin {
        let txoid = match vin {
          bitcoin::TxInput::Coinbase { .. } => continue,
          bitcoin::TxInput::Normal { txid, vout } => TXOID {
            txid: txid.0.into(),
            vout: *vout,
          },
        };

        let Some(txo) = tx.get_txo(&txoid)? else {
          anyhow::bail!("TXO not found for input: {:?}, {:?}", vin, txoid);
        };

        let prev_balance = tx.get_recent_balance(&txo.locker_script)?;
        let new_balance = prev_balance.satoshis.checked_sub(txo.value.into()).ok_or_else(|| anyhow::anyhow!(
          "Negative balance for script {:?}: {:?} - {:?} @ {:?}#{:?}",
          txo.locker_script, prev_balance, txo.value, block.height, transaction.txid,
        ))?.into();
        tx.insert_balance(&txo.locker_script, &block.height.into(), &new_balance);
      }

      for vout in transaction.vout.iter() {
        let txoid = TXOID {
          txid: transaction.txid.0.into(),
          vout: vout.n,
        };

        tx.insert_txo(&block.height.into(), &txoid, &TXO {
          locker_script: vout.scriptPubKey.hex.0.clone().into(),
          value: vout.value.satoshis.into(),
        });

        let prev_balance = tx.get_recent_balance(&vout.scriptPubKey.hex.0.clone().into())?;
        let new_balance = prev_balance.satoshis.checked_add(vout.value.satoshis).ok_or_else(|| anyhow::anyhow!(
          "Overflow balance for script {:?}: {:?} + {:?} @ {:?}#{:?}",
          vout.scriptPubKey.hex, prev_balance, vout.value, block.height, transaction.txid,
        ))?.into();
        tx.insert_balance(&vout.scriptPubKey.hex.0.clone().into(), &block.height.into(), &new_balance);
      }
    }

    Ok(())
  }
}
