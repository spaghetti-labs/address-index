use bitcoin::{hashes::Hash, BlockHash};
use futures::{stream, StreamExt};
use tokio::task::block_in_place;

use crate::{bitcoin_rpc::BitcoinRpcClient, store::{self, account::{AccountStoreRead as _, AccountStoreWrite as _}, block::{BlockStoreRead as _, BlockStoreWrite}, common::{BlockHeight, Script}, txo::{self, TXOStoreRead as _, TXOStoreWrite, TXO, TXOID}, Store, WriteTx}};


pub struct Scanner<'a> {
  bitcoin_rpc: BitcoinRpcClient,
  store: &'a Store,
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

  pub async fn scan_blocks(&self) -> anyhow::Result<()> {
    let start_height = block_in_place(||{
      let tx = self.store.read_tx();
      tx.get_tip_block()
    })?.map_or(0, |(height, _)| height.height + 1);

    let block_height_iter = start_height..;
    let block_hash_stream = stream::iter(block_height_iter).map(|height| async move {
      let block_hash = self.bitcoin_rpc.getblockhash(height).await?;
      Ok::<_, anyhow::Error>((height, block_hash))
    }).buffered(4);
    let block_stream = block_hash_stream.map(|res| async move {
      let (height, hash) = res?;
      let block = self.bitcoin_rpc.getblock(hash).await?;
      if block.header.block_hash() != hash {
        anyhow::bail!("Mismatched block hash for height {}: expected {}, got {}", height, hash, block.header.block_hash());
      }
      Ok(block)
    }).buffered(2);
    tokio::pin!(block_stream);

    while let Some(block) = block_stream.next().await.transpose()? {
      self.scan_block(&block).await?;
      println!("Processed block: {:?}", block.block_hash());
    }

    Ok(())
  }

  async fn scan_block(&self, next_block: &bitcoin::Block) -> anyhow::Result<()> {
    let mut tx = self.store.write_tx();

    let next_block_height = match tx.get_tip_block()? {
      Some((tip_height, tip_hash)) => {
        if tip_hash.bytes != next_block.header.prev_blockhash.to_byte_array() {
          anyhow::bail!(
            "Reorg detected at height {:?}: expected previous block hash {:?}, got {:?}",
            tip_height.height + 1,
            tip_hash,
            next_block.header.prev_blockhash
          );
        }
        tip_height.height + 1
      },
      None => {
        if next_block.header.prev_blockhash != bitcoin::BlockHash::all_zeros() {
          anyhow::bail!(
            "Genesis block must have previous block hash of all zeros, got {:?}",
            next_block.header.prev_blockhash
          );
        }
        0
      }
    };

    self.scan_block_transactions(&next_block, next_block_height.into(), &mut tx).await?;

    block_in_place(||tx.insert_block(&next_block.block_hash().to_raw_hash().to_byte_array().into(), &next_block_height.into()));

    block_in_place(||tx.commit())?;

    Ok(())
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

        let Some(txo) = block_in_place(||tx.get_txo(&txoid))? else {
          anyhow::bail!("TXO not found for input: {:?}, {:?}", vin, txoid);
        };

        let prev_balance = block_in_place(||tx.get_recent_balance(&txo.locker_script))?;
        let new_balance = prev_balance.satoshis.checked_sub(txo.value.into()).ok_or_else(|| anyhow::anyhow!(
          "Negative balance for script {:?}: {:?} - {:?} @ {:?}",
          txo.locker_script, prev_balance, txo.value, transaction.compute_txid(),
        ))?.into();
        block_in_place(||tx.insert_balance(&txo.locker_script, &height, &new_balance));
      }

      for (vout_index, vout) in transaction.output.iter().enumerate() {
        let txoid = TXOID {
          txid: transaction.compute_txid().into(),
          vout: vout_index as u32,
        };

        block_in_place(||tx.insert_txo(&height, &txoid, &TXO {
          locker_script: vout.script_pubkey.to_bytes().into(),
          value: vout.value.to_sat().into(),
        }));

        let prev_balance = block_in_place(||tx.get_recent_balance(&vout.script_pubkey.to_bytes().into()))?;
        let new_balance = prev_balance.satoshis.checked_add(vout.value.to_sat()).ok_or_else(|| anyhow::anyhow!(
          "Overflow balance for script {:?}: {:?} + {:?} @ {:?}#{:?}",
          vout.script_pubkey, prev_balance, vout.value, height, transaction.compute_txid(),
        ))?.into();
        block_in_place(||tx.insert_balance(&vout.script_pubkey.to_bytes().into(), &height, &new_balance));
      }
    }

    Ok(())
  }
}
