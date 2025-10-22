use std::collections::{BTreeMap};
use bitcoin::{OutPoint, ScriptBuf};

use crate::store::BlockHeight;

pub struct Batch {
  pub(crate) start_height: BlockHeight,
  pub(crate) end_height: BlockHeight,
  pub(crate) blocks: Vec<bitcoin::BlockHash>,

  pub(crate) unspent_txos: BTreeMap<OutPoint, bitcoin::TxOut>,
  pub(crate) spent_txos: BTreeMap<OutPoint, BlockHeight>,
  pub(crate) intermediate_account_changes: BTreeMap<(ScriptBuf, BlockHeight), bitcoin::SignedAmount>,
}

impl Batch {
  pub fn build(
    start_height: BlockHeight,
    blocks: &[bitcoin::Block],
  ) -> anyhow::Result<Self> {
    let mut batch = Batch {
      start_height,
      end_height: start_height + blocks.len() as BlockHeight,
      blocks: Vec::new(),
      unspent_txos: BTreeMap::new(),
      intermediate_account_changes: BTreeMap::new(),
      spent_txos: BTreeMap::new(),
    };
    for (i, block) in blocks.iter().enumerate() {
      let height = start_height + i as BlockHeight;
      batch.scan_block(height, block)?;
    }
    Ok(batch)
  }

  fn scan_block(
    &mut self,
    height: BlockHeight,
    block: &bitcoin::Block,
  ) -> anyhow::Result<()> {
    self.scan_transactions(height, block)?;
    self.blocks.push(block.block_hash());
    Ok(())
  }

  fn scan_transactions(
    &mut self,
    height: BlockHeight,
    block: &bitcoin::Block,
  ) -> anyhow::Result<()> {
    for tx in &block.txdata {
      for txin in &tx.input {
        if txin.previous_output.is_null() {
          continue;
        }
        if let Some(utxo) = self.unspent_txos.remove(&txin.previous_output) {
          *self.intermediate_account_changes.entry(
            (utxo.script_pubkey, height),
          ).or_insert(bitcoin::SignedAmount::ZERO) -= utxo.value.try_into()?;
        } else {
          self.spent_txos.insert(
            txin.previous_output,
            height,
          );
        }
      }
      for (txo_index, txout) in tx.output.iter().enumerate() {
        *self.intermediate_account_changes.entry(
          (txout.script_pubkey.clone(), height),
        ).or_insert(bitcoin::SignedAmount::ZERO) += txout.value.try_into()?;
        self.unspent_txos.insert(
          OutPoint::new(tx.compute_txid(), txo_index as u32),
          txout.clone(),
        );
      }
    }
    Ok(())
  }
}
