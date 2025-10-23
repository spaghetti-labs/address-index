use std::collections::{BTreeMap, HashMap};
use bitcoin::{OutPoint, ScriptBuf};
use tracing::instrument;

use crate::{hash::{CollidingOutPoint, CollidingScriptBuf, XORHashBuilder}, store::BlockHeight};

pub struct Batch {
  pub(crate) start_height: BlockHeight,
  pub(crate) end_height: BlockHeight,
  pub(crate) blocks: Vec<bitcoin::BlockHash>,

  pub(crate) unspent_txos: HashMap<CollidingOutPoint, bitcoin::TxOut, XORHashBuilder>,
  pub(crate) spent_txos: HashMap<CollidingOutPoint, BlockHeight, XORHashBuilder>,
  pub(crate) intermediate_account_changes: HashMap<CollidingScriptBuf, BTreeMap<BlockHeight, bitcoin::SignedAmount>, XORHashBuilder>,
}

impl Batch {
  #[instrument(name = "Batch::build", level="trace", skip_all)]
  pub fn build(
    start_height: BlockHeight,
    blocks: &[bitcoin::Block],
  ) -> anyhow::Result<Self> {
    let mut batch = Batch {
      start_height,
      end_height: start_height + blocks.len() as BlockHeight,
      blocks: Vec::with_capacity(blocks.len()),
      unspent_txos: HashMap::with_hasher(XORHashBuilder::new()),
      intermediate_account_changes: HashMap::with_hasher(XORHashBuilder::new()),
      spent_txos: HashMap::with_hasher(XORHashBuilder::new()),
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
        if let Some(utxo) = self.unspent_txos.remove(&txin.previous_output.into()) {
          *self.intermediate_account_changes.entry(
            utxo.script_pubkey.into(),
          ).or_insert_with(|| BTreeMap::new()).entry(height).or_insert(bitcoin::SignedAmount::ZERO) -= utxo.value.try_into()?;
        } else {
          self.spent_txos.insert(
            txin.previous_output.into(),
            height,
          );
        }
      }
      for (txo_index, txout) in tx.output.iter().enumerate() {
        *self.intermediate_account_changes.entry(
          txout.script_pubkey.clone().into(),
        ).or_insert_with(|| BTreeMap::new()).entry(height).or_insert(bitcoin::SignedAmount::ZERO) += txout.value.try_into()?;
        self.unspent_txos.insert(
          OutPoint::new(tx.compute_txid(), txo_index as u32).into(),
          txout.clone(),
        );
      }
    }
    Ok(())
  }
}
