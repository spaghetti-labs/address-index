use std::collections::{BTreeMap, HashMap};
use tracing::instrument;

use crate::{hash::{CollidingScriptHash, CollidingTxid, LazyHasherBuilder}, store::{tx::{TxState, TXO}, BlockHeight}};

pub struct Batch {
  pub(crate) start_height: BlockHeight,
  pub(crate) end_height: BlockHeight,
  pub(crate) blocks: Vec<bitcoin::BlockHash>,

  pub(crate) new_tx_states: HashMap<CollidingTxid, TxState, LazyHasherBuilder>,
  pub(crate) spent_txos: HashMap<CollidingTxid, BTreeMap<u32, BlockHeight>, LazyHasherBuilder>,
  pub(crate) intermediate_account_changes: HashMap<CollidingScriptHash, BTreeMap<BlockHeight, bitcoin::SignedAmount>, LazyHasherBuilder>,
}

impl Batch {
  #[instrument(name = "Batch::build", level="trace", skip_all, fields(
    start_height = start_height,
    num_blocks = tracing::field::Empty,
    num_new_tx_states = tracing::field::Empty,
    num_spent_txos = tracing::field::Empty,
    num_intermediate_account_changes = tracing::field::Empty,
    num_txs = tracing::field::Empty,
    bytes_total_size = tracing::field::Empty,
  ))]
  pub fn build(
    start_height: BlockHeight,
    blocks: &[bitcoin::Block],
  ) -> anyhow::Result<Self> {
    let mut batch = Batch {
      start_height,
      end_height: start_height + blocks.len() as BlockHeight,
      blocks: Vec::with_capacity(blocks.len()),
      new_tx_states: HashMap::with_hasher(LazyHasherBuilder::new()),
      intermediate_account_changes: HashMap::with_hasher(LazyHasherBuilder::new()),
      spent_txos: HashMap::with_hasher(LazyHasherBuilder::new()),
    };
    for (i, block) in blocks.iter().enumerate() {
      let height = start_height + i as BlockHeight;
      batch.scan_block(height, block)?;
    }

    batch.new_tx_states.retain(|_, tx_state| !tx_state.is_empty());

    tracing::Span::current().record("num_blocks", batch.blocks.len());
    tracing::Span::current().record("num_new_tx_states", batch.new_tx_states.len());
    tracing::Span::current().record("num_spent_txos", batch.spent_txos.len());
    tracing::Span::current().record("num_intermediate_account_changes", batch.intermediate_account_changes.len());
    tracing::Span::current().record("num_txs", blocks.iter().map(|b| b.txdata.len()).sum::<usize>());
    tracing::Span::current().record("bytes_total_size", blocks.iter().map(|b| b.total_size()).sum::<usize>());

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

        let utxo = self.new_tx_states
          .get_mut(&txin.previous_output.txid.into())
          .and_then(|m| m.unspent_outputs.remove(&txin.previous_output.vout));

        if let Some(utxo) = utxo {
          *self.intermediate_account_changes.entry(
            utxo.locker_script_hash.into(),
          ).or_insert_with(|| BTreeMap::new()).entry(height).or_insert(bitcoin::SignedAmount::ZERO) -= utxo.value.try_into()?;
        } else {
          self.spent_txos.entry(txin.previous_output.txid.into())
            .or_insert_with(|| BTreeMap::new())
            .insert(txin.previous_output.vout, height);
        }
      }

      let txid = tx.compute_txid();

      let mut unspent_txos = BTreeMap::new();
      for (txo_index, txout) in tx.output.iter().enumerate() {
        *self.intermediate_account_changes.entry(
          txout.script_pubkey.script_hash().into(),
        ).or_insert_with(|| BTreeMap::new()).entry(height).or_insert(bitcoin::SignedAmount::ZERO) += txout.value.try_into()?;
        unspent_txos.insert(txo_index as u32, TXO {
          locker_script_hash: txout.script_pubkey.script_hash(),
          value: txout.value,
        });
      }

      if self.new_tx_states.insert(txid.into(), TxState::unspent(unspent_txos)).is_some() {
        // This can be due to Coinbase transactions with the same output script and value before BIP-30
        tracing::warn!("Duplicate TXID detected in block at height {}: {}", height, txid);
      }
    }
    Ok(())
  }
}
