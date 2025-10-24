use std::collections::{hash_map, BTreeMap, HashMap};
use bitcoin::{ScriptHash};
use tracing::instrument;

use crate::{hash::{CollidingScriptHash, CollidingTxid, LazyHasherBuilder}, store::{self, account::{AccountState, AccountStoreRead, AccountStoreWrite}, block::{BlockStoreRead, BlockStoreWrite}, tx::{TxState, TxStoreRead as _, TxStoreWrite as _}, BlockHeight}};
use super::batch;


pub struct Layer<'a, 'b> {
  store: &'a mut store::Batch<'b>,

  start_height: BlockHeight,
  end_height: BlockHeight,
  blocks: Vec<bitcoin::BlockHash>,

  tx_states: HashMap<CollidingTxid, TxState, LazyHasherBuilder>,
  account_states: HashMap<CollidingScriptHash, AccountState, LazyHasherBuilder>,
}

impl<'a, 'b> Layer<'a, 'b> {
  #[instrument(name="Layer::build", level="trace", skip_all)]
  pub fn build(
    store: &'a mut store::Batch<'b>,
    mut batch: batch::Batch,
  ) -> anyhow::Result<Self> {
    match store.store.get_tip_block()? {
      Some((tip_height, _)) if tip_height + 1 != batch.start_height => {
        anyhow::bail!(
          "Batch start height {} does not follow store tip height {}",
          batch.start_height,
          tip_height,
        );
      }
      None if batch.start_height != 0 => {
        anyhow::bail!(
          "Batch start height {} is invalid for empty store",
          batch.start_height,
        );
      }
      _ => {}
    }

    let mut tx_states = batch.new_tx_states;
    tx_states.reserve(batch.spent_txos.len());
    tracing::trace_span!("Layer::build::tx_states").in_scope(|| -> anyhow::Result<()> {
      for (txid, spent_txos) in &batch.spent_txos {
        let hash_map::Entry::Vacant(tx_state) = tx_states.entry(txid.clone()) else {
          anyhow::bail!("Duplicate TXID detected when processing spent TXOs: {}", txid.as_ref());
        };
        let tx_state = tx_state.insert(store.store.get_tx_state(txid.as_ref())?.ok_or(
          anyhow::format_err!("Spent TXO refers to unknown TXID {}", txid.as_ref())
        )?);

        for (spent_vout, spent_height) in spent_txos {
          let Some(txo) = tx_state.unspent_outputs.remove(&spent_vout) else {
            anyhow::bail!("Spent TXO refers to already spent output {}:{}", txid.as_ref(), spent_vout);
          };
          *batch.intermediate_account_changes
            .entry(txo.locker_script_hash.into()).or_insert_with(|| BTreeMap::new())
            .entry(*spent_height).or_insert(bitcoin::SignedAmount::ZERO)
            -= txo.value.try_into()?;
        }
      }
      Ok(())
    })?;

    let mut account_states = HashMap::with_capacity_and_hasher(batch.intermediate_account_changes.len(), LazyHasherBuilder::new());
    tracing::trace_span!("Layer::build::account_states").in_scope(|| -> anyhow::Result<()> {
      for (script_hash, changes) in batch.intermediate_account_changes {
        let hash_map::Entry::Vacant(account_state) = account_states.entry(script_hash) else {
          anyhow::bail!("Duplicate script hash detected when processing intermediate account changes: {}", script_hash.as_ref());
        };
        let account_state = account_state.insert(store.store.get_account_state(script_hash.as_ref())?);

        for (height, change) in changes {
          account_state.recent_balance = {
            let mut signed = bitcoin::SignedAmount::try_from(account_state.recent_balance)?;
            signed += change;
            signed.try_into().map_err(
              |_| anyhow::format_err!("Negative recent balance for script hash {:?}", Into::<ScriptHash>::into(script_hash))
            )?
          };
          account_state.balance_history.insert(height, account_state.recent_balance);
        }
      }
      Ok(())
    })?;

    tracing::Span::current().record("num_blocks", batch.blocks.len());
    tracing::Span::current().record("num_tx_states", tx_states.len());
    tracing::Span::current().record("num_account_states", account_states.len());

    Ok(Layer {
      store,
      start_height: batch.start_height,
      end_height: batch.end_height,
      blocks: batch.blocks,
      tx_states,
      account_states,
    })
  }

  #[instrument(name="Layer::write", level="trace", skip_all)]
  pub fn write(self) -> anyhow::Result<()> {
    for (i, block_hash) in self.blocks.iter().enumerate() {
      let block_height = self.start_height + i as BlockHeight;
      self.store.insert_block(&block_hash, block_height);
    }

    for (txid, tx_state) in self.tx_states {
      self.store.set_tx_state(txid.as_ref(), &tx_state);
    }

    for (script_hash, account_state) in self.account_states {
      self.store.set_account_state(script_hash.as_ref(), &account_state);
    }

    Ok(())
  }
}
