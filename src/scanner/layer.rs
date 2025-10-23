use std::collections::{btree_map, BTreeMap, HashMap};
use bitcoin::{ScriptBuf, ScriptHash};
use tracing::instrument;

use crate::{hash::{CollidingOutPoint, CollidingScriptHash, LazyHasherBuilder}, store::{self, account::{AccountStoreRead, AccountStoreWrite}, block::{BlockStoreRead, BlockStoreWrite}, txo::{TXOStoreRead as _, TXOStoreWrite as _, UTXO}, BlockHeight}};
use super::batch;


pub struct Layer<'a, 'b> {
  store: &'a mut store::Batch<'b>,

  start_height: BlockHeight,
  end_height: BlockHeight,
  blocks: Vec<bitcoin::BlockHash>,

  unspent_txos: HashMap<CollidingOutPoint, UTXO, LazyHasherBuilder>,
  spent_txos: HashMap<CollidingOutPoint, BlockHeight, LazyHasherBuilder>,
  account_histories: HashMap<CollidingScriptHash, BTreeMap<BlockHeight, bitcoin::Amount>, LazyHasherBuilder>,
  account_states: HashMap<CollidingScriptHash, bitcoin::Amount, LazyHasherBuilder>,
}

impl<'a, 'b> Layer<'a, 'b> {
  #[instrument(name="Layer::build", level="trace", skip_all)]
  pub fn build(
    store: &'a mut store::Batch<'b>,
    batch: batch::Batch,
  ) -> anyhow::Result<Self> {
    let mut layer = Layer {
      store,
      start_height: batch.start_height,
      end_height: batch.end_height,
      blocks: batch.blocks,
      unspent_txos: HashMap::with_capacity_and_hasher(batch.unspent_txos.len(), LazyHasherBuilder::new()), // will grow un-likely
      account_histories: HashMap::with_capacity_and_hasher(batch.intermediate_account_changes.len(), LazyHasherBuilder::new()), // will grow likely
      spent_txos: batch.spent_txos,
      account_states: HashMap::with_capacity_and_hasher(batch.intermediate_account_changes.len(), LazyHasherBuilder::new()), // will grow likely
    };

    match layer.store.store.get_tip_block()? {
      Some((tip_height, _)) if tip_height + 1 != layer.start_height => {
        anyhow::bail!(
          "Batch start height {} does not follow store tip height {}",
          layer.start_height,
          tip_height,
        );
      }
      None if layer.start_height != 0 => {
        anyhow::bail!(
          "Batch start height {} is invalid for empty store",
          layer.start_height,
        );
      }
      _ => {}
    }

    let mut account_changes = BTreeMap::new();
    tracing::trace_span!("Layer::build::account_changes").in_scope(|| -> anyhow::Result<()> {
      for (script_hash, changes) in batch.intermediate_account_changes {
        let account_changes = account_changes.entry(script_hash).or_insert_with(|| BTreeMap::new());
        for (height, change) in changes {
          *account_changes.entry(height)
            .or_insert(bitcoin::SignedAmount::ZERO) += change;
        }
      }
      Ok(())
    })?;

    tracing::trace_span!("Layer::build::unspent_txos").in_scope(|| -> anyhow::Result<()> {
      for (outpoint, utxo) in batch.unspent_txos {
        let locker_script_hash = utxo.script_pubkey.script_hash();
        layer.unspent_txos.insert(
          outpoint.clone(),
          UTXO {
            locker_script_hash,
            value: utxo.value,
          },
        );
      }
      Ok(())
    })?;

    tracing::trace_span!("Layer::build::spent_txos").in_scope(|| -> anyhow::Result<()> {
      for (outpoint, spent_height) in &layer.spent_txos {
        let Some(txo) = layer.store.store.get_utxo(outpoint.as_ref())? else {
          anyhow::bail!("Attempting to spend non-existent (or spent) TXO {}", outpoint.as_ref());
        };
        *account_changes
          .entry(txo.locker_script_hash.into()).or_insert_with(|| BTreeMap::new())
          .entry(*spent_height).or_insert(bitcoin::SignedAmount::ZERO)
          -= txo.value.try_into()?;
      }
      Ok(())
    })?;

    tracing::trace_span!("Layer::build::account_states").in_scope(|| -> anyhow::Result<()> {
      for script_hash in account_changes.keys().copied() {
        let current_balance = layer.store.store.get_recent_balance(&script_hash.into())?;
        layer.account_states.insert(script_hash, current_balance);
      }
      Ok(())
    })?;

    tracing::trace_span!("Layer::build::account_histories").in_scope(|| -> anyhow::Result<()> {
      for (script_hash, changes) in account_changes {
        let account_histories = layer.account_histories.entry(script_hash).or_insert_with(|| BTreeMap::new());
        let state = layer.account_states.entry(script_hash).or_insert(bitcoin::Amount::ZERO);
        for (height, change) in changes {
          *state = {
            let mut signed = bitcoin::SignedAmount::try_from(*state)?;
            signed += change;
            signed.try_into().map_err(
              |_| anyhow::format_err!("Negative balance for script hash {:?} at height {}", Into::<ScriptHash>::into(script_hash), height)
            )?
          };
          account_histories.insert(height, state.clone());
        }
      }
      Ok(())
    })?;

    tracing::Span::current().record("num_blocks", layer.blocks.len());
    tracing::Span::current().record("num_unspent_txos", layer.unspent_txos.len());
    tracing::Span::current().record("num_spent_txos", layer.spent_txos.len());
    tracing::Span::current().record("num_account_histories", layer.account_histories.len());

    Ok(layer)
  }

  #[instrument(name="Layer::write", level="trace", skip_all)]
  pub fn write(self) -> anyhow::Result<()> {
    for (i, block_hash) in self.blocks.iter().enumerate() {
      let block_height = self.start_height + i as BlockHeight;
      self.store.insert_block(&block_hash, block_height);
    }

    for (outpoint, _) in self.spent_txos {
      self.store.remove_utxo(outpoint.as_ref());
    }

    for (outpoint, txo) in self.unspent_txos {
      self.store.insert_utxo(outpoint.as_ref(), txo);
    }

    for (script_hash, history) in self.account_histories {
      for (height, balance) in history {
        self.store.insert_historical_balance(&script_hash.into(), height, balance);
      }
    }

    for (script_hash, balance) in self.account_states {
      self.store.insert_recent_balance(&script_hash.into(), balance);
    }

    Ok(())
  }
}
