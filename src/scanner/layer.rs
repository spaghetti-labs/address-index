use std::collections::{btree_map, BTreeMap, HashMap};
use bitcoin::{OutPoint, ScriptBuf};
use tracing::instrument;

use crate::{hash::{CollidingOutPoint, CollidingScriptBuf, XORHashBuilder}, store::{self, account::{AccountStoreRead, AccountStoreWrite}, block::{BlockStoreRead, BlockStoreWrite}, script::{ScriptID, ScriptStoreRead, ScriptStoreWrite}, txo::{TXOStoreRead as _, TXOStoreWrite as _, UTXO}, BlockHeight}};
use super::batch;


pub struct Layer<'a, 'b> {
  store: &'a mut store::Batch<'b>,

  start_height: BlockHeight,
  end_height: BlockHeight,
  blocks: Vec<bitcoin::BlockHash>,

  unspent_txos: HashMap<CollidingOutPoint, UTXO, XORHashBuilder>,
  spent_txos: HashMap<CollidingOutPoint, BlockHeight, XORHashBuilder>,
  account_histories: HashMap<ScriptID, BTreeMap<BlockHeight, bitcoin::Amount>, XORHashBuilder>,
  account_states: HashMap<ScriptID, bitcoin::Amount, XORHashBuilder>,
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
      unspent_txos: HashMap::with_capacity_and_hasher(batch.unspent_txos.len(), XORHashBuilder::new()), // will grow un-likely
      account_histories: HashMap::with_capacity_and_hasher(batch.intermediate_account_changes.len(), XORHashBuilder::new()), // will grow likely
      spent_txos: batch.spent_txos,
      account_states: HashMap::with_capacity_and_hasher(batch.intermediate_account_changes.len(), XORHashBuilder::new()), // will grow likely
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

    let mut script_id_cache = BTreeMap::new();
    tracing::trace_span!("Layer::build::script_id_cache").in_scope(|| -> anyhow::Result<()> {
      for (script, _) in &batch.intermediate_account_changes {
        let btree_map::Entry::Vacant(entry) = script_id_cache.entry(script.clone()) else {
          continue;
        };
        entry.insert(layer.store.use_script_id(script.as_ref())?);
      }
      for (_, utxo) in &batch.unspent_txos {
        let btree_map::Entry::Vacant(entry) = script_id_cache.entry(utxo.script_pubkey.clone().into()) else {
          continue;
        };
        entry.insert(layer.store.use_script_id(&utxo.script_pubkey)?);
      }
      Ok(())
    })?;
    let use_script_id = |script: &CollidingScriptBuf| -> anyhow::Result<ScriptID> {
      let Some(id) = script_id_cache.get(script).cloned() else {
        anyhow::bail!("ScriptID cache miss");
      };
      Ok(id)
    };


    let mut account_changes = BTreeMap::new();
    tracing::trace_span!("Layer::build::account_changes").in_scope(|| -> anyhow::Result<()> {
      for (script, changes) in batch.intermediate_account_changes {
        let script_id = use_script_id(&script)?;
        let account_changes = account_changes.entry(script_id).or_insert_with(|| BTreeMap::new());
        for (height, change) in changes {
          *account_changes.entry(height)
            .or_insert(bitcoin::SignedAmount::ZERO) += change;
        }
      }
      Ok(())
    })?;

    tracing::trace_span!("Layer::build::unspent_txos").in_scope(|| -> anyhow::Result<()> {
      for (outpoint, utxo) in batch.unspent_txos {
        let locker_script_id = use_script_id(&utxo.script_pubkey.into())?;
        layer.unspent_txos.insert(
          outpoint.clone(),
          UTXO {
            locker_script_id,
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
          .entry(txo.locker_script_id).or_insert_with(|| BTreeMap::new())
          .entry(*spent_height).or_insert(bitcoin::SignedAmount::ZERO)
          -= txo.value.try_into()?;
      }
      Ok(())
    })?;

    tracing::trace_span!("Layer::build::account_states").in_scope(|| -> anyhow::Result<()> {
      for script_id in account_changes.keys().copied() {
        let current_balance = layer.store.store.get_recent_balance(script_id)?;
        layer.account_states.insert(script_id, current_balance);
      }
      Ok(())
    })?;

    tracing::trace_span!("Layer::build::account_histories").in_scope(|| -> anyhow::Result<()> {
      for (script_id, changes) in account_changes {
        let account_histories = layer.account_histories.entry(script_id).or_insert_with(|| BTreeMap::new());
        let state = layer.account_states.entry(script_id).or_insert(bitcoin::Amount::ZERO);
        for (height, change) in changes {
          *state = {
            let mut signed = bitcoin::SignedAmount::try_from(*state)?;
            signed += change;
            signed.try_into().map_err(
              |_| anyhow::format_err!("Negative balance for script {:?} at height {}", layer.store.store.get_script(script_id), height)
            )?
          };
          account_histories.insert(height, state.clone());
        }
      }
      Ok(())
    })?;

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

    for (script_id, history) in self.account_histories {
      for (height, balance) in history {
        self.store.insert_historical_balance(script_id, height, balance);
      }
    }

    for (script_id, balance) in self.account_states {
      self.store.insert_recent_balance(script_id, balance);
    }

    Ok(())
  }
}
