use std::collections::{BTreeMap, BTreeSet};
use bitcoin::{OutPoint, ScriptBuf};

use crate::store::{self, account::{AccountStoreRead, AccountStoreWrite}, block::{BlockStoreRead, BlockStoreWrite}, BlockHeight, script::{ScriptID, ScriptStoreRead, ScriptStoreWrite}, txo::{TXOStoreRead as _, TXOStoreWrite as _, UTXO}};
use super::batch;


pub struct Layer<'a, 'b> {
  store: &'a mut store::WriteTx<'b>,

  start_height: BlockHeight,
  end_height: BlockHeight,
  blocks: Vec<bitcoin::BlockHash>,

  unspent_txos: BTreeMap<OutPoint, UTXO>,
  spent_txos: BTreeMap<OutPoint, BlockHeight>,
  account_histories: BTreeMap<(ScriptID, BlockHeight), bitcoin::Amount>,
}

impl<'a, 'b> Layer<'a, 'b> {
  pub fn build(
    store: &'a mut store::WriteTx<'b>,
    batch: batch::Batch,
  ) -> anyhow::Result<Self> {
    let mut layer = Layer {
      store,
      start_height: batch.start_height,
      end_height: batch.end_height,
      blocks: batch.blocks,
      unspent_txos: BTreeMap::new(),
      account_histories: BTreeMap::new(),
      spent_txos: batch.spent_txos,
    };

    match layer.store.get_tip_block()? {
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
    let mut use_script_id = |script: &ScriptBuf| -> anyhow::Result<ScriptID> {
      if let Some(id) = script_id_cache.get(script) {
        return Ok(*id);
      }
      let id = layer.store.use_script_id(script)?;
      script_id_cache.insert(script.clone(), id);
      Ok(id)
    };

    let mut account_changes = BTreeMap::new();
    for ((script, height), change) in batch.intermediate_account_changes {
      let script_id = use_script_id(&script)?; 
      *account_changes.entry((script_id, height))
        .or_insert(bitcoin::SignedAmount::ZERO) += change;
    }

    for (outpoint, utxo) in batch.unspent_txos {
      let locker_script_id = use_script_id(&utxo.script_pubkey)?;
      layer.unspent_txos.insert(
        outpoint.clone(),
        UTXO {
          locker_script_id,
          value: utxo.value,
        },
      );
    }

    for (outpoint, spent_height) in &layer.spent_txos {
      let Some(txo) = layer.store.get_utxo(outpoint)? else {
        anyhow::bail!("Attempting to spend non-existent (or spent) TXO {}", outpoint);
      };
      *account_changes.entry((txo.locker_script_id, *spent_height))
        .or_insert(bitcoin::SignedAmount::ZERO)
        -= txo.value.try_into()?;
    }

    let account_script_ids = account_changes.keys().map(|(script_id, _)| *script_id).collect::<BTreeSet<_>>();
    let mut account_states = BTreeMap::new();
    for script_id in account_script_ids {
      let current_balance = layer.store.get_recent_balance(script_id)?;
      account_states.insert(script_id, current_balance);
    }

    for ((script_id, height), change) in account_changes {
      let state = account_states.entry(script_id).or_insert(bitcoin::Amount::ZERO);
      *state = {
        let mut signed = bitcoin::SignedAmount::try_from(*state)?;
        signed += change;
        signed.try_into().map_err(
          |_| anyhow::format_err!("Negative balance for script {:?} at height {}", layer.store.get_script(script_id), height)
        )?
      };
      layer.account_histories.insert((script_id, height), state.clone());
    }

    Ok(layer)
  }

  pub fn write(self) -> anyhow::Result<()> {
    for (i, block_hash) in self.blocks.iter().enumerate() {
      let block_height = self.start_height + i as BlockHeight;
      self.store.insert_block(&block_hash, block_height);
    }

    for (outpoint, _) in self.spent_txos {
      self.store.remove_utxo(&outpoint);
    }

    for (outpoint, txo) in self.unspent_txos {
      self.store.insert_utxo(&outpoint, txo);
    }

    for ((script_id, height), balance) in self.account_histories {
      self.store.insert_historical_balance(script_id, height, balance);
    }

    Ok(())
  }
}