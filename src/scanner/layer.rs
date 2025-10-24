use std::collections::{hash_map, BTreeMap, HashMap};
use bitcoin::{ScriptHash};
use rayon::iter::{IntoParallelIterator, ParallelIterator as _};
use tracing::instrument;

use crate::{hash::{CollidingTxid, LazyHasherBuilder}, store::{self, account::{AccountState, AccountStoreRead, AccountStoreWrite}, block::{BlockStoreRead, BlockStoreWrite}, tx::{TxState, TxStoreRead as _, TxStoreWrite as _}, BlockHeight}};
use super::batch;


pub struct Layer<'a, 'b> {
  store: &'a mut store::Batch<'b>,

  start_height: BlockHeight,
  end_height: BlockHeight,
  blocks: Vec<bitcoin::BlockHash>,

  tx_states: HashMap<CollidingTxid, TxState, LazyHasherBuilder>,
  account_states: Vec<(ScriptHash, AccountState)>,
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

    let spent_txs = tracing::trace_span!("Layer::build::spent_txs", num_spent_txs=tracing::field::Empty).in_scope(|| -> anyhow::Result<_> {
      let spent_txs = batch.spent_txos
        .into_par_iter()
        .map(|(txid, spent_txos)| -> anyhow::Result<_> {
          let mut tx_state = store.store.get_tx_state(txid.as_ref())?.ok_or(
            anyhow::format_err!("Spent TXO refers to unknown TXID {}", txid.as_ref())
          )?;
          let spent_txos = spent_txos.into_iter().map(|(spent_vout, spent_height)| {
            let Some(txo) = tx_state.unspent_outputs.remove(&spent_vout) else {
              anyhow::bail!("Spent TXO refers to already spent output {}:{}", txid.as_ref(), spent_vout);
            };
            Ok((txo, spent_height))
          }).collect::<Result<Vec<_>, _>>()?;
          Ok((txid, tx_state, spent_txos))
        })
        .collect::<Result<Vec<_>, _>>()?;
      tracing::Span::current().record("num_spent_txs", spent_txs.len());
      Ok(spent_txs)
    })?;

    let tx_states = tracing::trace_span!("Layer::build::tx_states", num_tx_states=tracing::field::Empty).in_scope(|| -> anyhow::Result<_> {
      let mut tx_states = batch.new_tx_states;
      tx_states.reserve(spent_txs.len());
      for (txid, tx_state, spent_txos) in spent_txs {
        let hash_map::Entry::Vacant(tx_state_entry) = tx_states.entry(txid.clone()) else {
          anyhow::bail!("Duplicate TXID detected when processing spent TXOs: {}", txid.as_ref());
        };
        tx_state_entry.insert(tx_state);

        for (txo, spent_height) in spent_txos {
          *batch.intermediate_account_changes
            .entry(txo.locker_script_hash.into()).or_insert_with(|| BTreeMap::new())
            .entry(spent_height).or_insert(bitcoin::SignedAmount::ZERO)
            += txo.value.try_into()?;
        }
      }
      tracing::Span::current().record("num_tx_states", tx_states.len());
      Ok(tx_states)
    })?;

    let account_states = tracing::trace_span!("Layer::build::account_states", num_account_states=tracing::field::Empty).in_scope(|| -> anyhow::Result<_> {
      let account_states = batch.intermediate_account_changes
        .into_par_iter()
        .map(|(script_hash, changes)| -> anyhow::Result<_> {
          let mut account_state = store.store.get_account_state(script_hash.as_ref())?;

          for (height, change) in changes {
            account_state.recent_balance = {
              let mut signed = bitcoin::SignedAmount::try_from(account_state.recent_balance)?;
              signed += change;
              signed.try_into().map_err(
                |_| anyhow::format_err!("Negative recent balance for script hash {:?}", script_hash.as_ref())
              )?
            };
            account_state.balance_history.append(
              height,
              account_state.recent_balance,
            );
          }

          Ok((script_hash.into(), account_state))
        })
        .collect::<Result<Vec<_>, _>>()?;
      tracing::Span::current().record("num_account_states", account_states.len());
      Ok(account_states)
    })?;

    Ok(Layer {
      store,
      start_height: batch.start_height,
      end_height: batch.end_height,
      blocks: batch.blocks,
      tx_states,
      account_states,
    })
  }

  #[instrument(name="Layer::write", level="trace", skip_all, fields(
    start_height = self.start_height,
    end_height = self.end_height,
    num_blocks = self.blocks.len(),
    num_tx_states = self.tx_states.len(),
    num_account_states = self.account_states.len(),
  ))]
  pub fn write(self) -> anyhow::Result<()> {
    for (i, block_hash) in self.blocks.iter().enumerate() {
      let block_height = self.start_height + i as BlockHeight;
      self.store.insert_block(&block_hash, block_height);
    }

    for (txid, tx_state) in self.tx_states {
      self.store.set_tx_state(txid.as_ref(), &tx_state);
    }

    for (script_hash, account_state) in self.account_states {
      self.store.set_account_state(&script_hash, &account_state);
    }

    Ok(())
  }
}
