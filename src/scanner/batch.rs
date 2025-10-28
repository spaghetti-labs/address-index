use bitcoin::OutPoint;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator as _};
use tracing::instrument;

use crate::store::{self, block::{BlockStoreRead as _, BlockStoreWrite as _}, txo::{TXOGenerated, TXOSpent, TXOStoreWrite}, BlockHeight};

pub struct Batch {
  pub(crate) start_height: BlockHeight,
  pub(crate) end_height: BlockHeight,
  pub(crate) blocks: Vec<bitcoin::BlockHash>,

  pub(crate) generated_txos: Vec<(OutPoint, TXOGenerated)>,
  pub(crate) spent_txos: Vec<(OutPoint, TXOSpent)>,
}

impl Batch {
  #[instrument(name = "Batch::build", level="trace", skip_all, fields(
    start_height = start_height,
    num_blocks = tracing::field::Empty,
    num_generated_txos = tracing::field::Empty,
    num_spent_txos = tracing::field::Empty,
    num_txs = tracing::field::Empty,
    bytes_total_size = tracing::field::Empty,
  ))]
  pub fn build(
    start_height: BlockHeight,
    blocks: Vec<bitcoin::Block>,
  ) -> anyhow::Result<Self> {
    let mut batch = Batch {
      start_height,
      end_height: start_height + blocks.len() as BlockHeight,
      blocks: Vec::with_capacity(blocks.len()),
      generated_txos: Vec::new(),
      spent_txos: Vec::new(),
    };
    for (i, block) in blocks.iter().enumerate() {
      let height = start_height + i as BlockHeight;
      batch.scan_block(height, block)?;
    }

    tracing::Span::current().record("num_blocks", batch.blocks.len());
    tracing::Span::current().record("num_generated_txos", batch.generated_txos.len());
    tracing::Span::current().record("num_spent_txos", batch.spent_txos.len());
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

        self.spent_txos.push((txin.previous_output, TXOSpent { spent_height: height }));
      }

      let txid = tx.compute_txid();

      for (index, txout) in tx.output.iter().enumerate() {
        let outpoint = OutPoint {
          txid,
          vout: index as u32,
        };
        let locker_script_hash = txout.script_pubkey.script_hash();

        self.generated_txos.push((
          outpoint,
          TXOGenerated {
            locker_script_hash,
            value: txout.value,
            generated_height: height,
          },
        ));
      }
    }
    Ok(())
  }

  pub fn write(self, store: &mut store::Batch) -> anyhow::Result<()> {
    match store.store.get_tip_block()? {
      Some((tip_height, _)) if tip_height + 1 != self.start_height => {
        anyhow::bail!(
          "Batch start height {} does not follow store tip height {}",
          self.start_height,
          tip_height,
        );
      }
      None if self.start_height != 0 => {
        anyhow::bail!(
          "Batch start height {} is invalid for empty store",
          self.start_height,
        );
      }
      _ => {}
    }

    store.insert_blocks(self.blocks.iter().enumerate().map(|(i, block_hash)| {
      let block_height = self.start_height + i as BlockHeight;
      (block_hash, block_height)
    }));

    store.generated_txos(self.generated_txos.par_iter().map(|(outpoint, txo)| (outpoint, txo)));

    store.spent_txos(self.spent_txos.par_iter().map(|(outpoint, txo)| (outpoint, txo)));

    Ok(())
  }
}
