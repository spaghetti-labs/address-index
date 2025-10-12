use fjall::Slice;

use crate::impl_bincode_conversion;
use super::Batch;

#[derive(Debug, bincode::Encode, bincode::Decode, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct TXOID {
  pub txid: super::common::TransactionID,
  pub vout: u32,
}
impl_bincode_conversion!(TXOID);

#[derive(Debug, bincode::Encode, bincode::Decode, Clone)]
pub struct TXO {
  pub id: TXOID,
  pub value: super::common::Amount,
  pub address: Option<String>,
}
impl_bincode_conversion!(TXO);

pub struct TXOStore {
  pub(super) partition: fjall::Partition,
}

impl TXOStore {
  pub fn insert_txo(&self, txo: &TXO, batch: &mut Batch) {
    batch.batch.insert(
      &self.partition,
      &txo.id,
      txo,
    );
  }

  pub fn get_txo(&self, id: &TXOID) -> anyhow::Result<Option<TXO>> {
    let result: Option<fjall::Slice> = self.partition.get(Slice::from(id))?;
    Ok(result.map(|slice| slice.into()))
  }
}
