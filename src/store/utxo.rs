use crate::impl_bincode_conversion;
use super::Batch;

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct UTXOID {
  pub txid: super::common::TransactionID,
  pub vout: u32,
}
impl_bincode_conversion!(UTXOID);

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct UTXO {
  pub id: UTXOID,
  pub value: super::common::Amount,
  pub address: String,
}
impl_bincode_conversion!(UTXO);

pub struct UTXOStore {
  pub(super) partition: fjall::Partition,
}

impl UTXOStore {
  pub fn insert_utxo(&self, utxo: &UTXO, batch: &mut Batch) {
    batch.batch.insert(
      &self.partition,
      &utxo.id,
      utxo,
    );
  }
}
