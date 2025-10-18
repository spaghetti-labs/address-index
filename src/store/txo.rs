use fjall::Slice;

use crate::{impl_bincode_conversion, store::{common::{BlockHeight, ScriptID}, TxRead, WriteTx}};
use super::common::{Amount, TransactionID};

pub trait TXOStoreRead {
  fn get_txo(&self, id: &TXOID) -> anyhow::Result<Option<TXO>>;
}

pub trait TXOStoreWrite {
  fn insert_txo(&mut self, height: BlockHeight, txoid: &TXOID, txo: TXO);
}

impl<T: TxRead> TXOStoreRead for T {
  fn get_txo(&self, id: &TXOID) -> anyhow::Result<Option<TXO>> {
    Ok(self.get(&self.store().txoid_to_txo, Slice::from(id))?.map(Into::into))
  }
}

impl TXOStoreWrite for WriteTx<'_> {
  fn insert_txo(&mut self, height: BlockHeight, txoid: &TXOID, txo: TXO) {
    self.tx.insert(&self.store.txoid_to_txo, txoid, txo);
    self.tx.insert(&self.store.height_and_txoid, &HeightAndTXOID{ height, txoid: *txoid }, []);
  }
}

#[derive(Debug, bincode::Encode, bincode::Decode, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct TXOID {
  pub txid: TransactionID,
  pub vout: u32,
}
impl_bincode_conversion!(TXOID);

#[derive(Debug, bincode::Encode, bincode::Decode, Copy, Clone)]
pub struct TXO {
  pub locker_script_id: ScriptID,
  pub value: Amount,
}
impl_bincode_conversion!(TXO);

#[derive(Debug, bincode::Encode, bincode::Decode, Copy, Clone)]
pub struct HeightAndTXOID {
  pub height: BlockHeight,
  pub txoid: TXOID,
}
impl_bincode_conversion!(HeightAndTXOID);
