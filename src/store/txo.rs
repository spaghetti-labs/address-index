use fjall::Slice;

use crate::{impl_bincode_conversion, store::{common::{BlockHeight, ScriptID}, ReadTx, TxRead, WriteTx}};
use super::{common::{Amount, CompressedPubKey, PubKeyHash, Script, TransactionID, UncompressedPubKey}};

pub trait TXOStoreRead {
  fn get_txo(&self, id: &TXOID) -> anyhow::Result<Option<TXO>>;
  fn scan_txoids_by_height(&self, height: BlockHeight) -> impl Iterator<Item = anyhow::Result<TXOID>> + '_;
}

pub trait TXOStoreWrite {
  fn insert_txo(&mut self, height: BlockHeight, txoid: &TXOID, txo: TXO);
}

impl<T: TxRead> TXOStoreRead for T {
  fn get_txo(&self, id: &TXOID) -> anyhow::Result<Option<TXO>> {
    Ok(self.get(&self.store().txoid_to_txo, Slice::from(id))?.map(Into::into))
  }

  fn scan_txoids_by_height(&self, height: BlockHeight) -> impl Iterator<Item = anyhow::Result<TXOID>> + '_
  {
    self.prefix(&self.store().height_and_txoid, Slice::from(height)).map(|entry| {
      let (key, _) = entry?;
      Ok(key.into())
    })
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
