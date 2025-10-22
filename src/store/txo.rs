use binary_layout::prelude::*;
use bitcoin::{hashes::Hash, Amount, OutPoint};

use crate::store::{BlockHeight, TxRead, WriteTx, script::ScriptID};

pub trait TXOStoreRead {
  fn get_utxo(&self, outpoint: &OutPoint) -> anyhow::Result<Option<UTXO>>;
}

pub trait TXOStoreWrite {
  fn insert_utxo(&mut self, outpoint: &OutPoint, utxo: UTXO);
  fn remove_utxo(&mut self, outpoint: &OutPoint);
}

impl<T: TxRead> TXOStoreRead for T {
  fn get_utxo(&self, outpoint: &OutPoint) -> anyhow::Result<Option<UTXO>> {
    let mut key = txo_id::View::new([0u8; txo_id::SIZE.unwrap()]);
    *key.transaction_id_mut() = outpoint.txid.as_raw_hash().to_byte_array();
    key.output_index_mut().write(outpoint.vout);

    let Some(utxo) = self.get(&self.store().txoid_to_utxo, key.into_storage())? else {
      return Ok(None);
    };
    let utxo = txo::View::new(utxo);
    Ok(Some(UTXO {
      locker_script_id: utxo.locker_script_id().try_read()?,
      value: Amount::from_sat(utxo.value().try_read()?),
    }))
  }
}

impl TXOStoreWrite for WriteTx<'_> {
  fn insert_utxo(&mut self, txoid: &OutPoint, utxo: UTXO) {
    let mut key = txo_id::View::new([0u8; txo_id::SIZE.unwrap()]);
    *key.transaction_id_mut() = txoid.txid.as_raw_hash().to_byte_array();
    key.output_index_mut().write(txoid.vout);

    let mut value = txo::View::new([0u8; txo::SIZE.unwrap()]);
    value.locker_script_id_mut().write(utxo.locker_script_id);
    value.value_mut().write(utxo.value.to_sat());

    self.tx.insert(&self.store.txoid_to_utxo, key.into_storage(), value.into_storage());
  }

  fn remove_utxo(&mut self, txoid: &OutPoint) {
    let mut key = txo_id::View::new([0u8; txo_id::SIZE.unwrap()]);
    *key.transaction_id_mut() = txoid.txid.as_raw_hash().to_byte_array();
    key.output_index_mut().write(txoid.vout);

    self.tx.remove(&self.store.txoid_to_utxo, key.into_storage());
  }
}

#[derive(Clone, Copy)]
pub struct UTXO {
  pub locker_script_id: ScriptID,
  pub value: Amount,
}

binary_layout!(txo_id, BigEndian, {
  transaction_id: [u8; 32],
  output_index: u32,
});

binary_layout!(txo, BigEndian, {
  locker_script_id: ScriptID,
  value: u64
});

binary_layout!(height_and_txo_id, BigEndian, {
  height: BlockHeight,
  txoid: txo_id::NestedView,
});
