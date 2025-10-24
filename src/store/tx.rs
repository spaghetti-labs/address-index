use std::collections::BTreeMap;

use bincode::{de::Decoder, enc::Encoder, error::{DecodeError, EncodeError}, Decode, Encode};
use bitcoin::{hashes::Hash, Amount, ScriptHash, Txid};

use crate::store::{Batch, Store};

#[derive(Clone)]
pub struct TXO {
  pub locker_script_hash: ScriptHash,
  pub value: Amount,
}

impl Encode for TXO {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.locker_script_hash.to_byte_array().encode(encoder)?;
    self.value.to_sat().encode(encoder)?;
    Ok(())
  }
}

impl<Context> Decode<Context> for TXO {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    Ok(Self {
      locker_script_hash: ScriptHash::from_byte_array(
      <[u8; ScriptHash::LEN]>::decode(decoder)?,
    ),
      value: Amount::from_sat(u64::decode(decoder)?),
    })
  }
}

#[derive(Clone)]
pub struct TxState {
  pub unspent_outputs: BTreeMap<u32, TXO>,
}

impl Encode for TxState {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.unspent_outputs.len().encode(encoder)?;
    for (index, txo) in &self.unspent_outputs {
      index.encode(encoder)?;
      txo.encode(encoder)?;
    }
    Ok(())
  }
}

impl<Context> Decode<Context> for TxState {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    let len = usize::decode(decoder)?;
    let mut unspent_outputs = BTreeMap::new();
    for _ in 0..len {
      let index = u32::decode(decoder)?;
      let txo = TXO::decode(decoder)?;
      unspent_outputs.insert(index, txo);
    }
    Ok(Self { unspent_outputs })
  }
}

impl TxState {
  pub fn unspent(
    unspent_outputs: BTreeMap<u32, TXO>,
  ) -> Self {
    Self {
      unspent_outputs,
    }
  }

  pub fn is_empty(&self) -> bool {
    self.unspent_outputs.is_empty()
  }
}

pub trait TxStoreRead {
  fn get_tx_state(&self, txid: &Txid) -> anyhow::Result<Option<TxState>>;
}

pub trait TxStoreWrite {
  fn set_tx_state(&mut self, txid: &Txid, state: &TxState);
}

impl TxStoreRead for Store {
  fn get_tx_state(&self, txid: &Txid) -> anyhow::Result<Option<TxState>> {
    let Some(state) = self.txid_to_tx_state.get(txid.as_byte_array())? else {
      return Ok(None);
    };
    Ok(Some(bincode::decode_from_slice(&state, bincode::config::standard())?.0))
  }
}

impl TxStoreWrite for Batch<'_> {
  fn set_tx_state(&mut self, txid: &Txid, state: &TxState) {
    let encoded = bincode::encode_to_vec(state, bincode::config::standard()).unwrap();
    if encoded.is_empty() {
      self.batch.remove(&self.store.txid_to_tx_state, txid.as_byte_array());
      return;
    }
    self.batch.insert(&self.store.txid_to_tx_state, txid.as_byte_array(), encoded);
  }
}
