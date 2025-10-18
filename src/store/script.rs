use fjall::Slice;

use crate::{impl_bincode_conversion, store::{common::{BlockHeight, ScriptID}, ReadTx, TxRead, WriteTx}};
use super::{common::{Amount, CompressedPubKey, PubKeyHash, Script, TransactionID, UncompressedPubKey}};

pub trait TXOStoreRead {
  fn get_script(&self, id: ScriptID) -> anyhow::Result<Option<Script>>;
  fn get_script_id(&self, script: &Script) -> anyhow::Result<Option<ScriptID>>;
}

pub trait TXOStoreWrite {
  fn add_script(&mut self, script: &Script) -> anyhow::Result<ScriptID>;
}

impl<T: TxRead> TXOStoreRead for T {
  fn get_script(&self, id: ScriptID) -> anyhow::Result<Option<Script>> {
    Ok(self.get(&self.store().script_id_to_script, Slice::from(id))?.map(Into::into))
  }

  fn get_script_id(&self, script: &Script) -> anyhow::Result<Option<ScriptID>> {
    Ok(self.get(&self.store().script_to_script_id, Slice::from(script))?.map(Into::into))
  }
}

impl TXOStoreWrite for WriteTx<'_> {
  fn add_script(&mut self, script: &Script) -> anyhow::Result<ScriptID> {
    if let Some(id) = self.get(&self.store().script_to_script_id, Slice::from(script))? {
      return Ok(id.into());
    }

    let next_id: ScriptID = match self.store().script_id_to_script.last_key_value()? {
      Some((key, _)) => {
        let id: ScriptID = (&key).into();
        id.id + 1
      }
      None => 1,
    }.into();
    self.tx.insert(&self.store.script_to_script_id, script, next_id);
    self.tx.insert(&self.store.script_id_to_script, next_id, script);
    Ok(next_id)
  }
}

#[derive(Debug, bincode::Encode, bincode::Decode, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct TXOID {
  pub txid: TransactionID,
  pub vout: u32,
}
impl_bincode_conversion!(TXOID);

#[derive(Debug, bincode::Encode, bincode::Decode, Clone)]
pub struct TXO {
  pub locker_script: Script,
  pub value: Amount,
}
impl_bincode_conversion!(TXO);

#[derive(Debug, bincode::Encode, bincode::Decode, Copy, Clone)]
pub struct HeightAndTXOID {
  pub height: BlockHeight,
  pub txoid: TXOID,
}
impl_bincode_conversion!(HeightAndTXOID);
