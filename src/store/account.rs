use fjall::Slice;

use crate::{impl_bincode_conversion, store::{common::BlockHeight, ReadTx, TxRead, WriteTx}};
use super::{common::{Amount, CompressedPubKey, PubKeyHash, Script, TransactionID, UncompressedPubKey}};

pub trait AccountStoreRead {
  fn get_recent_balance(&self, script: &Script) -> anyhow::Result<Amount>;
  fn get_historical_balance(&self, script: &Script, height: &BlockHeight) -> anyhow::Result<Amount>;
}

pub trait AccountStoreWrite {
  fn insert_balance(&mut self, locker_script: &Script, height: &BlockHeight, balance: &Amount);
}

impl<T: TxRead> AccountStoreRead for T {
  fn get_recent_balance(&self, script: &Script) -> anyhow::Result<Amount> {
    let Some((last_height, last)) = self.prefix(&self.store().locker_script_and_height_to_balance, Slice::from(script)).rev().next().transpose()? else {
      return Ok(0.into());
    };
    return Ok(last.into());
  }

  fn get_historical_balance(&self, script: &Script, height: &BlockHeight) -> anyhow::Result<Amount> {
    let Some((_, balance)) = self.range(
      &self.store().locker_script_and_height_to_balance,
      Slice::from(&LockerScriptAndHeight { locker_script: script.clone(), height: BlockHeight { height: 0 } })
        ..=
        Slice::from(&LockerScriptAndHeight { locker_script: script.clone(), height: *height }),
    )
      .rev()
      .next()
      .transpose()? else {
        return Ok(0.into());
      };
    Ok(balance.into())
  }
}

impl AccountStoreWrite for WriteTx<'_> {
  fn insert_balance(&mut self, locker_script: &Script, height: &BlockHeight, balance: &Amount) {
    self.tx.insert(&self.store.locker_script_and_height_to_balance, &LockerScriptAndHeight { locker_script: locker_script.clone(), height: *height }, balance);
    self.tx.insert(&self.store.height_and_locker_script, &HeightAndLockerScript { height: *height, locker_script: locker_script.clone() }, []);
  }
}

#[derive(Debug, bincode::Encode, bincode::Decode, Clone)]
pub struct LockerScriptAndHeight {
  pub locker_script: Script,
  pub height: BlockHeight,
}
impl_bincode_conversion!(LockerScriptAndHeight);

#[derive(Debug, bincode::Encode, bincode::Decode, Clone)]
pub struct HeightAndLockerScript {
  pub height: BlockHeight,
  pub locker_script: Script,
}
impl_bincode_conversion!(HeightAndLockerScript);
