use fjall::Slice;

use crate::{impl_bincode_conversion, store::{common::{BlockHeight, ScriptID}, TxRead, WriteTx}};
use super::common::Amount;

pub trait AccountStoreRead {
  fn get_recent_balance(&self, locker_script_id: ScriptID) -> anyhow::Result<Amount>;
  fn get_historical_balance(&self, locker_script_id: ScriptID, height: BlockHeight) -> anyhow::Result<Amount>;
  fn get_balance_history(&self, locker_script_id: ScriptID) -> anyhow::Result<Vec<(BlockHeight, Amount)>>;
}

pub trait AccountStoreWrite {
  fn insert_balance(&mut self, locker_script_id: ScriptID, height: BlockHeight, balance: &Amount);
}

impl<T: TxRead> AccountStoreRead for T {
  fn get_recent_balance(&self, locker_script_id: ScriptID) -> anyhow::Result<Amount> {
    let Some((_, last)) = self.prefix(&self.store().locker_script_id_and_height_to_balance, Slice::from(locker_script_id)).rev().next().transpose()? else {
      return Ok(0.into());
    };
    return Ok(last.into());
  }

  fn get_historical_balance(&self, locker_script_id: ScriptID, height: BlockHeight) -> anyhow::Result<Amount> {
    let Some((_, balance)) = self.range(
      &self.store().locker_script_id_and_height_to_balance,
      Slice::from(&LockerScriptIDAndHeight { locker_script_id, height: BlockHeight { height: 0 } })
        ..=
        Slice::from(&LockerScriptIDAndHeight { locker_script_id, height }),
    )
      .rev()
      .next()
      .transpose()? else {
        return Ok(0.into());
      };
    Ok(balance.into())
  }

  fn get_balance_history(&self, locker_script_id: ScriptID) -> anyhow::Result<Vec<(BlockHeight, Amount)>> {
    let mut historical_balances = Vec::new();
    for entry in self.prefix(
      &self.store().locker_script_id_and_height_to_balance, 
      Slice::from(locker_script_id),
    ) {
      let (key, balance) = entry?;
      let LockerScriptIDAndHeight { height, .. } = key.into();
      historical_balances.push((height, balance.into()));
    }
    Ok(historical_balances)
  }
}

impl AccountStoreWrite for WriteTx<'_> {
  fn insert_balance(&mut self, locker_script_id: ScriptID, height: BlockHeight, balance: &Amount) {
    self.tx.insert(&self.store.locker_script_id_and_height_to_balance, &LockerScriptIDAndHeight { locker_script_id, height }, balance);
    self.tx.insert(&self.store.height_and_locker_script_id, &HeightAndLockerScriptID { height, locker_script_id }, []);
  }
}

#[derive(Debug, bincode::Encode, bincode::Decode, Copy, Clone)]
pub struct LockerScriptIDAndHeight {
  pub locker_script_id: ScriptID,
  pub height: BlockHeight,
}
impl_bincode_conversion!(LockerScriptIDAndHeight);

#[derive(Debug, bincode::Encode, bincode::Decode, Copy, Clone)]
pub struct HeightAndLockerScriptID {
  pub height: BlockHeight,
  pub locker_script_id: ScriptID,
}
impl_bincode_conversion!(HeightAndLockerScriptID);
