use binary_layout::prelude::*;
use bitcoin::Amount;
use fjall::Slice;

use crate::store::script::ScriptID;

use super::{BlockHeight, TxRead, WriteTx};

pub trait AccountStoreRead {
  fn get_recent_balance(&self, locker_script_id: ScriptID) -> anyhow::Result<Amount>;
  fn get_historical_balance(&self, locker_script_id: ScriptID, height: BlockHeight) -> anyhow::Result<Amount>;
  fn get_balance_history(&self, locker_script_id: ScriptID) -> anyhow::Result<Vec<(BlockHeight, Amount)>>;
}

pub trait AccountStoreWrite {
  fn insert_historical_balance(&mut self, locker_script_id: ScriptID, height: BlockHeight, balance: Amount);
}

impl<T: TxRead> AccountStoreRead for T {
  fn get_recent_balance(&self, locker_script_id: ScriptID) -> anyhow::Result<Amount> {
    let Some((_, last)) = self.prefix(&self.store().locker_script_id_and_height_to_balance, locker_script_id.to_be_bytes()).rev().next().transpose()? else {
      return Ok(Amount::ZERO);
    };
    return Ok(Amount::from_sat(u64::from_be_bytes(last.as_ref().try_into()?)));
  }

  fn get_historical_balance(&self, locker_script_id: ScriptID, height: BlockHeight) -> anyhow::Result<Amount> {
    let mut genesis = locker_script_id_and_height::View::new([0u8; locker_script_id_and_height::SIZE.unwrap()]);
    genesis.locker_script_id_mut().write(locker_script_id);
    genesis.height_mut().write(0);

    let mut target = locker_script_id_and_height::View::new([0u8; locker_script_id_and_height::SIZE.unwrap()]);
    target.locker_script_id_mut().write(locker_script_id);
    target.height_mut().write(height);

    let Some((_, balance)) = self.range(
      &self.store().locker_script_id_and_height_to_balance,
      genesis.into_storage()..=target.into_storage(),
    )
      .rev()
      .next()
      .transpose()? else {
        return Ok(Amount::ZERO);
      };
    Ok(Amount::from_sat(u64::from_be_bytes(balance.as_ref().try_into()?)))
  }

  fn get_balance_history(&self, locker_script_id: ScriptID) -> anyhow::Result<Vec<(BlockHeight, Amount)>> {
    let mut historical_balances = Vec::new();
    for entry in self.prefix(
      &self.store().locker_script_id_and_height_to_balance, 
      locker_script_id.to_be_bytes(),
    ) {
      let (key, balance) = entry?;
      let key = locker_script_id_and_height::View::new(key);
      historical_balances.push((key.height().read(), Amount::from_sat(u64::from_be_bytes(balance.as_ref().try_into()?))));
    }
    Ok(historical_balances)
  }
}

impl AccountStoreWrite for WriteTx<'_> {
  fn insert_historical_balance(&mut self, locker_script_id: ScriptID, height: BlockHeight, balance: Amount) {
    let mut key = locker_script_id_and_height::View::new([0u8; locker_script_id_and_height::SIZE.unwrap()]);
    key.locker_script_id_mut().write(locker_script_id);
    key.height_mut().write(height);

    let mut reverse_key = height_and_locker_script_id::View::new([0u8; height_and_locker_script_id::SIZE.unwrap()]);
    reverse_key.height_mut().write(height);
    reverse_key.locker_script_id_mut().write(locker_script_id);

    self.tx.insert(&self.store.locker_script_id_and_height_to_balance, Slice::new(key.into_storage().as_ref()), balance.to_sat().to_be_bytes());
    self.tx.insert(&self.store.height_and_locker_script_id, reverse_key.into_storage(), []);
  }
}

binary_layout!(locker_script_id_and_height, BigEndian, {
  locker_script_id: ScriptID,
  height: BlockHeight,
});

binary_layout!(height_and_locker_script_id, BigEndian, {
  height: BlockHeight,
  locker_script_id: ScriptID,
});

