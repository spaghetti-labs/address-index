use binary_layout::prelude::*;
use bitcoin::Amount;
use fjall::Slice;

use crate::{store::script::ScriptID};

use super::{BlockHeight, Batch, Store};

pub trait AccountStoreRead {
  fn get_recent_balance(&self, locker_script_id: ScriptID) -> anyhow::Result<Amount>;
  fn get_historical_balance(&self, locker_script_id: ScriptID, height: BlockHeight) -> anyhow::Result<Amount>;
  fn get_balance_history(&self, locker_script_id: ScriptID) -> anyhow::Result<Vec<(BlockHeight, Amount)>>;
}

pub trait AccountStoreWrite {
  fn insert_recent_balance(&mut self, locker_script_id: ScriptID, balance: Amount);
  fn insert_historical_balance(&mut self, locker_script_id: ScriptID, height: BlockHeight, balance: Amount);
}

impl AccountStoreRead for Store {
  fn get_recent_balance(&self, locker_script_id: ScriptID) -> anyhow::Result<Amount> {
    let Some(balance) = self.locker_script_id_to_balance.get(locker_script_id.to_be_bytes())? else {
      return Ok(Amount::ZERO);
    };
    Ok(Amount::from_sat(u64::from_be_bytes(balance.as_ref().try_into()?)))
  }

  fn get_historical_balance(&self, locker_script_id: ScriptID, height: BlockHeight) -> anyhow::Result<Amount> {
    let mut genesis = locker_script_id_and_height::View::new([0u8; locker_script_id_and_height::SIZE.unwrap()]);
    genesis.locker_script_id_mut().write(locker_script_id);
    genesis.height_mut().write(0);

    let mut target = locker_script_id_and_height::View::new([0u8; locker_script_id_and_height::SIZE.unwrap()]);
    target.locker_script_id_mut().write(locker_script_id);
    target.height_mut().write(height);

    let Some((_, balance)) = self.locker_script_id_and_height_to_balance.range(
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
    for entry in self.locker_script_id_and_height_to_balance.prefix(
      locker_script_id.to_be_bytes(),
    ) {
      let (key, balance) = entry?;
      let key = locker_script_id_and_height::View::new(key);
      historical_balances.push((key.height().read(), Amount::from_sat(u64::from_be_bytes(balance.as_ref().try_into()?))));
    }
    Ok(historical_balances)
  }
}

impl AccountStoreWrite for Batch<'_> {
  fn insert_recent_balance(&mut self, locker_script_id: ScriptID, balance: Amount) {
    self.batch.insert(&self.store.locker_script_id_to_balance, locker_script_id.to_be_bytes(), balance.to_sat().to_be_bytes());
  }

  fn insert_historical_balance(&mut self, locker_script_id: ScriptID, height: BlockHeight, balance: Amount) {
    let mut key = locker_script_id_and_height::View::new([0u8; locker_script_id_and_height::SIZE.unwrap()]);
    key.locker_script_id_mut().write(locker_script_id);
    key.height_mut().write(height);

    let mut reverse_key = height_and_locker_script_id::View::new([0u8; height_and_locker_script_id::SIZE.unwrap()]);
    reverse_key.height_mut().write(height);
    reverse_key.locker_script_id_mut().write(locker_script_id);

    self.batch.insert(&self.store.locker_script_id_and_height_to_balance, Slice::new(key.into_storage().as_ref()), balance.to_sat().to_be_bytes());
    self.batch.insert(&self.store.height_and_locker_script_id, reverse_key.into_storage(), []);
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

