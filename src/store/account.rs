use bincode::{de::Decoder, enc::Encoder, error::{DecodeError, EncodeError}, Decode, Encode};
use bitcoin::{hashes::{Hash}, Amount, ScriptHash};
use fjall::Slice;

use crate::sorted_vec::{SortedEntry, SortedMap};

use super::{BlockHeight, Batch, Store};

#[derive(Debug, Clone)]
pub struct AccountState {
  pub recent_balance: Amount,
  pub balance_history: SortedMap<BlockHeight, Amount>,
}

impl Encode for AccountState {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.recent_balance.to_sat().encode(encoder)?;
    self.balance_history.as_ref().len().encode(encoder)?;
    for SortedEntry { key: height, value: balance } in self.balance_history.as_ref().iter() {
      height.encode(encoder)?;
      balance.to_sat().encode(encoder)?;
    }
    Ok(())
  }
}

impl<Context> Decode<Context> for AccountState {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    let recent_balance = Amount::from_sat(u64::decode(decoder)?);

    let history_len = usize::decode(decoder)?;
    let mut balance_history = Vec::with_capacity(history_len);
    for _ in 0..history_len {
      let height = BlockHeight::decode(decoder)?;
      let amount = Amount::from_sat(u64::decode(decoder)?);
      balance_history.push(SortedEntry{
        key: height,
        value: amount,
      });
    }
    let balance_history = SortedMap::ingest(balance_history);

    Ok(Self {
      recent_balance,
      balance_history,
    })
  }
}

impl AccountState {
  pub fn empty() -> Self {
    Self {
      recent_balance: Amount::ZERO,
      balance_history: SortedMap::empty(),
    }
  }
}

pub trait AccountStoreRead {
  fn get_account_state(&self, locker_script_hash: &ScriptHash) -> anyhow::Result<AccountState>;
}

pub trait AccountStoreWrite {
  fn set_account_state(&mut self, locker_script_hash: &ScriptHash, state: &AccountState);
}

impl AccountStoreRead for Store {
  fn get_account_state(&self, locker_script_hash: &ScriptHash) -> anyhow::Result<AccountState> {
    let Some(state) = self.locker_script_hash_to_account_state.get(locker_script_hash.as_byte_array())? else {
      return Ok(AccountState::empty());
    };
    Ok(bincode::decode_from_slice(state.as_ref(), bincode::config::standard())?.0)
  }
}

impl AccountStoreWrite for Batch<'_> {
  fn set_account_state(&mut self, locker_script_hash: &ScriptHash, state: &AccountState) {
    let encoded = bincode::encode_to_vec(state, bincode::config::standard()).unwrap();
    self.batch.insert(&self.store.locker_script_hash_to_account_state, locker_script_hash.as_byte_array(), Slice::from(encoded));
  }
}
