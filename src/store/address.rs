use crate::impl_bincode_conversion;
use super::Batch;

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct AddressState {
  pub address: String,
  pub utxo_balance: super::common::Amount,
}
impl_bincode_conversion!(AddressState);

pub struct AddressStore {
  pub(super) partition: fjall::Partition,
}

impl AddressStore {
  pub fn insert_address(&self, state: &AddressState, batch: &mut Batch) {
    match state.utxo_balance.satoshis {
      0 => batch.batch.remove(&self.partition, &state.address),
      _ => batch.batch.insert(&self.partition, &state.address, state),
    }
  }

  pub fn get_address(&self, address: &str) -> anyhow::Result<Option<AddressState>> {
    Ok(self.partition.get(address)?.map(Into::into))
  }
}
