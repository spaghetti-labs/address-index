use fjall::Slice;

use crate::{impl_bincode_conversion, store::{common::BlockHeight, ReadTx, TxRead, WriteTx}};
use super::{common::{Amount, CompressedPubKey, PubKeyHash, Script, TransactionID, UncompressedPubKey}};

pub trait AccountStoreRead {
  fn get_recent_balance(&self, script: &Script) -> anyhow::Result<Amount>;
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

pub enum StandardScript {
  P2PKCompressed { pubkey: CompressedPubKey },
  P2PKUncompressed { pubkey: UncompressedPubKey },
  P2PKH { pubkey_hash: PubKeyHash },
}

impl TryFrom<&Script> for StandardScript {
  type Error = anyhow::Error;

  fn try_from(script: &Script) -> Result<Self, Self::Error> {
    let bytes = &script.bytes;
    if bytes.len() == 35 && bytes[0] == 0x41 && bytes[34] == 0xac {
      // P2PK (compressed pubkey)
      let mut pubkey_bytes = [0u8; 33];
      pubkey_bytes.copy_from_slice(&bytes[1..34]);
      let pubkey = CompressedPubKey { bytes: pubkey_bytes };
      Ok(StandardScript::P2PKCompressed { pubkey })
    } else if bytes.len() == 67 && bytes[0] == 0x41 && bytes[66] == 0xac {
      // P2PK (uncompressed pubkey)
      let mut pubkey_bytes = [0u8; 65];
      pubkey_bytes.copy_from_slice(&bytes[1..66]);
      let pubkey = UncompressedPubKey { bytes: pubkey_bytes };
      Ok(StandardScript::P2PKUncompressed { pubkey })
    } else if bytes.len() == 25 && bytes[0] == 0x76 && bytes[1] == 0xa9 && bytes[2] == 0x14 && bytes[23] == 0x88 && bytes[24] == 0xac {
      // P2PKH
      let mut hash_bytes = [0u8; 20];
      hash_bytes.copy_from_slice(&bytes[3..23]);
      let pubkey_hash = PubKeyHash { bytes: hash_bytes };
      Ok(StandardScript::P2PKH { pubkey_hash })
    } else {
      Err(anyhow::anyhow!("Unsupported script format"))
    }
  }
}
