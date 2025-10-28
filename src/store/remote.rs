use std::result::Result;

use bitcoin::{hashes::Hash, Amount, OutPoint, ScriptHash, Txid};
use scroll::{ctx::{ActualSizeWith, SizeWith, TryFromCtx, TryIntoCtx}, Endian, Pread, Pwrite, SizeWith};

#[derive(Pread, Pwrite, SizeWith)]
pub struct ScriptHashWrapper {
  pub bytes: [u8; 20],
}

impl From<&ScriptHash> for ScriptHashWrapper {
  fn from(script_hash: &ScriptHash) -> Self {
    ScriptHashWrapper {
      bytes: script_hash.to_byte_array(),
    }
  }
}

impl From<&ScriptHashWrapper> for ScriptHash {
  fn from(wrapper: &ScriptHashWrapper) -> Self {
    ScriptHash::from_byte_array(wrapper.bytes)
  }
}

#[derive(Pread, Pwrite, SizeWith)]
pub struct TxidWrapper {
  pub bytes: [u8; 32],
}

impl From<&Txid> for TxidWrapper {
  fn from(txid: &Txid) -> Self {
    TxidWrapper {
      bytes: txid.to_byte_array(),
    }
  }
}

impl From<&TxidWrapper> for Txid {
  fn from(wrapper: &TxidWrapper) -> Self {
    Txid::from_byte_array(wrapper.bytes)
  }
}

#[derive(Pread, Pwrite, SizeWith)]
pub struct AmountWrapper {
  pub satoshis: u64,
}

impl From<&Amount> for AmountWrapper {
  fn from(amount: &Amount) -> Self {
    AmountWrapper {
      satoshis: amount.to_sat(),
    }
  }
}

impl From<&AmountWrapper> for Amount {
  fn from(wrapper: &AmountWrapper) -> Self {
    Amount::from_sat(wrapper.satoshis)
  }
}

#[derive(Pread, Pwrite, SizeWith)]
pub struct OutPointWrapper {
  #[scroll(with = TxidWrapper)]
  pub txid: Txid,
  pub vout: u32,
}

impl From<&OutPoint> for OutPointWrapper {
  fn from(outpoint: &OutPoint) -> Self {
    OutPointWrapper {
      txid: outpoint.txid,
      vout: outpoint.vout,
    }
  }
}

impl From<&OutPointWrapper> for OutPoint {
  fn from(wrapper: &OutPointWrapper) -> Self {
    OutPoint {
      txid: wrapper.txid,
      vout: wrapper.vout,
    }
  }
}

pub struct OptionWrapper<T> {
  pub option: Option<T>,
}

impl<'a, T: TryFromCtx<'a, Endian, Error = scroll::Error>> TryFromCtx<'a, Endian> for OptionWrapper<T> {
  type Error = scroll::Error;

  fn try_from_ctx(
    src: &'a [u8],
    ctx: Endian,
  ) -> Result<(Self, usize), Self::Error> {
    let mut offset = 0;
    let flag: u8 = src.gread_with(&mut offset, ctx)?;
    if flag == 0 {
      Ok((OptionWrapper { option: None }, offset))
    } else {
      let value: T = src.gread_with(&mut offset, ctx)?;
      Ok((OptionWrapper { option: Some(value) }, offset))
    }
  }
}

impl<T> TryIntoCtx<Endian> for &OptionWrapper<T>
where
  for<'a> &'a T: TryIntoCtx<Endian, Error = scroll::Error>,
  T: SizeWith<Endian>,
{
  type Error = scroll::Error;

  fn try_into_ctx(
    self,
    dst: &mut [u8],
    ctx: Endian,
  ) -> Result<usize, Self::Error> {
    let mut offset = 0;
    match &self.option {
      None => {
        dst.gwrite_with(0u8, &mut offset, ctx)?;
      }
      Some(value) => {
        dst.gwrite_with(1u8, &mut offset, ctx)?;
        dst.gwrite_with(value, &mut offset, ctx)?;
      }
    }
    Ok(offset)
  }
}

impl<T: ActualSizeWith<Endian>> ActualSizeWith<Endian> for OptionWrapper<T>
{
  fn actual_size_with(&self, ctx: &Endian) -> usize {
    1 + self.option.as_ref().map_or(0, |v| v.actual_size_with(ctx))
  }
}

impl<T: Clone> From<&Option<T>> for OptionWrapper<T> {
  fn from(option: &Option<T>) -> Self {
    OptionWrapper { option: option.clone() }
  }
}

impl<T: Clone> From<&OptionWrapper<T>> for Option<T> {
  fn from(wrapper: &OptionWrapper<T>) -> Self {
    wrapper.option.clone()
  }
}
