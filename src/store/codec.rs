use std::result::Result;

use bitcoin::{hashes::Hash, Amount, OutPoint, ScriptHash, Txid};
use byten::{byten, DecodeError, Decoder, EncodeError, Encoder, FixedMeasurer, Measurer};
use byten::DecodeDefault;
use byten::EncodeDefault;

pub struct ScriptHashCodec;

impl Decoder<'_, '_> for ScriptHashCodec {
  type Decoded = ScriptHash;

  fn decode(&self, encoded: &[u8], offset: &mut usize) -> Result<Self::Decoded, DecodeError> {
    let bytes = <[u8; _]>::decode(encoded, offset)?;
    Ok(ScriptHash::from_byte_array(bytes))
  }
}

impl Encoder for ScriptHashCodec {
  type Decoded = ScriptHash;
  fn encode(&self, decoded: &Self::Decoded, encoded: &mut [u8], offset: &mut usize) -> Result<(), EncodeError> {
    let bytes = decoded.to_byte_array();
    <[u8; _]>::encode(&bytes, encoded, offset)
  }
}

impl Measurer for ScriptHashCodec {
  type Decoded = ScriptHash;
  fn measure(&self, _value: &Self::Decoded) -> Result<usize, EncodeError> { Ok(self.measure_fixed()) }
}

impl FixedMeasurer for ScriptHashCodec {
  fn measure_fixed(&self) -> usize { ScriptHash::LEN }
}

pub struct TxidCodec;

impl Decoder<'_, '_> for TxidCodec {
  type Decoded = Txid;

  fn decode(&self, encoded: &[u8], offset: &mut usize) -> Result<Self::Decoded, DecodeError> {
    let bytes = <[u8; _]>::decode(encoded, offset)?;
    Ok(Txid::from_byte_array(bytes))
  }
}

impl Encoder for TxidCodec {
  type Decoded = Txid;
  fn encode(&self, decoded: &Self::Decoded, encoded: &mut [u8], offset: &mut usize) -> Result<(), EncodeError> {
    let bytes = decoded.to_byte_array();
    <[u8; _]>::encode(&bytes, encoded, offset)
  }
}

impl Measurer for TxidCodec {
  type Decoded = Txid;
  fn measure(&self, _decoded: &Self::Decoded) -> Result<usize, EncodeError> { Ok(self.measure_fixed()) }
}

impl FixedMeasurer for TxidCodec {
  fn measure_fixed(&self) -> usize { Txid::LEN }
}

pub enum AmountCodec {
  Fix,
  Var,
}

impl Decoder<'_, '_> for AmountCodec {
  type Decoded = Amount;

  fn decode(&self, encoded: &[u8], offset: &mut usize) -> Result<Self::Decoded, DecodeError> {
    let satoshis = match self {
      AmountCodec::Fix => byten!(u64 $be).decode(encoded, offset)?,
      AmountCodec::Var => byten!(u64 $uvarbe).decode(encoded, offset)?,
    };
    Ok(Amount::from_sat(satoshis))
  }
}

impl Encoder for AmountCodec {
  type Decoded = Amount;

  fn encode(&self, decoded: &Self::Decoded, encoded: &mut [u8], offset: &mut usize) -> Result<(), EncodeError> {
    match self {
      AmountCodec::Fix => byten!(u64 $be).encode(&decoded.to_sat(), encoded, offset),
      AmountCodec::Var => byten!(u64 $uvarbe).encode(&decoded.to_sat(), encoded, offset),
    }
  }
}

impl Measurer for AmountCodec {
  type Decoded = Amount;

  fn measure(&self, decoded: &Self::Decoded) -> Result<usize, EncodeError> {
    Ok(match self {
      AmountCodec::Fix => self.measure_fixed(),
      AmountCodec::Var => byten!(u64 $uvarbe).measure(&decoded.to_sat())?,
    })
  }
}

impl FixedMeasurer for AmountCodec {
  fn measure_fixed(&self) -> usize {
    match self {
      AmountCodec::Fix => byten!(u64 $be).measure_fixed(),
      AmountCodec::Var => panic!("AmountCodec::Var does not have a fixed measure"),
    }
  }
}

pub enum OutPointCodec {
  Var,
  Fix,
}

impl Decoder<'_, '_> for OutPointCodec {
  type Decoded = OutPoint;

  fn decode(&self, encoded: &[u8], offset: &mut usize) -> Result<Self::Decoded, DecodeError> {
    let txid = TxidCodec.decode(encoded, offset)?;
    let vout = match self {
      OutPointCodec::Fix => byten!(u64 $be).decode(encoded, offset)? as u32,
      OutPointCodec::Var => byten!(u32 $uvarbe).decode(encoded, offset)?,
    };
    Ok(OutPoint { txid, vout })
  }
}

impl Encoder for OutPointCodec {
  type Decoded = OutPoint;
  fn encode(&self, decoded: &Self::Decoded, encoded: &mut [u8], offset: &mut usize) -> Result<(), EncodeError> {
    TxidCodec.encode(&decoded.txid, encoded, offset)?;
    match self {
      OutPointCodec::Fix => byten!(u64 $be).encode(&(decoded.vout as u64), encoded, offset),
      OutPointCodec::Var => byten!(u32 $uvarbe).encode(&decoded.vout, encoded, offset),
    }
  }
}

impl Measurer for OutPointCodec {
  type Decoded = OutPoint;
  fn measure(&self, decoded: &Self::Decoded) -> Result<usize, EncodeError> {
    Ok(match self {
      OutPointCodec::Fix => self.measure_fixed(),
      OutPointCodec::Var => Txid::LEN + byten!(u32 $uvarbe).measure(&decoded.vout)?,
    })
  }
}

impl FixedMeasurer for OutPointCodec {
  fn measure_fixed(&self) -> usize {
    match self {
      OutPointCodec::Fix => Txid::LEN + byten!(u64 $be).measure_fixed(),
      OutPointCodec::Var => panic!("OutPointCodec::Var does not have a fixed measure"),
    }
  }
}
