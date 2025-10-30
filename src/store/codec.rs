use std::result::Result;

use bitcoin::{hashes::Hash, Amount, OutPoint, ScriptHash, Txid};
use byten::{Decode, DecodeError, Decoder, Encode, EncodeError, Encoder, FixedMeasurer, Measurer, prim::U64BE, var};

pub struct ScriptHashCodec;

impl Decoder for ScriptHashCodec {
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
    Encode::encode(&bytes, encoded, offset)
  }
}

impl Measurer for ScriptHashCodec {
  type Decoded = ScriptHash;
  fn measure(&self, _value: &Self::Decoded) -> usize { self.fixed_measure() }
}

impl FixedMeasurer for ScriptHashCodec {
  fn fixed_measure(&self) -> usize { ScriptHash::LEN }
}

pub struct TxidCodec;

impl Decoder for TxidCodec {
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
    Encode::encode(&bytes, encoded, offset)
  }
}

impl Measurer for TxidCodec {
  type Decoded = Txid;
  fn measure(&self, _decoded: &Self::Decoded) -> usize { self.fixed_measure() }
}

impl FixedMeasurer for TxidCodec {
  fn fixed_measure(&self) -> usize { Txid::LEN }
}

pub enum AmountCodec {
  Fix,
  Var,
}

impl Decoder for AmountCodec {
  type Decoded = Amount;

  fn decode(&self, encoded: &[u8], offset: &mut usize) -> Result<Self::Decoded, DecodeError> {
    let satoshis = match self {
      AmountCodec::Fix => U64BE.decode(encoded, offset)?,
      AmountCodec::Var => var::U64BE.decode(encoded, offset)?,
    };
    Ok(Amount::from_sat(satoshis))
  }
}

impl Encoder for AmountCodec {
  type Decoded = Amount;

  fn encode(&self, decoded: &Self::Decoded, encoded: &mut [u8], offset: &mut usize) -> Result<(), EncodeError> {
    match self {
      AmountCodec::Fix => U64BE.encode(&decoded.to_sat(), encoded, offset),
      AmountCodec::Var => var::U64BE.encode(&decoded.to_sat(), encoded, offset),
    }
  }
}

impl Measurer for AmountCodec {
  type Decoded = Amount;

  fn measure(&self, decoded: &Self::Decoded) -> usize {
    match self {
      AmountCodec::Fix => self.fixed_measure(),
      AmountCodec::Var => var::U64BE.measure(&decoded.to_sat()),
    }
  }
}

impl FixedMeasurer for AmountCodec {
  fn fixed_measure(&self) -> usize {
    match self {
      AmountCodec::Fix => U64BE.fixed_measure(),
      AmountCodec::Var => panic!("AmountCodec::Var does not have a fixed measure"),
    }
  }
}

pub enum OutPointCodec {
  Var,
  Fix,
}

impl Decoder for OutPointCodec {
  type Decoded = OutPoint;

  fn decode(&self, encoded: &[u8], offset: &mut usize) -> Result<Self::Decoded, DecodeError> {
    let txid = TxidCodec.decode(encoded, offset)?;
    let vout = match self {
      OutPointCodec::Fix => U64BE.decode(encoded, offset)? as u32,
      OutPointCodec::Var => var::U32BE.decode(encoded, offset)?,
    };
    Ok(OutPoint { txid, vout })
  }
}

impl Encoder for OutPointCodec {
  type Decoded = OutPoint;
  fn encode(&self, decoded: &Self::Decoded, encoded: &mut [u8], offset: &mut usize) -> Result<(), EncodeError> {
    TxidCodec.encode(&decoded.txid, encoded, offset)?;
    match self {
      OutPointCodec::Fix => U64BE.encode(&(decoded.vout as u64), encoded, offset),
      OutPointCodec::Var => var::U32BE.encode(&decoded.vout, encoded, offset),
    }
  }
}

impl Measurer for OutPointCodec {
  type Decoded = OutPoint;
  fn measure(&self, decoded: &Self::Decoded) -> usize {
    match self {
      OutPointCodec::Fix => self.fixed_measure(),
      OutPointCodec::Var => Txid::LEN + var::U32BE.measure(&decoded.vout),
    }
  }
}

impl FixedMeasurer for OutPointCodec {
  fn fixed_measure(&self) -> usize {
    match self {
      OutPointCodec::Fix => Txid::LEN + U64BE.fixed_measure(),
      OutPointCodec::Var => panic!("OutPointCodec::Var does not have a fixed measure"),
    }
  }
}
