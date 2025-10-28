use bincode::{config::{BigEndian, Configuration, Fixint, Varint}, de::Decoder, enc::Encoder, error::{AllowedEnumVariants, DecodeError, EncodeError}, Decode, Encode};
use bitcoin::{hashes::Hash, Amount, OutPoint, ScriptHash, Txid};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rocksdb::SliceTransform;

use crate::{iter_util::IterExt, store::{Batch, BlockHeight, Store}};

const BIN_KEY: Configuration<BigEndian, Fixint> = bincode::config::standard().with_big_endian().with_fixed_int_encoding();
const BIN_VALUE: Configuration<BigEndian, Varint> = bincode::config::standard().with_big_endian().with_variable_int_encoding();

pub fn cf_descriptors(common_opts: &rocksdb::Options) -> Vec<rocksdb::ColumnFamilyDescriptor> {
  let mut outpoint_to_txo_opts = common_opts.clone();
  outpoint_to_txo_opts.set_merge_operator("txo_merge", |key, existing, operands| {
    let mut state: Option<TXOState> = existing.map(|v| bincode::decode_from_slice(v.as_ref(), BIN_VALUE).unwrap().0);
    for op in operands {
      let update: TXOUpdate = bincode::decode_from_slice(op.as_ref(), BIN_VALUE).unwrap().0;
      match update {
        TXOUpdate::Generated(g) => {
          if let Some(ref state) = state {
            let TXOKey { outpoint } = bincode::decode_from_slice( key, BIN_KEY).unwrap().0;
            if outpoint.vout != 0 {
              panic!("TXOGenerated update for existing TXOState at non-coinbase outpoint {}", outpoint);
            } if state.generated_height >= g.generated_height {
              panic!("TXOGenerated update with non-increasing generated_height for coinbase outpoint {}", outpoint);
            } else {
              tracing::warn!("TXOGenerated update for existing TXOState at coinbase outpoint {}", outpoint);
              continue;
            }
          }
          state = Some(TXOState {
            locker_script_hash: g.locker_script_hash,
            value: g.value,
            generated_height: g.generated_height,
            spent_height: None,
          });
        }
        TXOUpdate::Spent(s) => {
          let Some(ref mut state) = state else {
            panic!("TXOSpent update for missing TXOState");
          };
          state.spent_height = Some(s.spent_height);
        }
      }
    }
    let state = state.expect("no operands in TXO merge");
    Some(bincode::encode_to_vec(&state, BIN_VALUE).unwrap())
  }, |_key, _existing, _operands| None);

  let mut locker_script_hash_and_outpoint_opts = common_opts.clone();
  locker_script_hash_and_outpoint_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(ScriptHash::LEN));
  let mut locker_script_hash_and_outpoint_opts_block = rocksdb::BlockBasedOptions::default();
  locker_script_hash_and_outpoint_opts_block.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
  locker_script_hash_and_outpoint_opts_block.set_whole_key_filtering(false);
  locker_script_hash_and_outpoint_opts.set_block_based_table_factory(&locker_script_hash_and_outpoint_opts_block);

  let mut height_and_outpoint_opts = common_opts.clone();
  height_and_outpoint_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(BlockHeight::BITS as usize / 8));
  let mut height_and_outpoint_opts_block = rocksdb::BlockBasedOptions::default();
  height_and_outpoint_opts_block.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
  height_and_outpoint_opts_block.set_whole_key_filtering(false);
  height_and_outpoint_opts.set_block_based_table_factory(&height_and_outpoint_opts_block);

  vec![
    rocksdb::ColumnFamilyDescriptor::new("outpoint_to_txo_state", outpoint_to_txo_opts),
    rocksdb::ColumnFamilyDescriptor::new("locker_script_hash_and_outpoint", locker_script_hash_and_outpoint_opts.clone()),
    rocksdb::ColumnFamilyDescriptor::new("generated_height_and_outpoint", height_and_outpoint_opts.clone()),
    rocksdb::ColumnFamilyDescriptor::new("spent_height_and_outpoint", height_and_outpoint_opts.clone()),
  ]
}

#[derive(Copy, Clone)]
pub struct TXOGenerated {
  pub locker_script_hash: ScriptHash,
  pub value: Amount,
  pub generated_height: BlockHeight,
}

impl Encode for TXOGenerated {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.locker_script_hash.to_byte_array().encode(encoder)?;
    self.value.to_sat().encode(encoder)?;
    self.generated_height.encode(encoder)?;
    Ok(())
  }
}

impl<Context> Decode<Context> for TXOGenerated {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    Ok(Self {
      locker_script_hash: ScriptHash::from_byte_array(
      <[u8; _]>::decode(decoder)?,
    ),
      value: Amount::from_sat(u64::decode(decoder)?),
      generated_height: BlockHeight::decode(decoder)?,
    })
  }
}

#[derive(Copy, Clone)]
pub struct TXOSpent {
  pub spent_height: BlockHeight,
}

impl Encode for TXOSpent {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.spent_height.encode(encoder)?;
    Ok(())
  }
}

impl<Context> Decode<Context> for TXOSpent {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    Ok(Self {
      spent_height: BlockHeight::decode(decoder)?,
    })
  }
}

#[derive(Copy, Clone)]
#[repr(u8)]
pub enum TXOUpdate {
  Generated(TXOGenerated),
  Spent(TXOSpent),
}

impl Encode for TXOUpdate {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    match self {
      TXOUpdate::Generated(g) => {
        1u8.encode(encoder)?;
        g.encode(encoder)?;
      }
      TXOUpdate::Spent(s) => {
        2u8.encode(encoder)?;
        s.encode(encoder)?;
      }
    }
    Ok(())
  }
}

impl<Context> Decode<Context> for TXOUpdate {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    let tag = u8::decode(decoder)?;
    match tag {
      1 => Ok(TXOUpdate::Generated(TXOGenerated::decode(decoder)?)),
      2 => Ok(TXOUpdate::Spent(TXOSpent::decode(decoder)?)),
      _ => Err(DecodeError::UnexpectedVariant { type_name: "TXOUpdate", allowed: &AllowedEnumVariants::Range { min: 1, max: 2 }, found: tag as u32 }),
    }
  }
}

#[derive(Copy, Clone)]
pub struct TXOState {
  pub locker_script_hash: ScriptHash,
  pub value: Amount,
  pub generated_height: BlockHeight,
  pub spent_height: Option<BlockHeight>,
}

impl Encode for TXOState {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.locker_script_hash.to_byte_array().encode(encoder)?;
    self.value.to_sat().encode(encoder)?;
    self.generated_height.encode(encoder)?;
    self.spent_height.encode(encoder)?;
    Ok(())
  }
}

impl<Context> Decode<Context> for TXOState {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    Ok(Self {
      locker_script_hash: ScriptHash::from_byte_array(
      <[u8; _]>::decode(decoder)?,
    ),
      value: Amount::from_sat(u64::decode(decoder)?),
      generated_height: BlockHeight::decode(decoder)?,
      spent_height: Option::<BlockHeight>::decode(decoder)?,
    })
  }
}

pub trait TXOStoreRead {
  fn get_txos<'store, 'key>(
    &'store self,
    outpoints: impl 'key + IntoIterator<Item = &'key OutPoint>,
  ) -> anyhow::Result<impl 'store + Iterator<Item = anyhow::Result<Option<TXOState>>>>;

  fn get_locker_script_txos<'store, 'key>(
    &'store self,
    locker_script_hash: &ScriptHash,
  ) -> anyhow::Result<impl 'store + Iterator<Item = anyhow::Result<OutPoint>>>;
}

pub trait TXOStoreWrite {
  fn generated_txos<'data>(&mut self, entries: impl IntoParallelIterator<Item = (&'data OutPoint, &'data TXOGenerated)>);
  fn spent_txos<'data>(&mut self, entries: impl IntoParallelIterator<Item = (&'data OutPoint, &'data TXOSpent)>);
}

impl TXOStoreRead for Store {
  fn get_txos<'store, 'key>(
    &'store self,
    outpoints: impl 'key + IntoIterator<Item = &'key OutPoint>,
  ) -> anyhow::Result<impl 'store + Iterator<Item = anyhow::Result<Option<TXOState>>>> {
    let cf = self.db.cf_handle("outpoint_to_txo_state").unwrap();

    let keys = outpoints
      .into_iter()
      .map(|h| bincode::encode_to_vec(TXOKey { outpoint: *h }, BIN_KEY).unwrap())
      .collect::<Vec<_>>();

    if !keys.is_sorted() {
      anyhow::bail!("outpoints must be provided in sorted order");
    }

    Ok(
      self.db.batched_multi_get_cf(&cf, &keys, true)
        .into_iter()
        .map(|res| -> anyhow::Result<_> {
          let Some(value) = res? else {
            return Ok(None);
          };
          Ok(Some(bincode::decode_from_slice(value.as_ref(), BIN_VALUE)?.0))
        })
    )
  }

  fn get_locker_script_txos<'store, 'key>(
    &'store self,
    locker_script_hash: &ScriptHash,
  ) -> anyhow::Result<impl 'store + Iterator<Item = anyhow::Result<OutPoint>>> {
    let cf = self.db.cf_handle("locker_script_hash_and_outpoint").unwrap();
    let prefix = locker_script_hash.as_byte_array();

    let iter = self.db.prefix_iterator_cf(&cf, prefix);
    Ok(
      iter.map(|res| -> anyhow::Result<LockerScriptHashAndOutpointKey> {
        let (key, _value) = res?;
        Ok(bincode::decode_from_slice(key.as_ref(), BIN_KEY)?.0)
      })
      .take_while({
        let locker_script_hash = locker_script_hash.clone();
        move |key| {
          match key {
            Ok(k) => k.locker_script_hash == locker_script_hash,
            Err(_) => true,
          }
        }
      })
      .map_ok(|k| Ok(k.outpoint))
    )
  }
}

pub struct TXOKey {
  pub outpoint: OutPoint,
}

impl Encode for TXOKey {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.outpoint.txid.as_byte_array().encode(encoder)?;
    self.outpoint.vout.encode(encoder)?;
    Ok(())
  }
}

impl<Context> Decode<Context> for TXOKey {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    Ok(Self {
      outpoint: OutPoint {
        txid: Txid::from_byte_array(<[u8; _]>::decode(decoder)?),
        vout: u32::decode(decoder)?,
      },
    })
  }
}

pub struct LockerScriptHashAndOutpointKey {
  pub locker_script_hash: ScriptHash,
  pub outpoint: OutPoint,
}

impl Encode for LockerScriptHashAndOutpointKey {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.locker_script_hash.to_byte_array().encode(encoder)?;
    self.outpoint.txid.as_byte_array().encode(encoder)?;
    self.outpoint.vout.encode(encoder)?;
    Ok(())
  }
}

impl<Context> Decode<Context> for LockerScriptHashAndOutpointKey {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    Ok(Self {
      locker_script_hash: ScriptHash::from_byte_array(<[u8; _]>::decode(decoder)?),
      outpoint: OutPoint {
        txid: Txid::from_byte_array(<[u8; _]>::decode(decoder)?),
        vout: u32::decode(decoder)?,
      },
    })
  }
}

pub struct GeneratedHeightAndOutpointKey {
  pub generated_height: BlockHeight,
  pub outpoint: OutPoint,
}

impl Encode for GeneratedHeightAndOutpointKey {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.generated_height.encode(encoder)?;
    self.outpoint.txid.as_byte_array().encode(encoder)?;
    self.outpoint.vout.encode(encoder)?;
    Ok(())
  }
}

impl<Context> Decode<Context> for GeneratedHeightAndOutpointKey {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    Ok(Self {
      generated_height: BlockHeight::decode(decoder)?,
      outpoint: OutPoint {
        txid: Txid::from_byte_array(<[u8; _]>::decode(decoder)?),
        vout: u32::decode(decoder)?,
      },
    })
  }
}

pub struct SpentHeightAndOutpointKey {
  pub spent_height: BlockHeight,
  pub outpoint: OutPoint,
}

impl Encode for SpentHeightAndOutpointKey {
  fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
    self.spent_height.encode(encoder)?;
    self.outpoint.txid.as_byte_array().encode(encoder)?;
    self.outpoint.vout.encode(encoder)?;
    Ok(())
  }
}

impl<Context> Decode<Context> for SpentHeightAndOutpointKey {
  fn decode<D: Decoder<Context=Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
    Ok(Self {
      spent_height: BlockHeight::decode(decoder)?,
      outpoint: OutPoint {
        txid: Txid::from_byte_array(<[u8; _]>::decode(decoder)?),
        vout: u32::decode(decoder)?,
      },
    })
  }
}

impl TXOStoreWrite for Batch<'_> {
  fn generated_txos<'data>(&mut self, entries: impl IntoParallelIterator<Item = (&'data OutPoint, &'data TXOGenerated)>) {
    let cf_outpoint_to_txo_state = self.store.db.cf_handle("outpoint_to_txo_state").unwrap();
    let cf_locker_script_hash_and_outpoint = self.store.db.cf_handle("locker_script_hash_and_outpoint").unwrap();
    let cf_generated_height_and_outpoint = self.store.db.cf_handle("generated_height_and_outpoint").unwrap();

    let entries = entries
      .into_par_iter()
      .map(|(outpoint, generated)| {
        let key_outpoint_to_txo_state = bincode::encode_to_vec(TXOKey { outpoint: *outpoint }, BIN_KEY).unwrap();
        let value = bincode::encode_to_vec(&TXOUpdate::Generated(generated.clone()), BIN_VALUE).unwrap();
        let key_locker_script_hash_and_outpoint = bincode::encode_to_vec(LockerScriptHashAndOutpointKey {
          locker_script_hash: generated.locker_script_hash,
          outpoint: *outpoint,
        }, BIN_KEY).unwrap();
        let key_generated_height_and_outpoint = bincode::encode_to_vec(GeneratedHeightAndOutpointKey {
          generated_height: generated.generated_height,
          outpoint: *outpoint,
        }, BIN_KEY).unwrap();
        (key_outpoint_to_txo_state, value, key_locker_script_hash_and_outpoint, key_generated_height_and_outpoint)
      })
      .collect_vec_list()
      .into_iter()
      .flatten();

    for (key_outpoint_to_txo_state, value, key_locker_script_hash_and_outpoint, key_generated_height_and_outpoint) in entries {
      self.batch.merge_cf(&cf_outpoint_to_txo_state, key_outpoint_to_txo_state, value);
      self.batch.put_cf(&cf_locker_script_hash_and_outpoint, key_locker_script_hash_and_outpoint, &[]);
      self.batch.put_cf(&cf_generated_height_and_outpoint, key_generated_height_and_outpoint, &[]);
    }
  }

  fn spent_txos<'data>(&mut self, entries: impl IntoParallelIterator<Item = (&'data OutPoint, &'data TXOSpent)>) {
    let cf_outpoint_to_txo_state = self.store.db.cf_handle("outpoint_to_txo_state").unwrap();
    let cf_spent_height_and_outpoint = self.store.db.cf_handle("spent_height_and_outpoint").unwrap();
    let entries = entries
      .into_par_iter()
      .map(|(outpoint, spent)| {
        let key_outpoint_to_txo_state = bincode::encode_to_vec(TXOKey { outpoint: *outpoint }, BIN_KEY).unwrap();
        let value = bincode::encode_to_vec(&TXOUpdate::Spent(spent.clone()), BIN_VALUE).unwrap();
        let key_spent_height_and_outpoint = bincode::encode_to_vec(SpentHeightAndOutpointKey {
          spent_height: spent.spent_height,
          outpoint: *outpoint,
        }, BIN_KEY).unwrap();
        (key_outpoint_to_txo_state, value, key_spent_height_and_outpoint)
      })
      .collect_vec_list()
      .into_iter()
      .flatten();

    for (key, value, key_spent_height_and_outpoint) in entries {
      self.batch.merge_cf(&cf_outpoint_to_txo_state, key, value);
      self.batch.put_cf(&cf_spent_height_and_outpoint, key_spent_height_and_outpoint, &[]);
    }
  }
}
