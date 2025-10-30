use bitcoin::{hashes::Hash, Amount, OutPoint, ScriptHash};
use byten::{Decode, Decoder, Encode, Measure, prelude::{EncodeToVec, EncoderToVec as _}, var};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rocksdb::SliceTransform;

use crate::{iter_util::IterExt, store::{Batch, BlockHeight, Store, codec::{AmountCodec, ScriptHashCodec, OutPointCodec}}};

pub fn cf_descriptors(common_opts: &rocksdb::Options) -> Vec<rocksdb::ColumnFamilyDescriptor> {
  let mut outpoint_to_txo_opts = common_opts.clone();
  outpoint_to_txo_opts.set_merge_operator("txo_merge", |key, existing, operands| {
    let mut state = existing.map(|v| TXOState::decode(v, &mut 0).unwrap());
    for op in operands {
      let update = TXOUpdate::decode(op, &mut 0).unwrap();
      match update {
        TXOUpdate::Generated(g) => {
          if let Some(ref state) = state {
            let outpoint: OutPoint = OutPointCodec::Fix.decode(key, &mut 0).unwrap();
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
    Some(state.encode_to_vec().unwrap())
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

#[derive(Copy, Clone, Encode, Decode, Measure)]
pub struct TXOGenerated {
  #[byten(ScriptHashCodec)]
  pub locker_script_hash: ScriptHash,
  #[byten(AmountCodec::Var)]
  pub value: Amount,
  #[byten(var::U32BE)]
  pub generated_height: BlockHeight,
}

#[derive(Copy, Clone, Encode, Decode, Measure)]
pub struct TXOSpent {
  #[byten(var::U32BE)]
  pub spent_height: BlockHeight,
}

#[derive(Copy, Clone, Encode, Decode, Measure)]
#[repr(u8)]
pub enum TXOUpdate {
  Generated(TXOGenerated) = 1,
  Spent(TXOSpent) = 2,
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
      .map(|h| OutPointCodec::Fix.encode_to_vec(h).unwrap())
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
          Ok(Some(TXOState::decode(value.as_ref(), &mut 0)?))
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
      iter.map(|res| -> anyhow::Result<_> {
        let (key, _value) = res?;
        Ok(LockerScriptHashAndOutpoint::decode(key.as_ref(), &mut 0)?)
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

impl TXOStoreWrite for Batch<'_> {
  fn generated_txos<'data>(&mut self, entries: impl IntoParallelIterator<Item = (&'data OutPoint, &'data TXOGenerated)>) {
    let cf_outpoint_to_txo_state = self.store.db.cf_handle("outpoint_to_txo_state").unwrap();
    let cf_locker_script_hash_and_outpoint = self.store.db.cf_handle("locker_script_hash_and_outpoint").unwrap();
    let cf_generated_height_and_outpoint = self.store.db.cf_handle("generated_height_and_outpoint").unwrap();

    let entries = entries
      .into_par_iter()
      .map(|(outpoint, generated)| {
        let key_outpoint_to_txo_state = OutPointCodec::Fix.encode_to_vec(outpoint).unwrap();
        let value = TXOUpdate::Generated(generated.clone()).encode_to_vec().unwrap();
        let key_locker_script_hash_and_outpoint = LockerScriptHashAndOutpoint {
          locker_script_hash: generated.locker_script_hash,
          outpoint: *outpoint,
        }.encode_to_vec().unwrap();
        let key_generated_height_and_outpoint = GeneratedHeightAndOutPoint {
          generated_height: generated.generated_height,
          outpoint: *outpoint,
        }.encode_to_vec().unwrap();
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
        let key_outpoint_to_txo_state = OutPointCodec::Fix.encode_to_vec(outpoint).unwrap();
        let value = TXOUpdate::Spent(spent.clone()).encode_to_vec().unwrap();
        let key_spent_height_and_outpoint = SpentHeightAndOutPoint {
          spent_height: spent.spent_height,
          outpoint: *outpoint,
        }.encode_to_vec().unwrap();
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

#[derive(Clone, Copy, Encode, Decode, Measure)]
pub struct TXOState {
  #[byten(ScriptHashCodec)]
  pub locker_script_hash: ScriptHash,
  #[byten(AmountCodec::Var)]
  pub value: Amount,
  #[byten(var::U32BE)]
  pub generated_height: BlockHeight,
  #[byten(var::Option::<var::U32BE>::default())]
  pub spent_height: Option<BlockHeight>,
}

#[derive(Encode, Decode, Measure)]
pub struct SpentHeightAndOutPoint {
  #[byten(var::U32BE)]
  pub spent_height: BlockHeight,
  #[byten(OutPointCodec::Fix)]
  pub outpoint: OutPoint,
}

#[derive(Encode, Decode, Measure)]
pub struct GeneratedHeightAndOutPoint {
  #[byten(var::U32BE)]
  pub generated_height: BlockHeight,
  #[byten(OutPointCodec::Fix)]
  pub outpoint: OutPoint,
}

#[derive(Encode, Decode, Measure)]
pub struct LockerScriptHashAndOutpoint {
  #[byten(ScriptHashCodec)]
  pub locker_script_hash: ScriptHash,
  #[byten(OutPointCodec::Fix)]
  pub outpoint: OutPoint,
}
