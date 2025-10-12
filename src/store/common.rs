use bincode::config::{standard, BigEndian, Configuration, Fixint, NoLimit};

pub const BINCODE_CONFIG: Configuration<BigEndian, Fixint, NoLimit> = standard()
  .with_big_endian()
  .with_fixed_int_encoding()
  .with_no_limit();

#[macro_export]
macro_rules! impl_bincode_conversion {
  ($type:ty) => {
    impl From<&fjall::Slice> for $type {
      fn from(binary: &fjall::Slice) -> Self {
        bincode::decode_from_slice(binary, crate::store::common::BINCODE_CONFIG).unwrap().0
      }
    }

    impl From<fjall::Slice> for $type {
      fn from(binary: fjall::Slice) -> Self {
        (&binary).into()
      }
    }

    impl From<&$type> for fjall::Slice {
      fn from(value: &$type) -> Self {
        bincode::encode_to_vec(value, crate::store::common::BINCODE_CONFIG).unwrap().into()
      }
    }
  };
}

#[macro_export]
macro_rules! impl_primitive_conversion {
  ($wrapper_type:ty, $primitive_type:ty, $field_name:ident) => {
    impl From<$primitive_type> for $wrapper_type {
      fn from($field_name: $primitive_type) -> Self {
        Self { $field_name }
      }
    }

    impl Into<$primitive_type> for $wrapper_type {
      fn into(self) -> $primitive_type {
        self.$field_name
      }
    }
  };
}

#[macro_export]
macro_rules! impl_hex_debug {
  ($type:ty, $field_name:ident) => {
    impl std::fmt::Debug for $type {
      fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.$field_name))
      }
    }
  };
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct BlockHeight {
  pub height: u64,
}
impl_bincode_conversion!(BlockHeight);
impl_primitive_conversion!(BlockHeight, u64, height);

#[derive(Debug, bincode::Encode, bincode::Decode, Clone, Copy)]
pub struct Amount {
  pub satoshis: u64,
}
impl_bincode_conversion!(Amount);
impl_primitive_conversion!(Amount, u64, satoshis);

#[derive(bincode::Encode, bincode::Decode)]
pub struct BlockHash {
  pub bytes: [u8; 32],
}
impl_bincode_conversion!(BlockHash);
impl_primitive_conversion!(BlockHash, [u8; 32], bytes);
impl_hex_debug!(BlockHash, bytes);

#[derive(bincode::Encode, bincode::Decode)]
pub struct ScriptPubKey {
  pub bytes: [u8; 67],
}
impl_bincode_conversion!(ScriptPubKey);
impl_primitive_conversion!(ScriptPubKey, [u8; 67], bytes);
impl_hex_debug!(ScriptPubKey, bytes);

#[derive(bincode::Encode, bincode::Decode)]
pub struct ScriptPubKeyHash {
  pub bytes: [u8; 20],
}
impl_bincode_conversion!(ScriptPubKeyHash);
impl_primitive_conversion!(ScriptPubKeyHash, [u8; 20], bytes);
impl_hex_debug!(ScriptPubKeyHash, bytes);


#[derive(bincode::Encode, bincode::Decode, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct TransactionID {
  pub bytes: [u8; 32],
}
impl_bincode_conversion!(TransactionID);
impl_primitive_conversion!(TransactionID, [u8; 32], bytes);
impl_hex_debug!(TransactionID, bytes);
