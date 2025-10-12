use bigdecimal::num_bigint;

use crate::jsonrpc1::RpcClient;

pub struct BitcoinRpcClient {
  rpc_client: RpcClient,
}

impl BitcoinRpcClient {
  pub fn new(rpc_client: RpcClient) -> Self {
    Self {
      rpc_client,
    }
  }

  pub async fn getblockchaininfo(&self) -> anyhow::Result<BlockchainInfo> {
    self.rpc_client.request("getblockchaininfo", ()).await
  }

  pub async fn getblockhash(&self, block_height: u64) -> anyhow::Result<BlockHash> {
    self.rpc_client.request("getblockhash", (block_height,)).await
  }

  pub async fn getblock(&self, block_hash: BlockHash) -> anyhow::Result<Block> {
    self.rpc_client.request("getblock", (block_hash, /* verbosity: */ 2u8)).await
  }
}

#[derive(Debug, serde::Deserialize)]
pub struct BlockchainInfo {
  pub chain: String,
  pub blocks: u64,
}

#[derive(Debug, serde::Deserialize)]
pub struct Block {
  pub hash: BlockHash,
  pub previousblockhash: Option<BlockHash>,
  pub nextblockhash: Option<BlockHash>,
  pub height: u64,
  pub time: u64,
  pub tx: Vec<Transaction>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Transaction {
  pub txid: TransactionID,
  pub vin: Vec<TxInput>,
  pub vout: Vec<TxOutput>,
}

pub type BlockHash = Hex<[u8; 32]>;
pub type TransactionID = Hex<[u8; 32]>;

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub enum TxInput {
  Coinbase { coinbase: Hex<Vec<u8>> },
  Normal { txid: TransactionID, vout: u32 },
}

#[derive(Debug, serde::Deserialize)]
pub struct TxOutput {
  pub value: Amount,
  pub n: u32,
  pub scriptPubKey: ScriptPubKey,
}

#[derive(Debug, serde::Deserialize)]
pub struct ScriptPubKey {
  pub address: Option<String>,
}

#[derive(Clone)]
pub struct Hex<T>(pub T);

impl<T: AsRef<[u8]>> std::fmt::Debug for Hex<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", hex::encode(self.0.as_ref()))
  }
}

impl<T: AsRef<[u8]>> serde::Serialize for Hex<T> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    let hex_str = hex::encode(self.0.as_ref());
    serializer.serialize_str(&hex_str)
  }
}

impl<'de, const SIZE: usize> serde::Deserialize<'de> for Hex<[u8; SIZE]> {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let s = String::deserialize(deserializer)?;
    let bytes = hex::decode(s).map_err(serde::de::Error::custom)?;
    if bytes.len() != SIZE {
      return Err(serde::de::Error::custom(format!("Invalid hex length, expected {}, got {}", SIZE, bytes.len())));
    }
    let mut arr = [0u8; SIZE];
    arr.copy_from_slice(&bytes);
    Ok(Self(arr))
  }
}

impl<'de> serde::Deserialize<'de> for Hex<Vec<u8>> {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let s = String::deserialize(deserializer)?;
    let bytes = hex::decode(s).map_err(serde::de::Error::custom)?;
    Ok(Self(bytes))
  }
}

#[derive(Debug)]
pub struct Amount {
  pub satoshis: u64,
}

impl<'de> serde::Deserialize<'de> for Amount {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let btc = bigdecimal::BigDecimal::deserialize(deserializer)?;
    if btc.sign() == num_bigint::Sign::Minus {
      return Err(serde::de::Error::custom("Negative amount"));
    }
    let (satoshis, _) = btc.with_scale(8).into_bigint_and_scale();
    let satoshis: u64 = satoshis.try_into().map_err(|_| serde::de::Error::custom("Amount too large"))?;
    Ok(Amount { satoshis })
  }
}
