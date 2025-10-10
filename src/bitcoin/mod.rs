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
}

#[derive(Debug, serde::Deserialize)]
pub struct BlockchainInfo {
  pub chain: String,
  pub blocks: u64,
}

pub struct BlockHash([u8; 32]);

impl BlockHash {
  pub fn from_hex(s: &str) -> anyhow::Result<Self> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
      return Err(anyhow::anyhow!("Invalid block hash length"));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(Self(arr))
  }
}

impl std::fmt::Debug for BlockHash {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", hex::encode(self.0))
  }
}

impl<'de> serde::Deserialize<'de> for BlockHash {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let s = String::deserialize(deserializer)?;
    BlockHash::from_hex(&s).map_err(serde::de::Error::custom)
  }
}
