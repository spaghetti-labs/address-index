use bitcoin::consensus;

use super::json_rpc_v1::RpcClient;

pub struct BitcoinRpcClient {
  rpc_client: RpcClient,
}

impl BitcoinRpcClient {
  pub fn new(rpc_client: RpcClient) -> Self {
    Self {
      rpc_client,
    }
  }

  pub async fn getblockhash(&self, block_height: u64) -> anyhow::Result<bitcoin::BlockHash> {
    self.rpc_client.request("getblockhash", (block_height,)).await
  }

  pub async fn getblock(&self, block_hash: bitcoin::BlockHash) -> anyhow::Result<bitcoin::Block> {
    self.rpc_client.request::<_, HexWrapper>("getblock", (block_hash, /* verbosity: */ 0)).await?.decode()
  }
}

#[derive(serde::Deserialize)]
struct HexWrapper(String);

impl HexWrapper {
  fn decode<T>(&self) -> anyhow::Result<T>
  where
    T: consensus::Decodable,
  {
    let bytes = hex::decode(&self.0)?;
    Ok(consensus::deserialize(&bytes)?)
  }
}
