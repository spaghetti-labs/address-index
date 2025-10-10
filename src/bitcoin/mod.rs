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
}

#[derive(Debug, serde::Deserialize)]
pub struct BlockchainInfo {
  pub chain: String,
  pub blocks: u64,
}
