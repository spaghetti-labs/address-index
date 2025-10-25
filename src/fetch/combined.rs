use std::sync::Arc;

use async_trait::async_trait;

use crate::fetch::{blocks_dir::BlocksDirReader, rest_api::{self, BitcoinRestClient}};

pub struct CombinedFetcher {
  pub rest_client: BitcoinRestClient,
  pub blocks_dir: Option<BlocksDirReader>,
}

impl CombinedFetcher {
  pub fn new(
    rest_client: BitcoinRestClient,
    blocks_dir: Option<BlocksDirReader>,
  ) -> Self {
    Self {
      rest_client,
      blocks_dir,
    }
  }
}

#[async_trait]
impl crate::fetch::BlockFetcher for Arc<CombinedFetcher> {
  type FetchedBlock = rest_api::BlockBytes;

  async fn fetch_block(
    &self,
    block_hash: &bitcoin::BlockHash,
  ) -> anyhow::Result<rest_api::BlockBytes> {
    if let Some(blocks_dir) = &self.blocks_dir {
      return blocks_dir.fetch_block(block_hash).await;
    }

    self.rest_client.fetch_block(block_hash).await
  }
}

#[async_trait]
impl crate::fetch::HeaderFetcher for Arc<CombinedFetcher> {
  async fn fetch_headers(
    &self,
    from_block_hash: &bitcoin::BlockHash,
    count: usize,
  ) -> anyhow::Result<Box<dyn Send + Iterator<Item = anyhow::Result<bitcoin::block::Header>>>> {
    self.rest_client.fetch_headers(from_block_hash, count).await
  }
}

#[async_trait]
impl crate::fetch::HashFetcher for Arc<CombinedFetcher> {
  async fn fetch_hash(
    &self,
    height: u32,
  ) -> anyhow::Result<bitcoin::BlockHash> {
    self.rest_client.fetch_hash(height).await
  }
}
