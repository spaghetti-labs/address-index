pub mod rest_api;
pub mod blocks_dir;
pub mod combined;

use async_trait::async_trait;
use bitcoin::{block::Header, Block, BlockHash};

#[async_trait]
pub trait BlockFetcher {
  type FetchedBlock: TryInto<Block, Error = anyhow::Error> + Send + 'static;

  async fn fetch_block(
    &self,
    block_hash: &BlockHash,
  ) -> anyhow::Result<Self::FetchedBlock>;
}

#[async_trait]
pub trait HeaderFetcher {
  async fn fetch_headers(
    &self,
    from_block_hash: &BlockHash,
    count: usize,
  ) -> anyhow::Result<Box<dyn Iterator<Item = anyhow::Result<Header>> + Send>>;
}

#[async_trait]
pub trait HashFetcher {
  async fn fetch_hash(
    &self,
    height: u32,
  ) -> anyhow::Result<BlockHash>;
}
