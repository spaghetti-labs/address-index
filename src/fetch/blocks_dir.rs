use std::path::{PathBuf};

use async_trait::async_trait;
use bitcoin::{BlockHash};

use crate::fetch::{BlockFetcher};

pub struct BlocksDirReader {
  blocks_dir: PathBuf,
}

impl BlocksDirReader {
  pub fn try_open(
    blocks_dir: String,
  ) -> Result<Self, anyhow::Error> {
    let blocks_dir = PathBuf::try_from(&blocks_dir)?;
    Ok(Self { blocks_dir })
  }
}

#[async_trait]
impl BlockFetcher for BlocksDirReader {
  type FetchedBlock = super::rest_api::BlockBytes;

  async fn fetch_block(
    &self,
    block_hash: &BlockHash,
  ) -> anyhow::Result<super::rest_api::BlockBytes> {
    anyhow::bail!(
      "BlocksDirReader::fetch_block is not yet implemented"
    )
  }
}
