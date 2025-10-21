use async_stream::try_stream;
use bitcoin::{consensus, BlockHash};
use futures::Stream;

pub struct BitcoinRestClient {
  client: reqwest::Client,
  url: String,
}

impl BitcoinRestClient {
  // See: https://github.com/bitcoin/bitcoin/blob/0eeae4d174a41c3fc2eae41e76b929fa3114d6f3/doc/REST-interface.md

  pub fn new(
    url: String,
  ) -> Self {
    Self {
      client: reqwest::Client::new(),
      url,
    }
  }

  pub async fn get_block(&self, block_hash: &BlockHash) -> anyhow::Result<bitcoin::Block> {
    let binary = self.client.get(format!("{}/rest/block/{}.bin", &self.url, block_hash))
      .send()
      .await?
      .error_for_status()?
      .bytes()
      .await?;

    Ok(consensus::deserialize(&binary).map_err(
      |e| anyhow::anyhow!("Failed to deserialize block: {}", e)
    )?)
  }

  pub async fn get_headers<'a>(&'a self, from_block_hash: &'a BlockHash, count: usize) -> impl Stream<Item = anyhow::Result<bitcoin::block::Header>> + 'a {
    try_stream! {
      let mut binary = self.client.get(format!("{}/rest/headers/{}.bin?count={}", &self.url, from_block_hash, count))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

      for _ in 0..count {
        if binary.is_empty() {
          break;
        }

        let (header, size) = consensus::deserialize_partial::<bitcoin::block::Header>(&binary)?;
        if size == 0 {
          Err(anyhow::format_err!("Received malformed block header"))?;
        }
        binary = binary.slice(size..);

        yield header;
      }

      if binary.len() > 0 {
        Err(anyhow::format_err!("Received more headers than requested"))?;
      }
    }
  }

  pub async fn get_block_hash(&self, height: u32) -> anyhow::Result<bitcoin::BlockHash> {
    let binary = self.client.get(format!("{}/rest/blockhashbyheight/{}.bin", &self.url, height))
      .send()
      .await?
      .error_for_status()?
      .bytes()
      .await?;

    Ok(consensus::deserialize(&binary).map_err(
      |e| anyhow::anyhow!("Failed to deserialize block hash: {}", e)
    )?)
  }
}
