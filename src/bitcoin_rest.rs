use bitcoin::{consensus, BlockHash};

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

  pub async fn get_headers(&self, from_block_hash: &BlockHash, count: usize) -> anyhow::Result<Vec<bitcoin::block::Header>> {
    let mut binary = self.client.get(format!("{}/rest/headers/{}.bin?count={}", &self.url, from_block_hash, count))
      .send()
      .await?
      .error_for_status()?
      .bytes()
      .await?;

    let mut headers: Vec<bitcoin::block::Header> = Vec::with_capacity(count);

    for _ in 0..count {
      if binary.is_empty() {
        break;
      }

      let (header, size) = consensus::deserialize_partial::<bitcoin::block::Header>(&binary)?;
      if size == 0 {
        anyhow::bail!("Received malformed block header");
      }
      binary = binary.slice(size..);

      headers.push(header);
    }

    if binary.len() > 0 {
      anyhow::bail!("Received more headers than requested");
    }

    Ok(consensus::deserialize(&binary).map_err(
      |e| anyhow::anyhow!("Failed to deserialize block headers: {}", e)
    )?)
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
