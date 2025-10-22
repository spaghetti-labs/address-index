use async_stream::try_stream;
use futures::{stream, Stream, StreamExt};
use tokio::{sync::mpsc, task::block_in_place};

use crate::{bitcoin_rest::BitcoinRestClient};

pub struct Fetcher {
  pub client: BitcoinRestClient,
}

impl Fetcher {
  pub fn new(client: BitcoinRestClient) -> Self {
    Self { client }
  }

  pub fn stream_block_headers(
    &self,
    start_hash: bitcoin::BlockHash,
    batch_size: usize,
  ) -> impl Stream<Item = anyhow::Result<Vec<bitcoin::block::Header>>> {
    let client = self.client.clone();
    try_stream! {
      let mut next_hash = start_hash;
      let mut skip_first = false;
      loop {
        let mut headers = client.get_headers(&next_hash, batch_size).await?;

        if skip_first {
          headers.next();
        } else {
          skip_first = true;
        }

        let headers = block_in_place(|| -> anyhow::Result<_> {
          let mut try_collect = Vec::new();
          for header in headers {
            try_collect.push(header?);
          }
          Ok(try_collect)
        })?;

        let Some(last) = headers.last() else {
          break;
        };
        next_hash = last.block_hash();

        yield headers;
      }
    }
  }

  pub fn prefetch_block_headers(
    &self,
    start_hash: bitcoin::BlockHash,
    batch_size: usize,
    buffer_size: usize,
  ) -> impl Stream<Item = anyhow::Result<Vec<bitcoin::block::Header>>> {
    let (sender, mut receiver) = mpsc::channel(buffer_size);
    let header_stream = self.stream_block_headers(start_hash, batch_size);
    tokio::spawn(async move {
      tokio::pin!(header_stream);
      while let Some(headers) = header_stream.next().await {
        let headers = match headers {
          Ok(h) => h,
          Err(e) => {
            let _ = sender.send(Err(e)).await;
            break;
          }
        };
        if let Err(_) = sender.send(Ok(headers)).await {
          break;
        }
      }
    });
    stream::poll_fn(move |cx| receiver.poll_recv(cx))
  }

  pub fn stream_blocks(
    &self,
    header_stream: impl Stream<Item = anyhow::Result<bitcoin::block::Header>>,
    concurrency: usize,
  ) -> impl Stream<Item = anyhow::Result<impl FnOnce() -> anyhow::Result<bitcoin::Block>>> {
    let client = self.client.clone();
    header_stream
      .map(move |header| {
        let client = client.clone();
        tokio::spawn(async move {
          let header = header?;
          let block = client.get_block(&block_in_place(|| header.block_hash())).await?;
          Ok(block)
        })
      })
      .map(async |handle| handle.await?)
      .buffered(concurrency)
  }
}
