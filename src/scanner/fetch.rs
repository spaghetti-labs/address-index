use async_stream::try_stream;
use bitcoin::Block;
use futures::{stream, Stream, StreamExt, TryStreamExt as _};
use tokio::{sync::mpsc, task::block_in_place};

use crate::fetch::{BlockFetcher, HeaderFetcher};

pub fn stream_block_header_batches<Fetcher: HeaderFetcher>(
  fetcher: Fetcher,
  start_hash: bitcoin::BlockHash,
  batch_size: usize,
) -> impl Stream<Item = anyhow::Result<Vec<bitcoin::block::Header>>> {
  try_stream! {
    let mut next_hash = start_hash;
    let mut skip_first = false;
    loop {
      let mut headers = fetcher.fetch_headers(&next_hash, batch_size).await?;

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

pub fn prefetch_block_headers<Fetcher: HeaderFetcher + Send + 'static>(
  fetcher: Fetcher,
  start_hash: bitcoin::BlockHash,
  batch_size: usize,
  batch_buffer: usize,
) -> impl Stream<Item = anyhow::Result<bitcoin::block::Header>> {
  let (sender, mut receiver) = mpsc::channel(batch_buffer);
  tokio::spawn(async move {
    let header_batches = stream_block_header_batches(fetcher, start_hash, batch_size);
    tokio::pin!(header_batches);

    while let Some(batch) = header_batches.next().await {
      let batch = match batch {
        Ok(batch) => batch,
        Err(e) => {
          let _ = sender.send(Err(e)).await;
          break;
        }
      };
      if sender.send(Ok(batch)).await.is_err() {
        break;
      }
    }
  });

  stream::poll_fn(move |cx| receiver.poll_recv(cx))
    .map_ok(|batch| {
      stream::iter(batch.into_iter().map(Ok))
    })
    .try_flatten()
}

pub fn stream_blocks<Fetcher: BlockFetcher + Send + 'static + Clone>(
  fetcher: Fetcher,
  header_stream: impl Stream<Item = anyhow::Result<bitcoin::block::Header>>,
  concurrency: usize,
) -> impl Stream<Item = anyhow::Result<Block>> {
  header_stream
    .map(move |header| {
      let fetcher = fetcher.clone();
      tokio::spawn(async move {
        let header = header?;
        let block = fetcher.fetch_block(&block_in_place(|| header.block_hash())).await?;
        let block = block_in_place(|| block.try_into())?;
        Ok(block)
      })
    })
    .map(async |handle| handle.await?)
    .buffered(concurrency)
}
