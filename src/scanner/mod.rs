mod batch;
mod fetch;

use std::{convert::Infallible, sync::Arc};
use futures::{StreamExt, TryStreamExt as _, stream};
use tokio::{sync::mpsc, task::{block_in_place, spawn_blocking}};

use crate::{fetch::{BlockFetcher, HashFetcher, HeaderFetcher}, scanner::{batch::Batch, fetch::{prefetch_block_headers, stream_blocks}}, store::{self, block::BlockStoreRead as _, Store}};


pub struct Scanner<Fetcher> {
  fetcher: Fetcher,
  store: Arc<Store>,
}

impl<Fetcher> Scanner<Fetcher> {
  pub fn open(
    fetcher: Fetcher,
    store: Arc<Store>,
  ) -> anyhow::Result<Self> {
    Ok(Self {
      fetcher,
      store,
    })
  }

  pub async fn scan_blocks(&self) -> anyhow::Result<Infallible>
  where
    Fetcher: HeaderFetcher + BlockFetcher + HashFetcher + Clone + Send + 'static,
  {
    let header_batch_buffer_size = num_cpus::get();
    let header_batch_size = 100;
    let block_fetch_concurrency = 2;
    let block_batch_size = 100;
    let block_batch_concurrency = num_cpus::get();

    let start_height = block_in_place(||{
      self.store.get_tip_block()
    })?.map_or(0, |(height, _)| height + 1);

    let start_hash = self.fetcher.fetch_hash(start_height).await?;

    let headers = prefetch_block_headers(self.fetcher.clone(), start_hash, header_batch_size, header_batch_buffer_size);

    let blocks = stream_blocks(self.fetcher.clone(), headers, block_fetch_concurrency);

    let blocks_heights = blocks
      .zip(stream::iter(start_height..))
      .then(|(block, height): (anyhow::Result<_>, _)| async move {
        let block = block?;
        Ok::<_, anyhow::Error>((block, height))
      });

    let block_chunks = blocks_heights.try_chunks(block_batch_size);
    let block_chunks = {
      let (sender, mut receiver) = mpsc::channel(block_batch_concurrency);
      tokio::spawn(async move {
        tokio::pin!(block_chunks);

        while let Some(chunk) = block_chunks.next().await {
          if let Err(_) = sender.send(chunk).await {
            break;
          }
        }
      });
      stream::poll_fn(move |cx| receiver.poll_recv(cx))
    };

    let batches = block_chunks.map(|blocks_heights| tokio::task::spawn_blocking(
      move || tracing::trace_span!("batch").in_scope(|| {
        let blocks_heights = blocks_heights?;
        let start_height = blocks_heights.first().map(|(_, h)| *h).unwrap();
        let blocks = blocks_heights.into_iter().map(|(block, _)| block).collect();
        Batch::build(start_height, blocks)
      })
    )).buffered(block_batch_concurrency);

    let store = self.store.clone();

    tokio::spawn(async move {
      tokio::pin!(batches);

      while let Some(batch) = batches.next().await.transpose()? {
        let batch = batch?;
        let end_height = batch.end_height;
        let store = store.clone();
        spawn_blocking(move || {
          let mut tx = store::Batch {
            store: &store,
            batch: rocksdb::WriteBatch::default(),
          };
          tracing::trace_span!("write").in_scope(|| batch.write(&mut tx))?;
          tracing::trace_span!("commit").in_scope(|| tx.commit())
        }).await??;

        println!("Scanned blocks up to {}", end_height);
      }
      Ok::<(), anyhow::Error>(())
    }).await??;

    unreachable!();
  }
}

pub async fn scan<Fetcher: HeaderFetcher + BlockFetcher + HashFetcher + Clone + Send + 'static>(store: Arc<Store>, fetcher: Fetcher) -> anyhow::Result<Infallible> {
  let scanner = Scanner::open(fetcher, store)?;
  scanner.scan_blocks().await?;

  unreachable!();
}
