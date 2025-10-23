mod batch;
mod layer;
mod fetch;

use std::{convert::Infallible, sync::Arc};
use futures::{StreamExt, TryStreamExt as _, stream};
use tokio::{sync::mpsc, task::{block_in_place, spawn_blocking}};

use crate::{bitcoin_rest::BitcoinRestClient, scanner::{batch::Batch, fetch::Fetcher, layer::Layer}, store::{self, block::BlockStoreRead as _, Store}};


pub struct Scanner {
  bitcoin_client: BitcoinRestClient,
  store: Arc<Store>,
}

impl Scanner {
  pub fn open(
    bitcoin_client: BitcoinRestClient,
    store: Arc<Store>,
  ) -> anyhow::Result<Self> {
    Ok(Self {
      bitcoin_client,
      store,
    })
  }

  pub async fn scan_blocks(&self) -> anyhow::Result<Infallible> {
    let concurrency = num_cpus::get();

    let start_height = block_in_place(||{
      self.store.get_tip_block()
    })?.map_or(0, |(height, _)| height + 1);

    let start_hash = self.bitcoin_client.get_block_hash(start_height).await?;

    let fetcher = Fetcher::new(self.bitcoin_client.clone());

    let headers = fetcher.prefetch_block_headers(start_hash, 100, concurrency);

    let headers = headers.map_ok(|hs| stream::iter(hs.into_iter().map(Ok))).try_flatten();

    let blocks = fetcher.stream_blocks(headers, 2);

    let blocks_heights = blocks
      .zip(stream::iter(start_height..))
      .then(|(block, height): (anyhow::Result<_>, _)| async move {
        let block = block?;
        Ok::<_, anyhow::Error>((block, height))
      });

    let block_chunks = blocks_heights.try_chunks(100);
    let block_chunks = {
      let (sender, mut receiver) = mpsc::channel(concurrency);
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

    let batches = block_chunks.map(|blocks_heights| tokio::task::spawn_blocking(move || {
      let blocks_heights = blocks_heights?;
      let start_height = blocks_heights.first().map(|(_, h)| *h).unwrap();
      tracing::trace_span!("building_batch").in_scope(|| {
        let blocks = {
          let mut try_collect = Vec::new();
          for (block, _) in blocks_heights {
            try_collect.push(block()?);
          }
          try_collect
        };
        Batch::build(start_height, &blocks)
      })
    })).buffered(concurrency);

    let store = self.store.clone();

    tokio::spawn(async move {
      tokio::pin!(batches);

      while let Some(batch) = batches.next().await.transpose()? {
        let batch = batch?;
        let start_height = batch.start_height;
        let end_height = batch.end_height;
        let store = store.clone();
        spawn_blocking(move || {
          tracing::trace_span!("processing_batch").in_scope(|| {
            let mut tx = store::Batch {
              store: &store,
              batch: store.keyspace.batch(),
            };
            let layer = Layer::build(&mut tx, batch)?;
            layer.write()?;
            tracing::trace_span!("commit").in_scope(|| tx.batch.commit().map_err(|e| anyhow::format_err!("Failed to commit batch at height {}: {}", start_height, e)))
          })
        }).await??;

        println!("Scanned blocks up to {}", end_height);
      }
      Ok::<(), anyhow::Error>(())
    }).await??;

    unreachable!();
  }
}

pub async fn scan(store: Arc<Store>, bitcoin_client: BitcoinRestClient) -> anyhow::Result<Infallible> {
  let scanner = Scanner::open(bitcoin_client, store)?;
  scanner.scan_blocks().await?;

  unreachable!();
}
