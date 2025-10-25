mod store;
mod scanner;
mod api;
mod hash;
mod sorted_vec;
mod fetch;

use std::{convert::Infallible, sync::Arc};
use clap::Parser;
use opentelemetry_otlp::WithExportConfig as _;
use tokio::select;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
use opentelemetry::trace::TracerProvider as _;

use crate::{api::serve, fetch::{blocks_dir::BlocksDirReader, combined::CombinedFetcher, rest_api::BitcoinRestClient}, scanner::scan, store::Store};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(long = "rest-url", env = "REST_URL")]
  rest_url: String,

  #[arg(long = "blocks-dir", env = "BLOCKS_DIR")]
  blocks_dir: Option<String>,

  #[arg(long = "data-dir", env = "DATA_DIR")]
  data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<Infallible> {
  let args = Args::try_parse()?;

  let rest_client = BitcoinRestClient::new(args.rest_url);

  let blocks_dir = if let Some(blocks_dir) = args.blocks_dir {
    Some(BlocksDirReader::try_open(blocks_dir)?)
  } else {
    None
  };

  let fetcher = Arc::new(CombinedFetcher::new(rest_client, blocks_dir));

  let store = Arc::new(Store::open(&args.data_dir)?);

  let exporter = opentelemetry_otlp::SpanExporter::builder().with_tonic().with_endpoint("http://localhost:4317").build().unwrap();

  let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
    .with_resource(
      opentelemetry_sdk::Resource::builder()
        .with_service_name("scanner")
        .build(),
    )
    .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
    .with_batch_exporter(exporter)
    .build();

  tracing_subscriber::Registry::default()
    .with(tracing_subscriber::EnvFilter::from_default_env())
    .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("scanner")))
    .init();

  select! {
    res = scan(store.clone(), fetcher) => res,
    res = serve(store.clone()) => res,
  }?;

  unreachable!();
}
