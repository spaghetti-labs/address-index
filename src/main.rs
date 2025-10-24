mod bitcoin_rest;
mod store;
mod scanner;
mod api;
mod hash;
mod sorted_vec;

use std::{convert::Infallible, sync::Arc};
use clap::Parser;
use opentelemetry_otlp::WithExportConfig as _;
use tokio::select;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
use opentelemetry::trace::TracerProvider as _;

use crate::{api::serve, bitcoin_rest::BitcoinRestClient, scanner::scan, store::Store};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(long = "rest-url", env = "REST_URL")]
  rest_url: String,

  #[arg(long = "data-dir", env = "DATA_DIR")]
  data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<Infallible> {
  let args = Args::try_parse()?;

  let bitcoin_client = BitcoinRestClient::new(args.rest_url);

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
    res = scan(store.clone(), bitcoin_client) => res,
    res = serve(store.clone()) => res,
  }?;

  unreachable!();
}
