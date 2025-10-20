mod bitcoin_rest;
mod store;
mod scanner;
mod api;

use std::{convert::Infallible, sync::Arc};
use clap::Parser;
use tokio::select;

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

  select! {
    res = scan(&store, bitcoin_client) => res?,
    res = serve(store.clone()) => res?,
  };

  unreachable!();
}

