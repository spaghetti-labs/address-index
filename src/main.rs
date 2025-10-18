mod bitcoin_rpc;
mod json_rpc_v1;
mod store;
mod scanner;
mod api;

use std::{convert::Infallible, sync::Arc};
use clap::Parser;
use tokio::select;

use crate::{api::serve, bitcoin_rpc::BitcoinRpcClient, scanner::scan, store::Store};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(long = "rpc-url", env = "RPC_URL")]
  rpc_url: String,

  #[arg(long = "data-dir", env = "DATA_DIR")]
  data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<Infallible> {
  let args = Args::try_parse()?;

  // Configure the client to be more lenient with JSON-RPC version validation
  let json_rpc_v1_client = json_rpc_v1::RpcClient::new(args.rpc_url);
  let bitcoin_rpc_client = BitcoinRpcClient::new(json_rpc_v1_client);

  let store = Arc::new(Store::open(&args.data_dir)?);

  select! {
    res = scan(&store, bitcoin_rpc_client) => res?,
    res = serve(store.clone()) => res?,
  };

  unreachable!();
}

