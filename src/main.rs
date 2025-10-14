mod bitcoin_rpc;
mod json_rpc_v1;
mod store;
mod scanner;
mod script;

use std::time::Duration;

use clap::Parser;
use tokio::time::sleep;

use crate::{bitcoin_rpc::BitcoinRpcClient};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(long = "rpc-url", env = "RPC_URL")]
  rpc_url: String,

  #[arg(long = "data-dir", env = "DATA_DIR")]
  data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let args = Args::try_parse()?;

  // Configure the client to be more lenient with JSON-RPC version validation
  let json_rpc_v1_client = json_rpc_v1::RpcClient::new(args.rpc_url);
  let bitcoin_rpc_client = BitcoinRpcClient::new(json_rpc_v1_client);

  let blockchain_info = bitcoin_rpc_client.getblockchaininfo().await?;
  println!("Blockchain info: {:?}", blockchain_info);

  let store = store::Store::open(&args.data_dir)?;

  let scanner = scanner::Scanner::open(bitcoin_rpc_client, &store)?;

  scanner.scan_blocks().await?;

  Ok(())
}