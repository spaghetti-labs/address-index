mod bitcoin;
mod jsonrpc1;
mod store;
mod scanner;

use std::time::Duration;

use clap::Parser;
use tokio::time::sleep;

use crate::scanner::ScanResult;

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
  let jsonrpc1_client = jsonrpc1::RpcClient::new(args.rpc_url);
  let bitcoin_rpc_client = bitcoin::BitcoinRpcClient::new(jsonrpc1_client);

  let blockchain_info = bitcoin_rpc_client.getblockchaininfo().await?;
  println!("Blockchain info: {:?}", blockchain_info);

  let store = store::Store::open(&args.data_dir)?;

  let scanner = scanner::Scanner::open(bitcoin_rpc_client, &store)?;

  let mut hint = scanner::ScanNextBlockHint::default();

  loop {
    if let ScanResult::ProcessedBlock { scan_next_block_hint } = scanner.scan_next_block(&hint).await? {
      hint = scan_next_block_hint;
      continue;
    }

    println!("No new block found...");
    sleep(Duration::from_secs(5)).await;
  }
}