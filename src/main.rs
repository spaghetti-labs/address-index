mod bitcoin;
mod jsonrpc1;
mod store;
mod scanner;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(long = "rpc-url", env = "RPC_URL")]
  rpc_url: String,

  #[arg(long = "data-dir", env = "DATA_DIR")]
  data_dir: String,

  #[arg(long = "start-height", env = "START_HEIGHT", default_value_t = 0)]
  start_height: u64,
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

  let scanner = scanner::Scanner::open(bitcoin_rpc_client, store)?;

  scanner.scan_blocks(args.start_height).await?;

  Ok(())
}