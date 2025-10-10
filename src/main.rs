mod bitcoin;
mod jsonrpc1;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(long = "rpc-url", env = "RPC_URL")]
  rpc_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let args = Args::try_parse()?;

  // Configure the client to be more lenient with JSON-RPC version validation
  let jsonrpc1_client = jsonrpc1::RpcClient::new(args.rpc_url);
  let bitcoin_rpc_client = bitcoin::BitcoinRpcClient::new(jsonrpc1_client);

  let blockchain_info = bitcoin_rpc_client.getblockchaininfo().await?;
  println!("Blockchain info: {:?}", blockchain_info);

  let last_block_hash = bitcoin_rpc_client.getblockhash(blockchain_info.blocks).await?;
  println!("Last block hash: {:?}", last_block_hash);

  let last_block = bitcoin_rpc_client.getblock(last_block_hash).await?;
  println!("Last block: {:?}", last_block);

  Ok(())
}