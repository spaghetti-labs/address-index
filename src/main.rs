mod bitcoin_rpc;
mod json_rpc_v1;
mod store;
mod scanner;
mod script;

use std::{convert::Infallible, marker::PhantomData, str::FromStr, sync::Arc, time::Duration};

use clap::Parser;
use juniper::{graphql_object, EmptyMutation, EmptySubscription, GraphQLObject, RootNode};
use rocket::{request::{FromRequest, Outcome}, response::content::RawHtml, routes, Request, State};
use tokio::{select, task::block_in_place, time::sleep};

use crate::{bitcoin_rpc::BitcoinRpcClient, store::{account::AccountStoreRead, block::BlockStoreRead, common::ScriptID, script::TXOStoreRead, Store}};

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

async fn scan(store: &Store, bitcoin_rpc_client: BitcoinRpcClient) -> anyhow::Result<Infallible> {
  let scanner = scanner::Scanner::open(bitcoin_rpc_client, &store)?;
  scanner.scan_blocks().await?;

  unreachable!();
}

async fn serve<'a>(store: Arc<Store>) -> anyhow::Result<Infallible> {
  _ = rocket::build()
    .manage(store)
    .mount(
      "/",
      routes![graphiql, playground, post_graphql],
    )
    .launch()
    .await?;

  unreachable!();
}

#[rocket::get("/graphiql")]
fn graphiql() -> RawHtml<String> {
  juniper_rocket::graphiql_source("/graphql", None)
}

#[rocket::get("/playground")]
fn playground() -> RawHtml<String> {
  juniper_rocket::playground_source("/graphql", None)
}

#[rocket::post("/graphql", data = "<request>")]
async fn post_graphql<'r>(
  request: juniper_rocket::GraphQLRequest,
  store: &'r State<Arc<Store>>,
) -> juniper_rocket::GraphQLResponse {
  let tx = store.read_tx();
  request.execute(&Schema::new(Query { tx: &tx }, EmptyMutation::new(), EmptySubscription::new()), &()).await
}

type Schema<'r> = RootNode<Query<'r>, EmptyMutation<()>, EmptySubscription<()>>;

struct Query<'r> {
  tx: &'r store::ReadTx<'r>,
}

#[graphql_object(rename_all = "none")]
impl<'r> Query<'r> {
  async fn height(&self) -> anyhow::Result<i32> {
    Ok(block_in_place(||self.tx.get_tip_block())?.map_or(0, |(height, _)| height.height as i32))
  }

  async fn locker_script(&self, hex: Option<String>, address: Option<String>) -> anyhow::Result<ScriptObject<'r>> {
    let script_bytes = match (hex, address) {
      (Some(hex), None) => hex::decode(hex)?,
      (None, Some(address)) => {
        let address = bitcoin::Address::from_str(&address)?.require_network(bitcoin::Network::Bitcoin)?;
        address.script_pubkey().into_bytes()
      }
      _ => return Err(anyhow::anyhow!("either hex or address must be provided")),
    };
    let script = store::common::Script::from(script_bytes);
    let script_id = block_in_place(||self.tx.get_script_id(&script))?;
    Ok(ScriptObject { tx: self.tx, script_id })
  }
}

struct ScriptObject<'r> {
  tx: &'r store::ReadTx<'r>,
  script_id: Option<ScriptID>,
}

#[graphql_object(rename_all = "none")]
impl<'r> ScriptObject<'r> {
  async fn balance(&self, height: Option<String>) -> anyhow::Result<String> {
    let Some(script_id) = self.script_id else {
      return Ok("0".to_string());
    };
    let balance = match height {
      Some(height) => {
        let height: u64 = height.parse()?;
        let block_height = store::common::BlockHeight { height };
        block_in_place(||self.tx.get_historical_balance(script_id, block_height))?
      }
      None => {
        block_in_place(||self.tx.get_recent_balance(script_id))?
      }
    };
    Ok(balance.satoshis.to_string())
  }

  async fn balance_history(&self) -> anyhow::Result<Vec<HistoricalBalance>> {
    let Some(script_id) = self.script_id else {
      return Ok(vec![]);
    };
    let historical_balances = block_in_place(|| self.tx.get_balance_history(script_id))?;
    Ok(historical_balances.iter().map(|(height, amount)| HistoricalBalance {
      height: height.height.to_string(),
      balance: amount.satoshis.to_string(),
    }).collect())
  }
}

#[derive(GraphQLObject)]
#[graphql(rename_all = "none")]
struct HistoricalBalance {
  pub height: String,
  pub balance: String,
}
