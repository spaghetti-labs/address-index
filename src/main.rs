mod bitcoin_rpc;
mod json_rpc_v1;
mod store;
mod scanner;
mod script;

use std::{convert::Infallible, sync::Arc, time::Duration};

use clap::Parser;
use juniper::{graphql_object, EmptyMutation, EmptySubscription, RootNode};
use rocket::{response::content::RawHtml, routes, State};
use tokio::{select, task::block_in_place, time::sleep};

use crate::{bitcoin_rpc::BitcoinRpcClient, store::{account::AccountStoreRead, block::BlockStoreRead, Store}};

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

  let blockchain_info = bitcoin_rpc_client.getblockchaininfo().await?;
  println!("Blockchain info: {:?}", blockchain_info);

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
    .manage(Schema::new(
      Query { store },
      EmptyMutation::new(),
      EmptySubscription::new(),
    ))
    .mount(
      "/",
      routes![graphiql, playground, get_graphql, post_graphql],
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

// GET request accepts query parameters like these:
// ?query=<urlencoded-graphql-query-string>
// &operationName=<optional-name>
// &variables=<optional-json-encoded-variables>
// See details here: https://graphql.org/learn/serving-over-http#get-request
#[rocket::get("/graphql?<request..>")]
async fn get_graphql(
  request: juniper_rocket::GraphQLRequest,
  schema: &State<Schema>,
) -> juniper_rocket::GraphQLResponse {
  request.execute(schema, &()).await
}

#[rocket::post("/graphql", data = "<request>")]
async fn post_graphql(
  request: juniper_rocket::GraphQLRequest,
  schema: &State<Schema>,
) -> juniper_rocket::GraphQLResponse {
  request.execute(schema, &()).await
}

type Schema = RootNode<Query, EmptyMutation<()>, EmptySubscription<()>>;

struct Query {
  store: Arc<Store>,
}

#[graphql_object(context=())]
impl Query {
  async fn height(&self) -> anyhow::Result<i32> {
    let tx = self.store.read_tx();
    Ok(block_in_place(||tx.get_tip_block())?.map_or(0, |(height, _)| height.height as i32))
  }

  async fn balance(&self, script: String, height: Option<String>) -> anyhow::Result<String> {
    let tx = self.store.read_tx();
    let script_bytes = hex::decode(script)?;
    let script = store::common::Script::from(script_bytes);
    let balance = match height {
      Some(height) => {
        let height: u64 = height.parse()?;
        let block_height = store::common::BlockHeight { height };
        block_in_place(||tx.get_historical_balance(&script, &block_height))?
      }
      None => {
        block_in_place(||tx.get_recent_balance(&script))?
      }
    };
    Ok(balance.satoshis.to_string())
  }
}
