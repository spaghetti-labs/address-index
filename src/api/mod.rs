use std::{convert::Infallible, str::FromStr, sync::Arc};
use bitcoin::{Amount, ScriptBuf};
use juniper::{graphql_object, EmptyMutation, EmptySubscription, RootNode};
use rocket::{response::content::RawHtml, routes, State};
use tokio::task::block_in_place;

use crate::{sorted_vec::SortedEntry, store::{account::{AccountState, AccountStoreRead}, block::BlockStoreRead, BlockHeight, Store}};

pub async fn serve<'a>(store: Arc<Store>) -> anyhow::Result<Infallible> {
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
  request.execute(&Schema::new(Query { store }, EmptyMutation::new(), EmptySubscription::new()), &()).await
}

type Schema<'r> = RootNode<Query<'r>, EmptyMutation<()>, EmptySubscription<()>>;

struct Query<'r> {
  store: &'r Store,
}

#[graphql_object(rename_all = "none")]
impl<'r> Query<'r> {
  async fn height(&self) -> anyhow::Result<i32> {
    Ok(block_in_place(||self.store.get_tip_block())?.map_or(0, |(height, _)| height as i32))
  }

  async fn locker_script(&self, hex: Option<String>, address: Option<String>) -> anyhow::Result<ScriptObject> {
    let script_bytes = match (hex, address) {
      (Some(hex), None) => hex::decode(hex)?,
      (None, Some(address)) => {
        let address = bitcoin::Address::from_str(&address)?.require_network(bitcoin::Network::Bitcoin)?;
        address.script_pubkey().into_bytes()
      }
      _ => return Err(anyhow::anyhow!("either hex or address must be provided")),
    };
    let script = ScriptBuf::from_bytes(script_bytes.clone());
    let script_hash = script.script_hash();
    let account_state = block_in_place(|| self.store.get_account_state(&script_hash))?;
    Ok(ScriptObject { account_state })
  }
}

struct ScriptObject {
  account_state: AccountState,
}

#[graphql_object(rename_all = "none")]
impl ScriptObject {
  async fn balance(&self, height: Option<String>) -> anyhow::Result<String> {
    let balance = if let Some(height) = height {
      let height = BlockHeight::from_str(&height)?;
      self.account_state.balance_history.as_ref().iter()
        .filter(|SortedEntry { key, .. }| *key <= height)
        .map(|SortedEntry { value, .. }| value)
        .last()
        .cloned()
        .unwrap_or(Amount::ZERO)
    } else {
      self.account_state.recent_balance
    };

    Ok(balance.to_sat().to_string())
  }

  async fn balance_history(&self) -> anyhow::Result<Vec<HistoricalBalance>> {
    Ok(self.account_state.balance_history.as_ref().iter().map(|SortedEntry { key: height, value: amount }| HistoricalBalance {
      height: *height,
      balance: *amount,
    }).collect())
  }
}

struct HistoricalBalance {
  pub height: BlockHeight,
  pub balance: Amount,
}

#[graphql_object(rename_all = "none")]
impl HistoricalBalance {
  pub fn height(&self) -> String {
    self.height.to_string()
  }

  pub fn balance(&self) -> String {
    self.balance.to_sat().to_string()
  }
}
