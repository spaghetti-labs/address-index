use std::{convert::Infallible, iter, str::FromStr, sync::Arc};
use bitcoin::{Amount, ScriptBuf, ScriptHash};
use juniper::{graphql_object, EmptyMutation, EmptySubscription, RootNode};
use rocket::{response::content::RawHtml, routes, State};
use tokio::task::block_in_place;

use crate::store::{block::BlockStoreRead, txo::{TXOState, TXOStoreRead}, BlockHeight, Store};

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
    Ok(ScriptObject { store: self.store, script_hash })
  }
}

struct ScriptObject<'r> {
  store: &'r Store,
  script_hash: ScriptHash,
}

impl<'r> ScriptObject<'r> {
  fn iterate_balance_history(&self) -> anyhow::Result<impl Iterator<Item = (BlockHeight, Amount)> + 'r> {
    let txo_outpoints = block_in_place(||{
      self.store.get_locker_script_txos(&self.script_hash)
    })?.collect::<anyhow::Result<Vec<_>>>()?;

    let mut txos = block_in_place(||{
      self.store.get_txos(txo_outpoints.iter())
    })?.map(
      |txo| {
        let Some(txo) = txo? else {
          anyhow::bail!("missing txo");
        };
        Ok(txo)
      }
    ).collect::<anyhow::Result<Vec<_>>>()?;
    txos.sort_by_key(|txo|txo.generated_height);

    let mut spent_txos = txos.iter().filter(|txo|txo.spent_height.is_some()).cloned().collect::<Vec<_>>();
    spent_txos.sort_by_key(|txo|txo.spent_height.unwrap());

    let mut txos = txos.into_iter().peekable();
    let mut spent_txos = spent_txos.into_iter().peekable();

    let mut balance = Amount::ZERO;
    Ok(iter::from_fn(move || {
      let next_generated_height = txos.peek().map(|txo|txo.generated_height);
      let next_spent_height = spent_txos.peek().map(|txo|txo.spent_height.unwrap());
      let next_height = match (next_generated_height, next_spent_height) {
        (Some(generated_height), Some(spent_height)) => Some(generated_height.min(spent_height)),
        (Some(generated_height), None) => Some(generated_height),
        (None, Some(spent_height)) => Some(spent_height),
        (None, None) => {
          return None;
        },
      }?;

      let prev_balance = balance;

      while let Some(txo) = txos.peek() {
        if txo.generated_height != next_height {
          break;
        }
        let txo = txos.next().unwrap();
        balance += txo.value;
      }
      while let Some(txo) = spent_txos.peek() {
        if txo.spent_height.unwrap() != next_height {
          break;
        }
        let txo = spent_txos.next().unwrap();
        balance -= txo.value;
      }

      if balance == prev_balance {
        return None;
      }

      Some((next_height, balance))
    }))
  }

  fn gen_unspent_txos(&self) -> anyhow::Result<impl Iterator<Item = TXOState> + 'r> {
    let txo_outpoints = block_in_place(||{
      self.store.get_locker_script_txos(&self.script_hash)
    })?.collect::<anyhow::Result<Vec<_>>>()?;
    let unspent_txos = block_in_place(||{
      self.store.get_txos(txo_outpoints.iter())
    })?.map(|txo| {
      let Some(txo) = txo? else {
        anyhow::bail!("missing txo");
      };
      Ok(txo)
    }).filter(|txo|{
      match txo {
        Ok(txo) => txo.spent_height.is_none(),
        _ => true,
      }
    }).collect::<anyhow::Result<Vec<_>>>()?.into_iter();
    Ok(unspent_txos)
  }

  fn recent_balance(&self) -> anyhow::Result<Amount> {
    let unspent_txos = self.gen_unspent_txos()?;
    let mut balance = Amount::ZERO;
    for txo in unspent_txos {
      balance += txo.value;
    }
    Ok(balance)
  }
}

#[graphql_object(rename_all = "none")]
impl<'r> ScriptObject<'r> {
  async fn balance(&self, height: Option<String>) -> anyhow::Result<String> {
    let balance = if let Some(height) = height {
      let height = BlockHeight::from_str(&height)?;
      let history = self.iterate_balance_history()?;
      history.take_while(|(h, _)| *h <= height).map(|(_, balance)| balance).last().unwrap_or(Amount::ZERO)
    } else {
      self.recent_balance()?
    };

    Ok(balance.to_sat().to_string())
  }

  async fn balance_history(&self) -> anyhow::Result<Vec<HistoricalBalance>> {
    Ok(self.iterate_balance_history()?.map(|(height, balance)| HistoricalBalance {
      height,
      balance,
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
