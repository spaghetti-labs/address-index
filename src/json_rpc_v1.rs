use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, serde::Serialize)]
struct RpcRequest<P> {
  pub id: u64,
  pub method: String,
  pub params: P,
}

#[derive(Debug, serde::Deserialize)]
struct RpcResponse<R> {
  pub result: R,
  pub error: Option<serde_json::Value>,
  pub id: u64,
}

pub struct RpcClient {
  next_id: AtomicU64,
  rpc_url: String,
  client: reqwest::Client,
}

impl RpcClient {
  pub fn new(rpc_url: String) -> Self {
    Self {
      next_id: AtomicU64::new(1),
      rpc_url,
      client: reqwest::Client::new(),
    }
  }

  pub async fn request<P: serde::Serialize, R: serde::de::DeserializeOwned>(
    &self,
    method: &str,
    params: P,
  ) -> anyhow::Result<R> {
    let id = self.next_id.fetch_add(1, Ordering::SeqCst);
    let request = RpcRequest {
      id,
      method: method.to_string(),
      params,
    };

    let response = self
      .client
      .post(&self.rpc_url)
      .json(&request)
      .send()
      .await?
      .json::<RpcResponse<R>>()
      .await?;

    if let Some(error) = response.error {
      Err(anyhow::anyhow!("RPC error: {:?}", error))
    } else if response.id != id {
      Err(anyhow::anyhow!(
        "Mismatched response ID: expected {}, got {}",
        id,
        response.id
      ))
    } else {
      Ok(response.result)
    }
  }
}
