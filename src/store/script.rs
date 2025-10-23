use bitcoin::{ScriptBuf};

use crate::store::Batch;

use super::{Store};

pub type ScriptID = super::id::ID;

pub trait ScriptStoreRead {
  fn get_script(&self, id: ScriptID) -> anyhow::Result<ScriptBuf>;
  fn get_script_id(&self, script: &ScriptBuf) -> anyhow::Result<Option<ScriptID>>;
}

pub trait ScriptStoreWrite {
  fn use_script_id(&mut self, script: &ScriptBuf) -> anyhow::Result<ScriptID>;
}

impl ScriptStoreRead for Store {
  fn get_script(&self, id: ScriptID) -> anyhow::Result<ScriptBuf> {
    let Some(script) = self.script_id_to_script.get(id.to_be_bytes())? else {
      anyhow::bail!("No script found for ScriptID {}", id);
    };
    Ok(ScriptBuf::from_bytes(script.to_vec()))
  }

  fn get_script_id(&self, script: &ScriptBuf) -> anyhow::Result<Option<ScriptID>> {
    let Some(id) = self.script_to_script_id.get(script)? else {
      return Ok(None);
    };
    Ok(Some(u64::from_be_bytes(id.as_ref().try_into()?)))
  }
}

impl ScriptStoreWrite for Batch<'_> {
  fn use_script_id(&mut self, script: &ScriptBuf) -> anyhow::Result<ScriptID> {
    if let Some(id) = self.store.get_script_id(script)? {
      return Ok(id);
    }
    let id = self.store.id_gen.generate_id();
    self.batch.insert(&self.store.script_to_script_id, script.as_bytes(), id.to_be_bytes());
    Ok(id)
  }
}
