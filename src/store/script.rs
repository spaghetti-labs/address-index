use bitcoin::{ScriptBuf};

use super::{TxRead, WriteTx};

pub type ScriptID = super::id::ID;

pub trait ScriptStoreRead {
  fn get_script(&self, id: ScriptID) -> anyhow::Result<ScriptBuf>;
  fn get_script_id(&self, script: &ScriptBuf) -> anyhow::Result<Option<ScriptID>>;
}

pub trait ScriptStoreWrite {
  fn use_script_id(&mut self, script: &ScriptBuf) -> anyhow::Result<ScriptID>;
}

impl<T: TxRead> ScriptStoreRead for T {
  fn get_script(&self, id: ScriptID) -> anyhow::Result<ScriptBuf> {
    let Some(script) = self.get(&self.store().script_id_to_script, id.to_be_bytes())? else {
      anyhow::bail!("No script found for ScriptID {}", id);
    };
    Ok(ScriptBuf::from_bytes(script.to_vec()))
  }

  fn get_script_id(&self, script: &ScriptBuf) -> anyhow::Result<Option<ScriptID>> {
    let Some(id) = self.get(&self.store().script_to_script_id, script)? else {
      return Ok(None);
    };
    Ok(Some(u64::from_be_bytes(id.as_ref().try_into()?)))
  }
}

impl ScriptStoreWrite for WriteTx<'_> {
  fn use_script_id(&mut self, script: &ScriptBuf) -> anyhow::Result<ScriptID> {
    if let Some(id) = self.get_script_id(script)? {
      return Ok(id);
    }
    let id = self.store.id_gen.generate_id();
    self.tx.insert(&self.store.script_to_script_id, script.as_bytes(), id.to_be_bytes());
    Ok(id)
  }
}
