use std::hash::{Hash};

use bitcoin::{OutPoint, ScriptBuf};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CollidingOutPoint(OutPoint);

impl Hash for CollidingOutPoint {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    state.write(&self.0.txid.as_raw_hash()[0..8]);
  }
}

impl From<OutPoint> for CollidingOutPoint {
  fn from(outpoint: OutPoint) -> Self {
    Self(outpoint)
  }
}

impl Into<OutPoint> for CollidingOutPoint {
  fn into(self) -> OutPoint {
    self.0
  }
}

impl AsRef<OutPoint> for CollidingOutPoint {
  fn as_ref(&self) -> &OutPoint {
    &self.0
  }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollidingScriptBuf(ScriptBuf);

impl Hash for CollidingScriptBuf {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    let size = self.0.len();
    // first 8 bytes
    state.write(&self.0.as_bytes()[0..size.min(8)]);
    // last 8 bytes
    state.write(&self.0.as_bytes()[..size.saturating_sub(8)]);
  }
}

impl From<ScriptBuf> for CollidingScriptBuf {
  fn from(script: ScriptBuf) -> Self {
    Self(script)
  }
}

impl Into<ScriptBuf> for CollidingScriptBuf {
  fn into(self) -> ScriptBuf {
    self.0
  }
}

impl AsRef<ScriptBuf> for CollidingScriptBuf {
  fn as_ref(&self) -> &ScriptBuf {
    &self.0
  }
}

pub struct XORHasher {
  hash: [u8; 8],
  offset: usize,
}

impl XORHasher {
  pub fn new() -> Self {
    Self { hash: [0; 8], offset: 0 }
  }
}

impl std::hash::Hasher for XORHasher {
  fn finish(&self) -> u64 {
    u64::from_le_bytes(self.hash)
  }

  fn write(&mut self, bytes: &[u8]) {
    for &byte in bytes {
      self.hash[self.offset % 8] ^= byte;
      self.offset += 1;
    }
  }
}

pub struct XORHashBuilder {}

impl XORHashBuilder {
  pub fn new() -> Self {
    Self {}
  }
}

impl std::hash::BuildHasher for XORHashBuilder {
  type Hasher = XORHasher;

  fn build_hasher(&self) -> Self::Hasher {
    XORHasher::new()
  }
}
