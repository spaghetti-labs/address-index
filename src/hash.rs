use std::hash::{Hash};

use bitcoin::{OutPoint, ScriptHash};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CollidingOutPoint(OutPoint);

impl Hash for CollidingOutPoint {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    state.write_u64(
      u64::from_le_bytes(self.0.txid.as_raw_hash()[0..8].try_into().unwrap())
      .wrapping_add(self.0.vout as u64),
    );
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollidingScriptHash(ScriptHash);

impl Hash for CollidingScriptHash {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    state.write_u64(
      u64::from_le_bytes(self.0.as_raw_hash()[0..8].try_into().unwrap()),
    );
  }
}

impl From<ScriptHash> for CollidingScriptHash {
  fn from(hash: ScriptHash) -> Self {
    Self(hash)
  }
}

impl Into<ScriptHash> for CollidingScriptHash {
  fn into(self) -> ScriptHash {
    self.0
  }
}

impl AsRef<ScriptHash> for CollidingScriptHash {
  fn as_ref(&self) -> &ScriptHash {
    &self.0
  }
}

pub struct LazyHasher {
  hash: u64,
}

impl LazyHasher {
  pub fn new() -> Self {
    Self { hash: 0 }
  }
}

impl std::hash::Hasher for LazyHasher {
  fn finish(&self) -> u64 {
    self.hash
  }

  fn write(&mut self, _: &[u8]) {
    panic!("LazyHasher only supports write_u64");
  }

  fn write_u64(&mut self, i: u64) {
    self.hash = i;
  }
}

pub struct LazyHasherBuilder {}

impl LazyHasherBuilder {
  pub fn new() -> Self {
    Self {}
  }
}

impl std::hash::BuildHasher for LazyHasherBuilder {
  type Hasher = LazyHasher;

  fn build_hasher(&self) -> Self::Hasher {
    LazyHasher::new()
  }
}

#[cfg(test)]
mod tests {
  use std::iter;
  use bitcoin::hashes::Hash;
  use bitcoin::{Txid};
  use rand::RngCore;

  #[test]
  fn test_lazy_colliding_outpoint() {
    use super::CollidingOutPoint;
    use bitcoin::OutPoint;
    use std::collections::HashSet;

    let test_data = iter::repeat_with(|| {
      let mut data = [0u8; 32];
      rand::rng().fill_bytes(&mut data);
      OutPoint {
        txid: Txid::from_byte_array(data),
        vout: rand::rng().next_u32() % 100,
      }
    }).take(10_000).collect::<Vec<_>>();

    // measure standard hash insert duration for all data
    let start = std::time::Instant::now();
    let mut default_set = HashSet::new();
    for data in &test_data {
      default_set.insert(data.clone());
    }
    let default_duration = start.elapsed();
    println!("Default hash insert duration: {:?}", default_duration);

    // measure lazy colliding hash insert duration for all data
    let start = std::time::Instant::now();
    let mut lazy_colliding_set = HashSet::with_hasher(super::LazyHasherBuilder::new());
    for data in &test_data {
      lazy_colliding_set.insert(CollidingOutPoint::from(data.clone()));
    }
    let lazy_colliding_duration = start.elapsed();
    println!("Lazy colliding hash insert duration: {:?}", lazy_colliding_duration);

    assert!(lazy_colliding_duration < default_duration, "Lazy colliding hash should be faster than default hash");

    // measure standard hash lookup duration for all data
    let start = std::time::Instant::now();
    for data in &test_data {
      assert!(default_set.contains(data), "Default set should contain the data");
    }
    let default_lookup_duration = start.elapsed();
    println!("Default hash lookup duration: {:?}", default_lookup_duration);

    // measure lazy colliding hash lookup duration for all data
    let start = std::time::Instant::now();
    for data in &test_data {
      assert!(lazy_colliding_set.contains(&CollidingOutPoint::from(data.clone())), "Lazy colliding set should contain the data");
    }
    let lazy_colliding_lookup_duration = start.elapsed();
    println!("Lazy colliding hash lookup duration: {:?}", lazy_colliding_lookup_duration);

    assert!(lazy_colliding_lookup_duration < default_lookup_duration, "Lazy colliding hash should be faster than default hash");
  }
}
