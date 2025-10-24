#[derive(Debug, Clone)]
pub struct SortedVec<T> {
  vec: Vec<T>,
}

impl<T: Ord> SortedVec<T> {
  pub fn empty() -> Self {
    Self { vec: Vec::new() }
  }

  pub fn ingest(items: Vec<T>) -> Self {
    assert!(items.windows(2).all(|w| w[0] < w[1]), "Items are not sorted");
    Self { vec: items }
  }

  pub fn append(&mut self, item: T) {
    if let Some(last) = self.vec.last() {
      assert!(item > *last, "Item to append is not greater than last item");
    }
    self.vec.push(item);
  }

  pub(crate) fn is_empty(&self) -> bool {
    self.vec.is_empty()
  }
}

impl<T> AsRef<[T]> for SortedVec<T> {
  fn as_ref(&self) -> &[T] {
    &self.vec
  }
}

impl<T> Into<Vec<T>> for SortedVec<T> {
  fn into(self) -> Vec<T> {
    self.vec
  }
}

#[derive(Debug, Clone)]
pub struct SortedMap<K, V> {
  sorted_vec: SortedVec<SortedEntry<K, V>>,
}

#[derive(Debug, Clone)]
pub struct SortedEntry<K, V> {
  pub key: K,
  pub value: V,
}

impl<K: Eq, V> Eq for SortedEntry<K, V> {}

impl<K: PartialEq, V> PartialEq for SortedEntry<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key
  }
}

impl<K: Ord, V> Ord for SortedEntry<K, V> {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.key.cmp(&other.key)
  }
}

impl<K: PartialOrd, V> PartialOrd for SortedEntry<K, V> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    self.key.partial_cmp(&other.key)
  }
}

impl<K: Ord, V> SortedMap<K, V> {
  pub fn empty() -> Self {
    Self {
      sorted_vec: SortedVec::empty(),
    }
  }

  pub fn ingest(entries: Vec<SortedEntry<K, V>>) -> Self {
    Self {
      sorted_vec: SortedVec::ingest(entries),
    }
  }

  pub fn append(&mut self, key: K, value: V) {
    self.sorted_vec.append(SortedEntry { key, value });
  }

  pub fn get(&self, key: &K) -> Option<&V>
  where
    K: Ord,
  {
    let slice = self.sorted_vec.as_ref();
    match slice.binary_search_by(|entry| entry.key.cmp(key)) {
      Ok(index) => Some(&slice[index].value),
      Err(_) => None,
    }
  }

  pub fn remove(&mut self, key: &K) -> Option<V>
  where
    K: Ord,
  {
    let slice = self.sorted_vec.as_ref();
    match slice.binary_search_by(|entry| entry.key.cmp(key)) {
      Ok(index) => Some(self.sorted_vec.vec.remove(index).value),
      Err(_) => None,
    }
  }
  
  pub(crate) fn is_empty(&self) -> bool {
    self.sorted_vec.as_ref().is_empty()
  }
}

impl<K, V> AsRef<[SortedEntry<K, V>]> for SortedMap<K, V> {
  fn as_ref(&self) -> &[SortedEntry<K, V>] {
    self.sorted_vec.as_ref()
  }
}
