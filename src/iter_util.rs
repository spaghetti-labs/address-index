pub trait IterExt: Iterator + Sized {
  fn lag(self) -> LagIter<Self> {
    LagIter {
      iter: self,
      prev_item: None,
    }
  }

  fn order_checked(self) -> impl Iterator<Item = anyhow::Result<Self::Item>>
  where
    Self::Item: Clone + Ord,
  {
    self.lag().map(|(prev, current)| {
      if let Some(prev) = prev {
        if prev > current {
          anyhow::bail!("iterator is not in sorted order");
        }
      }
      Ok(current)
    })
  }

  fn map_ok<T, R, E, F>(self, mut f: F) -> impl Iterator<Item = Result<R, E>>
  where
    Self: Iterator<Item = Result<T, E>>,
    F: FnMut(T) -> Result<R, E>,
  {
    self.map(move |item| match item {
      Ok(value) => f(value),
      Err(e) => Err(e),
    })
  }
}

impl<Iter: Iterator> IterExt for Iter {}

pub struct LagIter<Iter: Iterator> {
  iter: Iter,
  prev_item: Option<Iter::Item>,
}

impl<Iter: Iterator> Iterator for LagIter<Iter>
where
  Iter::Item: Clone,
{
  type Item = (Option<Iter::Item>, Iter::Item);

  fn next(&mut self) -> Option<Self::Item> {
    let current_item = self.iter.next();
    let previous_item = self.prev_item.clone();
    self.prev_item = current_item.clone();
    Some((previous_item, current_item?))
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}
