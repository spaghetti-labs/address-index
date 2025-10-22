use std::sync::atomic::AtomicU64;

pub struct IDGenerator {
  next_id: AtomicU64,
}

impl IDGenerator {
  pub fn new() -> Self {
    let start_id = {
      let time_component = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();
      let pid_component = std::process::id() as u128;
      let combined = time_component.wrapping_mul(pid_component) ^ pid_component;
      combined as u64
    };
    Self { next_id: AtomicU64::new(start_id) }
  }

  pub fn generate_id(&self) -> ID {
    self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
  }
}

pub type ID = u64;
