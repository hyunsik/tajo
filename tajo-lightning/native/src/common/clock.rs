use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock {
  fn now(&self) -> Duration;
}

pub fn default_clock() -> Box<Clock> {
  Box::new(SystemClock)
}

pub struct SystemClock;

impl Clock for SystemClock {
  fn now(&self) -> Duration {    
    SystemTime::now().duration_since(UNIX_EPOCH).expect("System time is incorrect")
  }
}