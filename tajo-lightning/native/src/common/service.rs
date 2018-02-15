use std::sync::{Arc, RwLock};

use common::err::Result;
use api_service::{ApiProvider};

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd)]
pub enum Status {
  /// Not initalized
  NOTINITED = 0,

  /// Initialized but not started or stopped
  INITED = 1,

  /// started and not stopped
  STARTED = 2,

  /// stopped. No further state transitions are permitted
  STOPPED = 3,
}

/// State Machine of Service Trait
static STATE_MACHINE: [[i32; 4];4] = [
  // Row Idx = Current Status, Column Idx = Next Status
  [1, 1, 0, 1], // NOTINITED -> (NOTINITED | INITED | STOPPED)
  [0, 0, 1, 1], // INITED -> (STARTED | STOPPED)
  [0, 0, 0, 1], // STARTED -> STOPPED
  [0, 0, 0, 1], // STOPPED -> STOPPED
];

pub struct ServiceState {
  name: String,
  status: RwLock<Status>,
}

impl ServiceState {
  pub fn new(name: &str) -> ServiceState {
    ServiceState {
      name: name.to_owned(),
      status: RwLock::new(Status::NOTINITED),
    }
  }

  pub fn name(&self) -> &str {
    &self.name
  }

  pub fn status(&self) -> Status {
    *self.status.read().unwrap()
  }

  pub fn transite(&self, to: Status) {
    let lock = self.status.write().ok();    
    let mut current = lock.unwrap();   
    
    if STATE_MACHINE[*current as usize][to as usize] != 1 {
      panic!("invalid transition in {} from {:?} to {:?}", &self.name, *current, to)
    }
    
    debug!("The state of {} is transited from {:?} to {:?}", &self.name, *current, to);
    *current = to;
  }
}

pub trait HasState {
  fn state(&self) -> &ServiceState;
}

pub trait Service: HasState + ApiProvider + Send + Sync {
  fn init(&mut self) -> Result<()>;
  
  fn start(&mut self) -> Result<()>;

  fn stop(&mut self) -> Result<()>;

  fn assert_status(&self, status: Status) {    
    if self.state().status() != status {
      panic!("The current status of '{}' is {:?}. It must be {:?}", self.name(), self.state().status(), status)
    }
  }

  /// an internal function
  fn _init(&mut self) -> Result<()> {
    debug!("Initializing {} service ... ", self.name());
    self.assert_status(Status::NOTINITED);
    self.init()?;
    self.state().transite(Status::INITED);
    info!("{} service initialized", self.name());
    Ok(())
  }

  fn _start(&mut self) -> Result<()> {
    debug!("Starting {} service ... ", self.name());
    self.assert_status(Status::INITED);
    self.start()?;
    self.state().transite(Status::STARTED);
    info!("{} service started", self.name());
    Ok(())
  }

  fn _stop(&mut self) -> Result<()> { 
    debug!("Stopping {} service ...", self.name());
    if *self.state().status.read().unwrap() == Status::STOPPED {
      return Ok(())
    }

    self.stop()?;
    self.state().transite(Status::STOPPED);
    info!("{} service stopped", self.name());
    Ok(())
  }

  fn name(&self) -> &str {
    &self.state().name
  }

  fn status(&self) -> Status {
    *self.state().status.read().unwrap()
  }
}

pub struct ServiceManager {
  service_state: ServiceState,
  services: Vec<Arc<RwLock<Box<Service>>>>
}

impl HasState for ServiceManager {
  fn state(&self) -> &ServiceState {
    &self.service_state
  }
}

impl ServiceManager {
  pub fn new(name: &str) -> ServiceManager {
    ServiceManager {
      service_state: ServiceState::new(name),
      services: Vec::new()
    }
  }

  pub fn name(&self) -> &str {
    self.service_state.name()
  }

  pub fn add(&mut self, service: Box<Service>) {
    self.services.push(Arc::new(RwLock::new(service)))
  }

  pub fn add_all(&mut self, services: Vec<Box<Service>>) {    
    services.into_iter().map(|s| Arc::new(RwLock::new(s))).for_each(|s| self.services.push(s))
  }

  pub fn services(&self) -> &[Arc<RwLock<Box<Service>>>] {
    &self.services
  }

  fn assert_status(&self, status: Status) {    
    if self.state().status() != status {
      panic!("The current status of '{}' is {:?}. It must be {:?}", self.name(), self.state().status(), status)
    }
  }

  pub fn init(&mut self) -> Result<()> {
    debug!("Initializing registred services ... ");
    self.assert_status(Status::NOTINITED);    
    for s in self.services().iter() {
      s.write().unwrap()._init()?;
    }
    self.state().transite(Status::INITED);
    info!("All registered services initialized");
    Ok(())
  }

  pub fn start(&mut self) -> Result<()> {
    debug!("Starting registred service ... ");
    self.assert_status(Status::INITED);
    for s in self.services().iter() {
      s.write().unwrap()._start()?;
    }
    self.state().transite(Status::STARTED);
    info!("All registered service started");
    Ok(())
  }

  pub fn stop(&mut self) -> Result<()> {
    debug!("Stopping {} service ...", self.name());
    if *self.state().status.read().unwrap() == Status::STOPPED {
      return Ok(())
    }

    for s in self.services().iter() {
      s.write().unwrap()._stop()?;
    }
    self.state().transite(Status::STOPPED);
    info!("{} service stopped", self.name());
    Ok(())
  }
}