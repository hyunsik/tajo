//! API service module
//!
//! This module contains everything about API services, including
//! HTTP API, ways to describe API services, and helper functions 
//! for custom API services

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

use serde_json;
use rocket::{Rocket, Route, State};
use rocket::http::Status;
use rocket::response::{content, status};
use rocket::response::status::Custom;

use common::Result;
use consts::api::service::*;
use common::service::{HasState, Service, ServiceState};

#[derive(Serialize, Deserialize, Clone)]
pub struct ServiceDescription {
  name: String,
  base_path: String,
  version: u32,
  description: Option<String>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ServiceEndpoint {
  pub method: String,
  pub endpoint: String,
}

impl ServiceEndpoint {
  pub fn new(method: &str, endpoint: &str) -> ServiceEndpoint {
    ServiceEndpoint {
      method: method.to_owned(),
      endpoint: endpoint.to_owned()
    }
  }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ServiceEntry {
  name: String,
  path: String,
  version: u32,
  description: String,
  endpoints: Vec<ServiceEndpoint>
}

impl ServiceEntry {
  pub fn new(name: &str, base_path: &str, version: u32, description: &str) -> ServiceEntry {
    ServiceEntry {      
      name: name.to_owned(),
      path: base_path.to_owned(),
      version: version,
      description: description.to_owned(),      
      endpoints: Vec::new()
    }
  }

  pub fn add_all(&mut self, endpoints: Vec<ServiceEndpoint>) {
    endpoints.into_iter().for_each(|e| self.endpoints.push(e));
  }
}

pub type ServiceRegistry = BTreeMap<&'static str, ServiceEntry>;

pub struct ApiService {
  pub state: ServiceState,
  rocket: Option<Rocket>,
  pub registry: Arc<ServiceRegistry>,
  _thread: Option<thread::JoinHandle<()>>,
}

impl HasState for ApiService {
  fn state(&self) -> &ServiceState {
    &self.state
  }
}

pub trait ApiProvider {
  fn register(&self, _builder: ApiServiceBuilder) -> ApiServiceBuilder;
}

pub struct ApiServiceBuilder {
  rocket: Option<Rocket>,
  registry: ServiceRegistry
}

impl ApiServiceBuilder {
  pub fn new() -> Self {
    ApiServiceBuilder {
      rocket: Some(Rocket::ignite()),
      registry: BTreeMap::new()
    }
  }

  pub fn add_api(mut self, name: &'static str, base_uri: &'static str, version: u32, 
      description: &str, routes: Vec<Route>) -> Self {
    self.rocket = Some(_add_api(self.rocket.take().unwrap(), &mut self.registry, name, base_uri, 
      version, description, routes));
    self
  }

  pub fn add_state<T: Send + Sync + 'static>(mut self, state: T) -> Self {
    let rocket = self.rocket.take().unwrap();
    self.rocket = Some(rocket.manage(state));
    self
  }

  pub fn build(mut self) -> ApiService {
    let mut registry = self.registry;
    let mut rocket = self.rocket.take().unwrap();
    rocket = _add_api(rocket, &mut registry, API_NAME, API_PATH, API_VER, API_DESC, routes![services]);
    let registry_arc = Arc::new(registry);
    rocket = rocket.manage(registry_arc.clone());

    ApiService {
      state: ServiceState::new("api-service"),
      rocket: Some(rocket),
      registry: registry_arc,
      _thread: None
    }
  }
}

impl ApiProvider for ApiService {
  fn register(&self, _builder: ApiServiceBuilder) -> ApiServiceBuilder {
    unreachable!("cannot be invoked")
  }
}

impl Service for ApiService {
  fn init(&mut self) -> Result<()> {
    Ok(())
  }
  
  fn start(&mut self) -> Result<()> {
    // move ownerships 
    let rocket = self.rocket.take().unwrap();
    self._thread = Some(thread::spawn(move || {
      rocket.launch();      
    }));    
    Ok(())
  }

  fn stop(&mut self) -> Result<()> {
    unimplemented!()
  }
}

fn _add_api(rocket: Rocket, registry: &mut BTreeMap<&'static str, ServiceEntry>, 
    name: &'static str, base_uri: &'static str, version: u32, 
    description: &str, routes: Vec<Route>) -> Rocket {
  
  let endpoints: Vec<ServiceEndpoint> = routes.iter()
    .map(|r| ServiceEndpoint::new(r.method.as_str(), &[base_uri, r.uri.path()].concat()))
    .collect();
  registry.entry(name)
    .or_insert(ServiceEntry::new(name, base_uri, version, description))
    .add_all(endpoints);
  rocket.mount(base_uri, routes)
}

#[get("/all")]
fn services(state: State<Arc<ServiceRegistry>>) -> content::Json<String> {
  content::Json(serde_json::to_string(state.as_ref()).unwrap())
}

pub static API_MESSAGE_OK: &'static str = "ok";

#[inline]
pub fn ok() -> status::Custom<String> {
  Custom(Status::Ok, API_MESSAGE_OK.to_owned())
}

#[inline]
pub fn ok_with<R>(value: R) -> status::Custom<R> {
  Custom(Status::Ok, value)
}

pub fn err(s: Status, message: &str) -> status::Custom<String> {
  status::Custom(s, message.to_owned())
}