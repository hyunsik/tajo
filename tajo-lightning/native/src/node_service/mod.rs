//! Discovery Service
//! 
//! It allows cluster nodes to advertise their services and 
//! continue to manage node informations.

use std::fmt;
use std::rc::Rc;
use std::string::ToString;
use std::sync::Arc;

use serde_json;
use rocket::{State};
use rocket::http::Status;
use rocket::response::status::{Custom};

use ::err::Result;
use driver::{NodeId, DriverContext};
use consts::api::node::*;
use api_service as api;
use api_service::{ApiProvider, ApiServiceBuilder};
use common::service::{Service, HasState, ServiceState};

pub struct NodeService {
  state: ServiceState,
  node: Arc<Node>
}

impl HasState for NodeService {
  fn state(&self) -> &ServiceState {
    &self.state 
  }
}

impl ApiProvider for NodeService {
  fn register(&self, builder: ApiServiceBuilder) -> ApiServiceBuilder {    
    builder
      .add_state(self.node.clone())
      .add_api(API_NAME, API_PATH, API_VER, API_DESC, routes![status])
  }
}

impl NodeService {
  pub fn new(context: Rc<DriverContext>) -> NodeService {
    NodeService {
      state: ServiceState::new(API_NAME),
      node: Arc::new(Node::new(&context.node_id, "localhost", 8000u16)),
    }
  }
}

impl Service for NodeService {
  fn init(&mut self) -> Result<()> {
    Ok(()) // nothing to do
  }
  
  fn start(&mut self) -> Result<()> {
    Ok(()) // nothing to do
  }

  fn stop(&mut self) -> Result<()> {
    Ok(()) // nothing to do
  }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Node {
  pub id: NodeId,
  pub host: String,
  pub port: u16,
  pub state: NodeStatus
}

impl Node {
  pub fn new(id: &str, host: &str, port: u16) -> Node {
    Node {
      id: id.to_string(),
      host: host.to_string(),
      port: port,
      state: NodeStatus::ACTIVE
    }
  }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
  ACTIVE,
  INACTIVE 
}

static ACTIVE: &'static str = "ACTIVE";
static INACTIVE: &'static str = "INACTIVE";

impl NodeStatus {
  pub fn as_str(&self) -> &'static str {
    match *self {
      NodeStatus::ACTIVE => ACTIVE,
      NodeStatus::INACTIVE => INACTIVE
    }
  }
}

impl fmt::Display for NodeStatus {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {    
    f.write_str(self.as_str())
  }
}

// #[post("/", format = "application/json", data = "<req>")]
// fn join(nodes: State<Arc<NodeManager>>, req: Json<JoinRequest>) -> Custom<String> {
//   let id = req.id.clone();
//   let node = Node {
//     id: id.clone(),
//     host: req.host.clone(),
//     port: req.port,
//     state: NodeState(status: NodeStatus::ACTIVE)
//   };

//   nodes.inner().join(id, node)
//     .map(|_| api::ok())
//     .map_err(|e| api::err(Status::InternalServerError, &e.to_string()))
//     .unwrap()
// }

// #[delete("/<id>")]
// fn leave(nodes: State<Arc<NodeManager>>, id: NodeId) -> Custom<String> {
//   match nodes.inner().leave(&id) {
//     Ok(()) => api::ok(),
//     Err(Error(NodeNotFound, ref msg)) => api::err(Status::NoContent, msg),
//     Err(e) => api::err(Status::InternalServerError, &e.to_string())
//   }
// }

//#[get("/<id>", format = "application/json")]
//fn get(nodes: State<Arc<NodeManager>>, id: NodeId) 
//    -> ::std::result::Result<Json<Node>, NotFound<String>> {
//  nodes.inner().get(&id)
//    .map(|n| Json(n))
//    .map_err(|e| NotFound(e.to_string()))
//}

//#[get("/status", format = "application/json")]
#[get("/status")]
fn status(node: State<Arc<Node>>) -> ::std::result::Result<String, Custom<String>> {
  serde_json::to_string(node.as_ref())
  .map(|n| n)
  .map_err(|e| api::err(Status::InternalServerError, &e.to_string()))
}

#[test]
fn test_json() {
  // let jq = JoinRequest {
  //   id: "AAA".to_owned(),
  //   host: String::from("localhost"),
  //   port: 32u16
  // };

  let id = String::from("xxxx");
  let n = Node {
    id: id.clone(),
    host: String::from("localhost"),
    port: 32u16,
    state: NodeState {status: NodeStatus::ACTIVE}
  };
  println!("{}", ::serde_json::to_string_pretty(&n).unwrap());
}