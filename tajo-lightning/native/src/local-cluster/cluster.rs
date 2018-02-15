#![allow(dead_code)]

extern crate flang;
extern crate uuid;

use std::collections::{BTreeMap, BTreeSet};
use std::iter::Iterator;

use self::uuid::Uuid;

use self::flang::common::{Result};
use self::flang::config::Config;
use self::flang::driver::{Driver, RunEnv};


pub struct LocalClusterBuilder {
  cluster_id: Uuid,
  nodes: BTreeMap<Uuid, Driver>
}

impl LocalClusterBuilder {
  pub fn new() -> Self {
    LocalClusterBuilder {
      cluster_id: Uuid::new_v4(),
      nodes: BTreeMap::new()
    }
  }

  pub fn add_node(&mut self, config: Config) -> &mut Self {    
    let driver = Driver::new(RunEnv::with(config.clone(), false).unwrap());
    let role = if config.coordinator.enabled == true {
      "coordinator"
    } else {
      "worker"
    };
    println!("adding {} {} to the cluster ({})", role, driver.uuid(), &self.cluster_id);
    self.nodes.insert(driver.uuid().clone(), driver);
    self
  }

  pub fn add_workers(&mut self, num: u16, config: Option<Config>) -> &mut Self {
    //let mut self_instance = self;
    let cloned = config.unwrap();
    //cloned.node.http.listen   
    for _ in 0..num {
      self.add_node(cloned.clone());
    }
    self
  }

  pub fn build(self) -> LocalCluster {
    let coordinators: BTreeSet<Uuid> = self.nodes.iter()
      .filter(|&(_, v)| v.is_coordinator())
      .map(|(k, _)| k.clone())
      .collect();

    if coordinators.is_empty() {
      panic!("A cluster instance requires at least one coordinator.");
    }

    if coordinators.len() > 1 {
      panic!("Multiple coordinators are not supported yet.");
    }

    LocalCluster {
      coordinators: coordinators,
      nodes: self.nodes
    }
  }
}

pub struct LocalCluster {
  coordinators: BTreeSet<Uuid>,
  nodes: BTreeMap<Uuid, Driver>
}

impl LocalCluster {
  pub fn startup(&mut self) -> Result<()> {    
    self.start_coordinators()?;
    self.start_workers()
  }

  pub fn shutdown(&mut self) -> Result<()> {
    self.stop_workers()?;
    self.stop_coordinators()
  }

  pub fn coordinators<'a>(&'a self) -> Box<Iterator<Item=&Uuid> + 'a> {
    Box::new(self.coordinators.iter())
  }

  pub fn workers<'a>(&'a self) -> Box<Iterator<Item=&Uuid> + 'a> {
    Box::new(self.nodes.keys().filter(move |u| !self.coordinators.contains(u)))
  }

  pub fn start_coordinators(&mut self) -> Result<()> {    
    let coordinators: Vec<Uuid> = self.coordinators().cloned().collect();
    self.start_nodes(&coordinators)
  }

  pub fn stop_coordinators(&mut self) -> Result<()> {
    let coordinators: Vec<Uuid> = self.coordinators().cloned().collect();
    self.stop_nodes(&coordinators)
  }

  pub fn start_workers(&mut self) -> Result<()> {
    let workers: Vec<Uuid> = self.workers().cloned().collect();
    self.start_nodes(&workers)
  }

  pub fn stop_workers(&mut self) -> Result<()> {
    let workers: Vec<Uuid> = self.workers().cloned().collect();
    self.stop_nodes(&workers)
  }

  pub fn start_nodes(&mut self, node_ids: &[Uuid]) -> Result<()> {
    println!("start_nodes enter");
    for uuid in node_ids {
      match self.nodes.get_mut(uuid) {
        Some(d) => d.startup()?,
        None => panic!("Couldn't find node {}", uuid)
      }
    }
    println!("start_nodes leave");
    Ok(())
  }

  pub fn stop_nodes(&mut self, node_ids: &[Uuid]) -> Result<()> {
    for uuid in node_ids {
      match self.nodes.get_mut(uuid) {
        Some(d) => d.shutdown()?,
        None => panic!("Couldn't find node {}", uuid)
      }
    }
    Ok(())
  }
  
  fn start_node(&mut self, nodeid: &Uuid) -> Result<()> {
    self.start_nodes(&vec![nodeid.clone()])
  }
  
  fn stop_node(&mut self, nodeid: &Uuid) -> Result<()> {
    self.stop_nodes(&vec![nodeid.clone()])
  }
}

pub fn main() {
  let coord_config = Config::from_str(r#"
[coordinator]
enabled = true
uri="http://localhost:8080"

[node]

[node.http]
listen="127.0.0.1:8080"

[node.executor]
data_dir="/tmp/flang/data"
"#).unwrap();

  let mut builder = LocalClusterBuilder::new();
  builder.add_node(coord_config);
  let mut cluster = builder.build();

  cluster.startup().ok();
}