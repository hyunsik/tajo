use std::path::Path;

use toml;

use ::err::{err_with, InvalidConfig, Result};
use util::file::{file_to_string, path_to_str};

#[derive(Deserialize, Clone)]
pub struct Config {
  pub coordinator: Coordinator,
  pub node: Node
}

#[derive(Deserialize, Clone)]
pub struct Coordinator {
  pub enabled: bool,
  pub uri: String,
}

#[derive(Deserialize, Clone)]
pub struct Node {
  http: Http,
  executor: Executor
}

#[derive(Deserialize, Clone)]
pub struct Http {
  pub listen: String,
}

#[derive(Deserialize, Clone)]
pub struct Executor {
  pub data_dir: String,
}

impl Config {
  
  pub fn from_file(path: &Path) -> Result<Config> {
    info!("Loading the config at '{}'", path.to_str().unwrap());
    let contents: String = file_to_string(path)?;
    match toml::from_str(&contents) {
      Ok(config) => Ok(config),
      Err(e) => {
        err_with(InvalidConfig, &[path_to_str(path)?, &format!("{}", e)])
      }
    }
  }

  pub fn from_str(conf_str: &str) -> Result<Config> {    
    match toml::from_str(conf_str) {
      Ok(config) => Ok(config),
      Err(e) => {
        err_with(InvalidConfig, &[r"Nofile", &format!("{}", e)])
      }
    }
  }
}

#[test]
fn test_config_from_str() {
  let config = Config::from_str(r#"
[coordinator]
enabled = true
uri="http://localhost:8080"

[node]

[node.http]
listen="127.0.0.1:8000"

[node.executor]
data_dir="/tmp/flang/data"
"#).unwrap();

  assert!(config.coordinator.enabled);
  assert_eq!(&config.coordinator.uri, "http://localhost:8080");
  assert_eq!(&config.node.http.listen, "127.0.0.1:8000");
  assert_eq!(&config.node.executor.data_dir, "/tmp/flang/data");
}