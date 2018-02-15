pub static CONF_MAIN_FILE: &'static str = "config.toml";

pub mod api {
  pub mod service {
    pub static API_NAME: &'static str = "services";
    pub static API_PATH: &'static str = "/1/services";
    pub const  API_VER : u32 = 1;
    pub static API_DESC: &'static str = "Service Discovery API";
  }

  pub mod node {
    pub static API_NAME: &'static str = "node";
    pub static API_PATH: &'static str = "/1/node";
    pub const  API_VER : u32 = 1;
    pub static API_DESC: &'static str = "Node API";
  }
}