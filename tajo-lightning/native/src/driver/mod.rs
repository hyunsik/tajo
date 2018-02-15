use std::env::current_dir;
use std::path::PathBuf;
use std::process::exit;
use std::rc::Rc;

use clap::{Arg, App, ArgMatches};
use uuid::Uuid;

use config::Config;
use common::messages::{AUTHORS, EXECUTABLE, PROJECT_WITH_VERSION, VERSION};
use common::Result;
use common::clock::{Clock, default_clock};
use common::service::{HasState, Service, ServiceState, ServiceManager};
use consts::CONF_MAIN_FILE;
use api_service::{ApiProvider, ApiServiceBuilder};
use node_service::NodeService;

/// environment variables and arguments for a running executable
pub struct RunEnv {
  pub conf: Rc<Config>,
  pub verbose: bool,
}

impl RunEnv {
  pub fn new() -> Result<RunEnv> {
    let args = RunEnv::parse_args();

    let conf_dir = match args.value_of("c") {
      Some(conf_dir) => PathBuf::from(conf_dir),
      None => RunEnv::default_dir("conf")
    };

    Ok(RunEnv {    
      conf: Rc::new(Config::from_file(&conf_dir.as_path().join(CONF_MAIN_FILE))?),
      verbose: args.is_present("verbose"),
    })
  }

  pub fn with(conf: Config, verbose: bool) -> Result<RunEnv> {
    Ok(RunEnv {    
      conf: Rc::new(conf),
      verbose: verbose
    })
  }

  fn parse_args<'a>() -> ArgMatches<'a> {
    App::new(EXECUTABLE)
      .version(VERSION)
      .author(AUTHORS)
      .about(PROJECT_WITH_VERSION)
      .arg(Arg::with_name("config")
          .short("c")
          .long("config")
          .help("Sets a directory containing configurations")
          .takes_value(true))
      .arg(Arg::with_name("verbose")
          .short("v")
          .help("Sets a verbose mdoe"))
      .arg(Arg::with_name("debug")
          .short("d")
          .help("print debug information verbosely"))
      .get_matches() 
  }

  fn default_dir(subdir: &str) -> PathBuf {
    let mut dir = current_dir().expect("Cannot get the current dir");
    dir.push(subdir);
    dir.to_path_buf()
  }
}

pub type NodeId = String;

pub struct DriverContext {
  pub node_id: NodeId,
  pub config: Rc<Config>,
  clock: Box<Clock>
}

impl DriverContext {
  fn new(config: Rc<Config>) -> DriverContext {
    DriverContext {
      node_id: Uuid::new_v4().hyphenated().to_string(),
      config: config,
      clock: default_clock()
    }
  }  
}

pub struct Driver {
  pub context: Rc<DriverContext>,
  state: ServiceState,
  services: ServiceManager,
}

impl HasState for Driver {
  fn state(&self) -> &ServiceState {
    &self.state
  }
}

unsafe impl Send for Driver {}
unsafe impl Sync for Driver {}

impl ApiProvider for Driver {
 fn register(&self, builder: ApiServiceBuilder) -> ApiServiceBuilder {
   builder
 }
}

impl Service for Driver {
  fn init(&mut self) -> Result<()> {    

    // By default, it's an empty list.
    let services: Vec<Box<Service>> = vec![
      Box::new(NodeService::new(self.context.clone()))
    ];

    // Register all APIs
    let mut api_builder = ApiServiceBuilder::new();
    for s in services.iter() {
      info!("Registring [{}] service APIs ...", s.name());
      api_builder = s.register(api_builder);
    }
    
    // Build and move API Service
    self.services.add_all(services);
    // Move services
    self.services.add(Box::new(api_builder.build()));    

    // Initialize all services
    self.services.init()
  }
  
  fn start(&mut self) -> Result<()> {
    trace!("Driver::start enter");
    let result = self.services.start();
    trace!("Driver::start leave");
    result
  }

  fn stop(&mut self) -> Result<()> {
    self.services.stop()
  }
}

impl Driver {
  pub fn new(env: RunEnv) -> Driver {
    Driver {
      state: ServiceState::new("Driver"),
      context: Rc::new(DriverContext::new(env.conf.clone())),
      services: ServiceManager::new("Sub Services")
    }
  }

  pub fn config(&self) -> &Config {
    &self.context.config
  }

  pub fn node_id(&self) -> &str {
    &self.context.node_id
  }

  pub fn is_coordinator(&self) -> bool {
    self.config().coordinator.enabled
  }

  pub fn startup(&mut self) -> Result<()> {    
    self._init()?;
    self._start()
  }

  pub fn shutdown(&mut self) -> Result<()> {
    Ok(())
  }
}

pub fn exit_if_failed<T>(res: Result<T>) -> T {
  match res {
    Ok(c) => c,
    Err(e) => {
      eprintln!("Error: {}", e);
      exit(-1);
    }
  }
}