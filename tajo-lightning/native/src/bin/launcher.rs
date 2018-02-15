#![feature(iterator_for_each)]
extern crate env_logger;
extern crate lightning;
#[macro_use] extern crate log;

use std::process::exit;
use std::thread::{self, sleep};
use std::time;

use lightning::common::messages::PROJECT_WITH_VERSION;
use lightning::driver::{Driver, RunEnv, exit_if_failed};

pub fn main() {
  env_logger::init().unwrap();  

  eprintln!("Initializing {}", PROJECT_WITH_VERSION);  
  info!("...");  
  
  let env = exit_if_failed(RunEnv::new());  
  let mut driver = Driver::new(env);  
  match driver.startup() {
    Ok(_) => {},
    Err(e) => panic!("{:?}", e)
  };

  let hang = thread::spawn(move || {
      loop {
        sleep(time::Duration::from_secs(1));
      }
  });
  hang.join().ok();
  exit(0);
}