#![feature(integer_atomics)]
#![feature(iterator_for_each)]
#![feature(plugin)]
#![plugin(rocket_codegen)]


extern crate clap;
extern crate futures;
#[macro_use]
extern crate lazy_static;
extern crate hyper;
#[macro_use] extern crate log;
extern crate rocket;
extern crate rocket_contrib;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;
extern crate toml;
extern crate uuid;

#[macro_use] pub mod common;
pub mod api_service;
pub mod node_service;
pub mod driver;
pub mod http_client;
pub mod util;

////////////////////////////////////////////////////////////
//                       Reexports                        //
////////////////////////////////////////////////////////////
pub mod asserts {
  pub use common::assert::*;
}

pub mod consts {
  pub use common::consts::*;
}

pub mod config {
  pub use common::config::Config;
}

pub mod err {
  pub use common::err::*;
}