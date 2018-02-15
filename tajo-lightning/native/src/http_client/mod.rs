use std::io;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use futures::{Future, Stream};
use hyper::{self, Client};
use hyper::client::{HttpConnector, Request};
use serde::de::{DeserializeOwned};
use serde_json;
use tokio_core::reactor::{Handle};

pub struct RequestsStats {
  pub all_request_cnt : AtomicU64,
  pub all_response_cnt: AtomicU64,
}

unsafe impl Send for RequestsStats {}
unsafe impl Sync for RequestsStats {}

pub trait HttpClient: Send + Sync {
  fn execute_async<T: DeserializeOwned + 'static>(&mut self, request: Request) -> Box<Future<Item=T, Error=hyper::Error>>;

  fn execute_async2<T: DeserializeOwned + 'static, F>(&mut self, request: Request, success: F) where F: FnOnce(T) + 'static;

  fn execute_async3<T: DeserializeOwned + 'static>(&mut self, request: Request, handler: &'static ResponseHandler<T>);
}

const DEFAULT_THREAD_NUM: usize = 2;

pub struct HttpClientBuilder {
  thread_num: usize,
  keep_alive: bool,
  keep_alive_timeout: Duration,
  max_idle: usize,
}

impl HttpClientBuilder {
  pub fn new() -> HttpClientBuilder {
    HttpClientBuilder {
      thread_num: DEFAULT_THREAD_NUM,
      keep_alive: true,
      keep_alive_timeout: Duration::from_secs(90),
      max_idle: 5,
    }
  }

  pub fn worker_num(mut self, thread_num: usize) -> HttpClientBuilder {
    self.thread_num = thread_num;
    self
  }

  pub fn keep_alive(mut self, flag: bool) -> HttpClientBuilder {
    self.keep_alive = flag;
    self
  }

  pub fn keep_alive_timeout(mut self, d: Duration) -> HttpClientBuilder {
    self.keep_alive_timeout = d;
    self
  }

  pub fn max_idle(mut self, max_idle: usize) -> HttpClientBuilder {
    self.max_idle = max_idle;
    self
  }

  pub fn build(self, handle: &Handle) -> io::Result<HyperHttpClient> {    
    let client = Client::configure()
      .connector(HttpConnector::new(self.thread_num, handle))
      .keep_alive(self.keep_alive)
      .build(handle);
    Ok(HyperHttpClient {
      handle: handle.clone(),
      client: client
    })
  }
}

pub struct HyperHttpClient {
  handle: Handle,
  client: Client<HttpConnector>
}

unsafe impl Send for HyperHttpClient {}
unsafe impl Sync for HyperHttpClient {}

impl HttpClient for HyperHttpClient {
  fn execute_async<T: DeserializeOwned + 'static>(&mut self, request: Request) -> Box<Future<Item=T, Error=hyper::Error>> {
    Box::new(self.client.request(request).and_then(|res| {
      res.body().concat2()
    })
    .then(move |concated| {
      let body = concated.unwrap();
      println!("{}", ::std::str::from_utf8(body.as_ref()).unwrap());
      Ok(serde_json::from_reader(body.as_ref()).unwrap())
    }))
  }

  fn execute_async2<T: DeserializeOwned + 'static, F>(&mut self, request: Request, success: F) where F: FnOnce(T) + 'static {
    let future: Box<Future<Item=(), Error=()>> = Box::new(self.client.request(request).and_then(|res| {
      res.body().concat2()
    })
    .then(move |concated| {      
      let body = concated.unwrap();
      println!("{}", ::std::str::from_utf8(body.as_ref()).unwrap());
      let json: T = serde_json::from_reader(body.as_ref()).unwrap();
      success(json);
      Ok(())
    }));
    self.handle.spawn(future);
  }

  fn execute_async3<T: DeserializeOwned + 'static>(&mut self, request: Request, handler: &'static ResponseHandler<T>) {
    let future: Box<Future<Item=(), Error=()>> = Box::new(self.client.request(request).and_then(|res| {
      res.body().concat2()
    })
    .then(move |concated| {      
      let body = concated.unwrap();
      println!("{}", ::std::str::from_utf8(body.as_ref()).unwrap());
      let json: T = serde_json::from_reader(body.as_ref()).unwrap();
      handler.success(json);
      Ok(())
    }));
    self.handle.spawn(future);
  }
}

pub trait ResponseHandler<T: DeserializeOwned + 'static> {
  fn success(&self, T);
}

#[derive(Serialize, Deserialize, Debug)]
struct Point {
    x: i32,
    y: i32,
}

#[test]
fn test() {
  //let uri = Uri::from_str("http://google.com").unwrap();
  //let mut core = Core::new().unwrap();
  //let handle = core.handle();
  //let mut http = HttpClientBuilder::new().build(&handle).ok().unwrap();
  //let req = Request::new(Method::Get, uri);
  //http.execute_async2(req, |r: Point| println!("{:?}", r));
  // let future: Box<Future<Item=Point, Error=hyper::Error>> = http.execute_async(req);
  // let p = core.run(future).unwrap();
  // let x = handle.execute(future.then(|x| {
  //   Ok(())
  // }));
  // println!("before turn!");
  // loop {
  //   core.turn(None);
  // }
  // println!("after turn!");
  // thread::sleep(Duration::from_secs(5));
  

  //println!("{:?}", p)
}