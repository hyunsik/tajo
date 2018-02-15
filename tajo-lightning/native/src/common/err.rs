#![allow(non_snake_case)]

/// Design Consideration:
/// * Error should be able to be serialized/deserialized.
/// * Easy to make error messages
/// * Should be easy to be used in match

use std::collections::HashMap;
use std::fmt;
use std::io;

use util::str::Formatter;

pub use self::ErrKind::*;

lazy_static! {
  static ref EMPTY_ARRAY_STR: Vec<&'static str> = { vec!() };
}

macro_rules! errors {
  ( 
    $( ( $index:expr, $err_name:ident, $err_msg:expr ) )+ 
  ) => {
    
    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
    pub enum ErrKind {
      $( $err_name = $index ),+
    }

    impl fmt::Display for ErrKind {
      fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
          $( $err_name => write!(f, stringify!($err_name)) ),*
        }
      }
    }

    lazy_static! {
      static ref MESSAGES: HashMap<ErrKind, &'static str> = {
        let mut m = HashMap::new();
        $( m.insert(ErrKind::$err_name, $err_msg); )*
        m
      };
    }

  }
}

impl ErrKind {
  pub fn index(&self) -> u32 {
    *self as u32
  }
}

errors! {
  // Reserved for Ok, actually not used here. 
  // But, the code number 0 will be used to indicate Success.
  (0, Success, "success")

  // General Errors
  (101, InternalError, "internal error")
  (102, NotImplemented, "not implemented")

  // Not implemented feature

  // Data Exception
  (451, DevisionByZero, "division by zero")
  (452, InvalidCast, "division by zero")  
  
  // Plan Error

  // Config Error
  (2, InvalidConfig, "invalid config at '{}', cause is {}")

  // Coordinator Error
  (5, NodeAlreadyJoined, "node {} already joined")
  (6, NodeNotFound, "node {} not found")
//   // Node Error

//   // Executor Error

  // I/O Error
  (500, IoError, "{}")
  (501, FileNotFound, "{} file doesn't exist")
  (502, InvalidPath, "{} is invalid path")
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error(pub ErrKind, pub String);

impl Error {
  pub fn kind(&self) -> ErrKind {
    self.0
  }
  
  pub fn message(&self) -> &str {
    &self.1
  }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
      write!(f, "[{}] {}: {}", self.0.index(), self.0, self.1)
    }
}

#[inline]
pub fn err<T>(kind: ErrKind) -> Result<T> {
  err_with(kind, &EMPTY_ARRAY_STR)
}

pub fn err_with<T>(kind: ErrKind, msg_args: &[&str]) -> Result<T> {  
  Err(err_with_raw(kind, msg_args))
}

fn err_with_raw(kind: ErrKind, msg_args: &[&str]) -> Error {
  let msg_fmt = *MESSAGES.get(&kind).expect(&format!("no such error message format for {}", kind));
  debug!("message format: '{}', arguments: '{:?}'", msg_fmt, msg_args);
  Error(kind, msg_fmt.format(msg_args))
}

#[derive(Clone, Debug)]
pub enum InternalErrKind {
  NodeAlreadyJoined,
  NodeNotFound
}

pub type Result<T> = ::std::result::Result<T, Error>;

impl From<io::Error> for Error {
  fn from(e: io::Error) -> Error {   
    err_with_raw(IoError, &[&format!("{}", e)])
  }
}

macro_rules! emit_err {
  ( $level:tt, $err:expr ) => {
    {
      let e = $err;
      $level!("{}", e.as_ref().err().unwrap());
      e
    }
  };
}

#[test]
fn test() {
  // println!("{}", ErrKind::InternalError);
  // println!("{}", err(InternalError).message())
}