use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::Path;

use ::err::{err_with, Result, InvalidPath};
use common::assert::check_file_exists;


pub fn file_to_string(path: &Path) -> Result<String> {
  check_file_exists(path)?;

  let file = File::open(path)?;
  let mut buf_reader = BufReader::new(file);
  let mut contents = String::new();
  buf_reader.read_to_string(&mut contents)?;

  Ok(contents)
}

pub fn path_to_str<'a>(path: &'a Path) -> Result<&'a str> {
  if let Some(s) = path.to_str() {
    Ok(s)
  } else {
    err_with(InvalidPath, &[&path.to_string_lossy()])
  }
}