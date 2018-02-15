//! A collection of functions to assert and check various conditions.
//!
//! There are two kinds of functions
//! * check_* :  it will return Err if the condition is false.
//! * assert_* :  it will throw panic if the condition is false.

use std::path::Path;
use common::err::{err_with, Result, FileNotFound};
use util::file::path_to_str;

pub fn check_file_exists(path: &Path) -> Result<()> {
  if path.exists() {
    Ok(())
  } else {
    err_with(FileNotFound, &[path_to_str(path)?])
  }
}