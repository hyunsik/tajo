pub trait Formatter {
  fn format(&self, &[&str]) -> String;
}

impl<'a> Formatter for &'a str {
  fn format(&self, args: &[&str]) -> String {
    format(self, args)
  }
}

impl Formatter for String {
  fn format(&self, args: &[&str]) -> String {
    format(self.as_str(), args)
  }
}

#[derive(Clone, Copy, PartialEq)]
enum State {
  None = 0,
  Escape = 1,
  VarOpen = 2,
}

pub fn format(fmt: &str, args: &[&str]) -> String {
  let mut state = State::None;
  let mut buf = String::new();
  let mut arg_iter = args.iter();

  for c in fmt.chars() {
    
    match (state, c) {      
      (State::None, '{') => {        
        state = State::VarOpen;
      }
      (State::None, '}') => {        
        panic!("curly parenthese is not opened");
      }
      (State::None, '\\') => state = State::Escape,
      (State::None, _) => buf.push(c),

      (State::VarOpen, '{') => {
        panic!("curly parentheses is not closed");
      }
      (State::VarOpen, '}') => {
        buf.push_str(*arg_iter.next().expect("variables are more than arguments"));
        state = State::None;
      }
      (State::VarOpen, '\\') => {
        panic!("escape character '\' cannot be used in variable")
      }
      (State::VarOpen, _) => {}

      (State::Escape, _) => {
        buf.push(c);
        state = State::None;
      }
    };
  }

  match state {
    State::VarOpen => panic!("curly parentheses is not closed"),
    State::Escape => panic!("no more character to be escaped"),
    _ => {}
  };

  if let Some(_) = arg_iter.next() {
    panic!("given arguments are more than variables");
  }

  buf
}

#[test]
fn test_format() {
  assert_eq!("a", &format("{}", &["a"]));
  assert_eq!("ab", &format("{}{}", &["a", "b"]));
  assert_eq!("a{b", &format("{}\\{{}", &["a", "b"]));
  assert_eq!("\\\\", &format("\\\\\\\\", &[]));
}

#[test]
fn test_not_closed() {
  use std::panic;

  assert!( panic::catch_unwind(|| format("{}", &["a", "b"])).is_err() );
  assert!( panic::catch_unwind(|| format("{}{}", &["a"])).is_err() );

  assert!( panic::catch_unwind(|| format("{", &[])).is_err() );
  assert!( panic::catch_unwind(|| format("}", &[])).is_err() );
  assert!( panic::catch_unwind(|| format("{{", &[])).is_err() );
}