use rocket::{State, Response};
use rocket::http::Status;
use rocket::response::status;

pub fn response<'r>(s: Status, message: &str) -> Response<'r> {
  status::Custom(s, message)
}