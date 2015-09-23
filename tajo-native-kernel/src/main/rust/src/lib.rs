// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#![crate_type="dylib"]
#![feature(libc)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unused_variables)]

extern crate libc;

extern crate jni;

use libc::c_void;
use std::ptr;

use jni::native::*;
use jni::helper::*;

#[no_mangle]
pub extern fn Java_org_github_hyunsik_NativeInvoke_procedure(jre: *mut JNIEnv,
    class: *const c_void) {
  println!("Invoked native method, jre: {:p}, class: {:p}", jre, class);
  unsafe {
    let v = ((**jre).GetVersion)(jre);
    println!(">> version: {:?}", v);
  }
}

#[no_mangle]
pub extern fn Java_org_github_hyunsik_NativeInvoke_stringArg(jre: *mut JNIEnv,
    class: *const c_void, name: jstring) {
  unsafe {
      let string = ((**jre).GetStringUTFChars)(jre, name, ptr::null_mut());
      println!("{}", chars_to_str(string));

       ((**jre).ReleaseStringUTFChars)(jre, name, string);
  }
}

#[no_mangle]
pub extern fn Java_org_github_hyunsik_NativeInvoke_returnString(jre: *mut JNIEnv,
    class: *const c_void) -> jstring {
  unsafe {
      return str_to_jstring(jre, "jni native");
  }
}
