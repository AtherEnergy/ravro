//! This library implements the Apache Avro Serialization [spec](https://avro.apache.org/docs/1.8.1/spec.html) 
#![deny(missing_docs)]

#![recursion_limit = "1024"]

#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate serde_json;
extern crate rand;
extern crate snap;
#[macro_use]
extern crate log;
extern crate crc;
extern crate byteorder;
extern crate regex;
#[macro_use]
extern crate lazy_static;

#[macro_use]
pub mod macros;

pub mod schema;
#[macro_use]
pub mod types;
pub mod conversion;
pub mod complex;
pub mod datafile;
/// Errors in context of avro data files
pub mod errors;
/// Allows reading from avro data file
pub mod reader;
