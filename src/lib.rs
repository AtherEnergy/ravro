#![cfg_attr(feature="clippy", feature(plugin))]

#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate serde_json;
extern crate rand;
extern crate snap;
#[macro_use]
extern crate log;
extern crate crc;
extern crate byteorder;
extern crate linked_hash_map;

#[macro_use]
pub mod macros;

pub mod schema;
#[macro_use]
pub mod types;
pub mod conversion;
pub mod complex;
pub mod datafile;
pub mod errors;
