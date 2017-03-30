//! Contains declaration of a struct repr of the Schema type

use std::io::{Write, Read};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use rand::thread_rng;
use rand::Rng;
use serde_json::{Value, from_reader};
use codec::{Codec, EncodeErr, DecodeErr};
use datafile::SyncMarker;
use types::{Schema, DecodeValue};
use std::fs::File;
use std::str;
use datafile::Header;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};

pub struct AvroSchema(pub Value);
impl AvroSchema {
	pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
		let schema_file = OpenOptions::new().read(true).open(path).unwrap();
		let file_json_obj = from_reader(schema_file).unwrap();
		Ok(AvroSchema(file_json_obj))
	}

	pub fn as_str(&self) -> Option<&str> {
		self.0.as_str()
	}
}

/// These allows conversion from the 
impl From<Schema> for String {
	fn from(schema: Schema) -> Self {
		if let Schema::Str(s) = schema {
			s
		} else {
			panic!("Expected String schema");
		}
	}
}

impl From<Schema> for i64 {
	fn from(schema: Schema) -> Self {
		if let Schema::Long(l) = schema {
			l
		} else {
			panic!("Expected Long schema");
		}
	}
}

#[test]
fn test_parse_double_encoded() {
	let mut f = OpenOptions::new().read(true).open("tests/encoded/double_encoded.avro").unwrap();
	let mut magic_buf = [0u8;4];
	Header::decode(&mut f, DecodeValue::Header);

	// f.read_exact(&mut magic_buf[..]).unwrap();
	// let decoded_magic = str::from_utf8(&magic_buf[..]).unwrap();
	// // Assert header is present
	// assert_eq!("Obj\u{1}", decoded_magic);
	// let map_block_count = Schema::decode(&mut f, DecodeValue::Long).unwrap();
	// let count = if let Schema::Long(l) = map_block_count {
	// 	l
	// } else {0};

	// let mut map = BTreeMap::new();
	// for i in 0..count as usize {
	// 	let key = Schema::decode(&mut f, DecodeValue::Str).unwrap();
	// 	let a = String::from(key);
	// 	let val = Schema::decode(&mut f, DecodeValue::Bytes).unwrap();
	// 	println!("val {:?}", val);
	// 	map.insert(a, val);
	// }
	// let sync_marker = SyncMarker::decode(&mut f, DecodeValue::SyncMarker).unwrap();
}
