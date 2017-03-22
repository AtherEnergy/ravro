//! Contains complex avro types declaration such as Records etc.

use codec::{Codec, EncodeErr, DecodeErr};
use std::io::{Write, Read};
use types::{DecodeValue};
use types::Schema;

use serde_json::Value;

#[derive(Debug, PartialEq, Clone)]
pub struct Field {
	pub name: String,
	pub doc: Option<String>,
	pub ty: Schema
}

impl Field {
	pub fn new(name: &str, doc: Option<&str>, ty: Schema) -> Self {
		Field {
			name: name.to_string(),
			doc: doc.map(|s| s.to_owned()),
			ty:ty
		}
	}

	pub fn get_name(&self) -> &str {
		self.name.as_str()
	}
}

#[derive(Debug, PartialEq, Clone)]
pub struct RecordSchema {
	pub name: String,
	pub doc: Option<String>,
	pub fields: Vec<Field>
}

impl RecordSchema {
	/// Create a new Record schema given a name, a doc string, and optional fields.
	pub fn new(name: &str, doc: Option<&str>, fields: Vec<Field>) -> Self {
		RecordSchema {
			name: name.to_string(),
			doc: doc.map(|s| s.to_string()),
			fields: fields
		}
	}

	/// Creates a RecordSchema out of a `serde_json::Value` object. This RecordSchema can then
	/// be used for decoding the record from the reader.
	// TODO return proper error.
	pub fn from_json(json: Value) -> Result<RecordSchema, ()> {
		if let Value::Object(obj) = json {
			let rec_name = obj.get("name").ok_or(())?;
			let fields = obj.get("fields").unwrap().as_array().map(|s| s.to_vec()).unwrap();
			let fields_vec = vec![];
			for i in fields.iter() {
				assert!(i.is_object());
				let field_type = i.get("type");
				let field_name = i.get("name");
			}
			let rec_name = rec_name.as_str().unwrap();
			let rec = RecordSchema::new(rec_name, None, fields_vec);
			Ok(rec)
		} else  {
			warn!("Expected a JSON object");
			Err(())
		}
	}
}

