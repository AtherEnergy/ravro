//! Contains complex avro types declaration such as Records etc.

use types::Schema;
use serde_json::Value;
use errors::AvroErr;
use std::io::Write;
use conversion::Encoder;
use std::collections::HashMap;

/// A field represents the elements of the `fields` attribute of the `RecordSchema`
#[derive(Debug, PartialEq, Clone)]
pub struct Field {
	/// Name of the field in a Record Schema
	name: String,
	/// Optional docs describing the field
	doc: Option<String>,
	/// The Schema of the field
	pub ty: Schema
}

impl Field {
	/// Create a new field given its name, schema and an optional doc string.
	pub fn new(name: &str, doc: Option<&str>, ty: Schema) -> Self {
		Field {
			name: name.to_string(),
			doc: doc.map(|s| s.to_owned()),
			ty:ty
		}
	}

	/// Retrieves the name of the field.
	pub fn get_name(&self) -> &str {
		self.name.as_str()
	}
}

/// The `RecordSchema` represents an Avro Record with all its field listed in order
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
			for i in &fields {
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

#[derive(Clone, PartialEq, Debug)]
pub struct EnumSchema {
	name: String,
	symbols: HashMap<String, usize>,
	current_val: Option<String>
}

impl EnumSchema {
	// TODO populate values from the schema
	pub fn new(name: &str, symbols: &[&'static str]) -> Self {
		let mut map = HashMap::new();
		for i in 0..symbols.len() {
			map.insert(symbols[i].to_string(), i);
		}
		EnumSchema {
			name:name.to_string(),
			symbols: map,
			current_val: None
		}
	}

	pub fn set_value(&mut self, val: &str) {
		self.current_val = Some(val.to_string());
	} 
}

impl Encoder for EnumSchema {
	fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr> {
		if let Some(ref current_val) = self.current_val {
			let idx = self.symbols.get(current_val);
			let int: Schema = (*idx.unwrap() as i64).into();
			int.encode(writer)
		} else {
			Err(AvroErr::EncodeErr)
		}
	}
}