//! Contains complex avro types declaration such as Records etc.

use types::{Schema, FromAvro};
use serde_json::Value;
use errors::AvroErr;
use std::io::Write;
use conversion::{Encoder, Decoder};
use std::collections::HashMap;
use regex::Regex;
use std::io::Read;

/// Represents `fullname` attribute of named type
#[derive(Debug, PartialEq, Clone)]
pub struct Named {
	name: String,
	namespace: Option<String>,
	doc: Option<String>
}

impl Named {
	fn new(name:&str, namespace: Option<String>, doc: Option<String>) -> Self {
		Named {
			name: name.to_string(),
			doc: doc,
			namespace: namespace
		}
	}

	fn validate(&self) -> Result<(), AvroErr> {
		let name_matcher = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*").unwrap();
		if !name_matcher.is_match(&self.name) {
			return Err(AvroErr::FullnameErr);
		} else if self.namespace.as_ref().map(|c|c.contains(".")).unwrap_or(false) {
			let names = self.namespace.as_ref().map(|s| s.split(".")).unwrap();
			for n in names {
				if !name_matcher.is_match(n) {
					return Err(AvroErr::FullnameErr);
				}
			}
			return Ok(());
		} else {
			Err(AvroErr::FullnameErr)
		}
	}

	pub fn fullname(&self) -> String {
		let namespace = self.namespace.as_ref().unwrap();
		format!("{:?}.{:?}", namespace, self.name)
	}
}

#[test]
fn test_fullname_attrib() {
	let named = Named::new("X", Some("org.foo".to_string()), None);
	assert!(named.validate().is_ok());
}

/// A field represents the elements of the `fields` attribute of the `RecordSchema`
#[derive(Debug, PartialEq, Clone)]
pub struct Field {
	/// Name of the field in a Record Schema
	name: String,
	/// Optional docs describing the field
	doc: Option<String>,
	/// The Schema of the field
	pub ty: Schema,
	/// The default value of this field
	default: Option<Schema>
}

impl Field {
	/// Create a new field given its name, schema and an optional doc string.
	pub fn new(name: &str, doc: Option<&str>, ty: Schema) -> Self {
		Field {
			name: name.to_string(),
			doc: doc.map(|s| s.to_owned()),
			ty:ty,
			default: None
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
	pub fullname: Named,
	pub fields: Vec<Field>
}

impl RecordSchema {
	/// Create a new Record schema given a name, a doc string, and optional fields.
	pub fn new(name: &str, doc: Option<&str>, fields: Vec<Field>) -> Self {
		RecordSchema {
			fullname: Named::new(name, doc.map(|s| s.to_string()), None),
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
	symbols: Vec<String>,
	current_val: Option<String>
}

impl EnumSchema {
	// TODO populate values from the schema
	pub fn new(name: &str, symbols: &[&'static str]) -> Self {
		let mut v = Vec::new();

		for i in 0..symbols.len() {
			v.push(symbols[i].to_string());
		}
		EnumSchema {
			name:name.to_string(),
			symbols: v,
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
			let idx = self.symbols.iter().position(|it| it == current_val).unwrap();
			let int: Schema = (idx as i64).into();
			int.encode(writer)
		} else {
			Err(AvroErr::EncodeErr)
		}
	}
}

impl Decoder for EnumSchema {
	type Out=Self;
	fn decode<R: Read>(self, reader: &mut R) -> Result<Self::Out, AvroErr> {
		let sym_idx = FromAvro::Long.decode(reader).unwrap().long_ref();
		let resolved_val = self.symbols[sym_idx as usize].clone();
		let schema = EnumSchema { name: self.name,
								  symbols: self.symbols,
								  current_val: Some(resolved_val)
		};
		Ok(schema)
	}
}

#[test]
fn test_enum_encode_decode() {
	use std::io::Cursor;
	use writer::{DataWriter, Codecs};
	let mut enum_scm = EnumSchema::new("Foo", &["CLUBS", "SPADE", "DIAMOND"]);
	let mut writer = Vec::new();
	enum_scm.set_value("DIAMOND");
	enum_scm.encode(&mut writer).unwrap();
	let mut writer = Cursor::new(writer);
	let decoder_enum = EnumSchema::new("Foo", &["CLUBS", "SPADE", "DIAMOND"]).decode(&mut writer);
}