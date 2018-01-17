//! Contains declaration of a struct repr of the Schema type

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use serde_json::{Value, from_reader, from_str};
use types::Schema;
use std::str;
use writer::SchemaTag;

lazy_static! {
	static ref PRIMITIVE: &'static [&'static str] = &["null", "boolean", "int", "long", "float", "double", "bytes", "string"];
	// static ref COMPLEX: &'static [&'static str] = &["record", "enum", "array", "map", "union", "fixed"];
}

/// `AvroSchema` represents user provided schema file which gets parsed as a json object.
#[derive(Debug)]
pub struct AvroSchema(pub Value);
impl AvroSchema {
	/// Create a AvroSchema from a given file `Path`.
	pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
		let schema_file = OpenOptions::new().read(true).open(path).unwrap();
		let file_json_obj = from_reader(schema_file).unwrap();
		Ok(AvroSchema(file_json_obj))
	}

	/// Create a AvroSchema from a schema as a string
	pub fn from_str(schema: &str) -> Result<Self, String> {
		if Path::new(schema).is_dir() {
			return Err("Not a valid schema".to_string())
		}
		let json_schema = from_str(schema).unwrap();
		Ok(AvroSchema(json_schema))
	}

	/// Returns an optional string slice for the schema.
	pub fn as_str(&self) -> Option<&str> {
		self.0.as_str()
	}
}

fn parse_schema_tag(schema_str: &str) -> SchemaTag {
	match schema_str {
		"null" => SchemaTag::Null,
		"boolean" => SchemaTag::Boolean,
		"int" => SchemaTag::Int,
		"long" => SchemaTag::Long,
		"float" => SchemaTag::Float,
		"double" => SchemaTag::Double,
		"bytes" => SchemaTag::Bytes,
		"string" => SchemaTag::String,
		"record" => SchemaTag::Record,
		"enum" => SchemaTag::Enum,
		"array" => SchemaTag::Array,
		"map" => SchemaTag::Map,
		"union" => SchemaTag::Union,
		"fixed" => SchemaTag::Fixed,
		_ => panic!("Unknown avro schema")
	}
}

// Converts a serde json avro schema to SchemaTag. SchemaTag is mainly used by data writer instances
// to type check the data being written into the datafile.
impl Into<SchemaTag> for AvroSchema {
	fn into(self) -> SchemaTag {
		match self.0 {
			Value::Object(map) => {
				if let Some(&Value::String(ref s)) = map.get("type") {
					return parse_schema_tag(s)
				} else {
					panic!("Could not find type attribute in complex avro schema");
				}
			}
			Value::String(s) => {
				if PRIMITIVE.contains(&&s[..]) {
					return parse_schema_tag(&s)
				} else {
					panic!("Json strings can only represent primitive avro formats");
				}
			}
			_ => unimplemented!()
		}
	}
} 

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

impl From<Schema> for BTreeMap<String, Schema> {
	fn from(schema: Schema) -> Self {
		if let Schema::Map(bmap) = schema {
			bmap
		} else {
			panic!("Expected Map schema");
		}
	}
}
