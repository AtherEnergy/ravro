//! Contains declaration of a struct repr of the Type type

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;
use serde_json::{self, Value, from_reader};
use types::Type;
use std::str;
use writer::SchemaTag;
use errors::SchemaParseErr;
use failure::Error;
use std::fmt::Debug;

lazy_static! {
	static ref PRIMITIVE: &'static [&'static str] = &["null", "boolean", "int", "long", "float", "double", "bytes", "string"];
	// static ref COMPLEX: &'static [&'static str] = &["record", "enum", "array", "map", "union", "fixed"];
}

/// The parsed avro schema
#[derive(Debug, Clone)]
pub enum AvroSchema {
	/// A primitive schema
	Primitive(Value),
	/// A complex schema
	Complex(Value)
}

fn parse_field_type(field_ty_str: &str) -> SchemaTag {
	match field_ty_str {
		"null" => SchemaTag::Null,
		"boolean" => SchemaTag::Boolean,
		"int" => SchemaTag::Int,
		"long" => SchemaTag::Long,
		"float" => SchemaTag::Float,
		"double" => SchemaTag::Double,
		"bytes" => SchemaTag::Bytes,
		"string" => SchemaTag::String,
		_ => panic!("Complex types are not supported yet as a Field")
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

/// Implement doc and other fields
/// The fields currently only support Primitive avro types
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaField {
	/// Name of the field
	pub name: String,
	/// Type of the field
	pub ty: SchemaTag,
	/// Default value if any given as a string
	pub default: Option<String>
}

impl SchemaField {
	/// Creates a new field with the given name and avro type
	/// NOTE: currently only primitive types are supported as a field in a record
	pub fn new(name: &str, ty: &str) -> Self {
		SchemaField {
			name: name.to_string(),
			ty: parse_field_type(ty),
			default: None
		}
	}

	/// Returns the schema type in field as string
	pub fn type_str(&self) -> &str {
		match self.ty {
		SchemaTag::Null => "null",
		SchemaTag::Boolean => "boolean",
		SchemaTag::Int => "int",
		SchemaTag::Long => "long",
		SchemaTag::Float => "float",
		SchemaTag::Double => "double",
		SchemaTag::Bytes => "bytes",
		SchemaTag::String => "string",
		_ => unimplemented!("Complex types are not supported yet as a Field")
		}
	}

	/// Returns the schema name in field as string
	pub fn name(&self) -> &str {
		&self.name
	}

	/// Returns the schema name in field as string
	pub fn default(&self) -> Option<String> {
		self.default.clone()
	}

	/// Sets the default value if provided in the schema declaration
	pub fn set_default(&mut self, val: Option<&str>) {
		self.default = val.map(|s| s.to_string());
	}
}

impl AvroSchema {
	/// Parse an avro schema from a string
	pub fn from_str(schema: &str) -> Result<Self, Error> {
		let json_schema: Value = serde_json::from_str(schema).map_err(|e|{
			debug!("Avro schema parse error: {:?}", e);
			SchemaParseErr::InvalidSchema
		})?;
		map_from_json(json_schema)
	}

	/// Parse an avro schema from a file path
	pub fn from_file<P: AsRef<Path> + Debug>(path: P) -> Result<Self, Error> {
		let schema_file = OpenOptions::new().read(true).open(&path).map_err(|e| {
			debug!("Schema file {:?} not found: {}", path, e);
			SchemaParseErr::NotFound
		})?;
		let json_schema: Value = from_reader(schema_file).unwrap();
		map_from_json(json_schema)
	}

	/// If the schema is a record then this method gives back the fields in the order
	/// they are declared.
	pub fn record_field_pairs(&self) -> Option<Vec<SchemaField>> {
		match *self {
			AvroSchema::Primitive(_) => None,
			AvroSchema::Complex(ref schema) => if schema.is_object() {
				let fields_vec = schema["fields"].as_array().unwrap();
				let mut fields = vec![];
				for obj in fields_vec.iter() {
					// TODO currently on primitive types as fields are supported
					let name = obj["name"].as_str().unwrap();
					let _type = obj["type"].as_str().unwrap();
					let default = obj["default"].as_str();
					let mut schema_field = SchemaField::new(name, _type);
					schema_field.set_default(default);
					fields.push(schema_field);
				}
				Some(fields)
			} else {
				None
			}
		}
	}
}

fn map_from_json(json_schema: Value) -> Result<AvroSchema, Error> {
	if json_schema.is_string() {
		Ok(AvroSchema::Primitive(json_schema))
	} else if json_schema.is_array() {
		Ok(AvroSchema::Complex(json_schema))
	} else if json_schema.is_object() {
		Ok(AvroSchema::Complex(json_schema))
	} else {
		bail!(SchemaParseErr::InvalidSchema);
	}
}

// Converts a serde json avro schema to SchemaTag. SchemaTag is mainly used by data writer instances
// to type check the data being written into the datafile.
impl Into<SchemaTag> for AvroSchema {
	fn into(self) -> SchemaTag {
		match self {
			AvroSchema::Primitive(v) => {
				let v_str = v.as_str().unwrap();
				if PRIMITIVE.contains(&v_str) {
					return parse_schema_tag(v.as_str().unwrap())
				} else {
					panic!("Json strings can only represent primitive avro formats");
				}
			}
			AvroSchema::Complex(obj) => {
				if let Some(&Value::String(ref s)) = obj.get("type") {
					return parse_schema_tag(s)
				} else {
					panic!("Could not find type attribute in complex avro schema");
				}
			}
		}
	}
} 

impl From<Type> for String {
	fn from(schema: Type) -> Self {
		if let Type::Str(s) = schema {
			s
		} else {
			panic!("Expected String schema");
		}
	}
}

impl From<Type> for i64 {
	fn from(schema: Type) -> Self {
		if let Type::Long(l) = schema {
			l
		} else {
			panic!("Expected Long schema");
		}
	}
}

impl From<Type> for HashMap<String, Type> {
	fn from(schema: Type) -> Self {
		if let Type::Map(bmap) = schema {
			bmap
		} else {
			panic!("Expected Map schema");
		}
	}
}
