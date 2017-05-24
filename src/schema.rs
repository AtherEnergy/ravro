//! Contains declaration of a struct repr of the Schema type

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use serde_json::{Value, from_reader, from_str};
use types::Schema;
use std::str;

/// `AvroSchema` represents user provided schema file which gets parsed as a json object.
pub struct AvroSchema(pub Value);
impl AvroSchema {
	/// Create a AvroSchema from a given file `Path`.
	pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
		if path.as_ref().is_dir() {
			let schema_file = OpenOptions::new().read(true).open(path).unwrap();
			let file_json_obj = from_reader(schema_file).unwrap();
			Ok(AvroSchema(file_json_obj))
		} else {
			Err("Not a valid schema".to_string())
		}
	}

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

#[cfg(test)]
mod tests {
	use datafile::Header;
	use std::fs::OpenOptions;
	use conversion::Decoder;
	#[test]
	fn test_parse_header() {
		let mut f = OpenOptions::new().read(true)
									  .open("tests/encoded/double_encoded.avro").unwrap();
		let magic_buf = [0u8;4];
		let header = Header::new().decode(&mut f);
		assert!(header.is_ok());
	}
}
