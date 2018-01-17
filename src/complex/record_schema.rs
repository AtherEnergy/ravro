
use complex::{Field, Named};

/// The `RecordSchema` represents an Avro Record with all its field listed in order
#[derive(Debug, PartialEq, Clone)]
pub struct RecordSchema {
	/// Represents a fullname of this record
	pub fullname: Named,
	/// Provides documentation to the user of this schema
	pub doc: Option<String>,
	/// array of strings, providing alternate names for this record
	pub aliases: Option<Vec<String>>,
	/// list of fields that this record contains
	pub fields: Vec<Field>
}

impl RecordSchema {
	/// Create a new Record schema given a name, a doc string, and optional fields.
	pub fn new(name: &str, doc: Option<&str>, fields: Vec<Field>) -> Self {
		RecordSchema {
			fullname: Named::new(name, doc.map(|s| s.to_string()), None),
			doc: doc.map(|s| s.to_string()),
			aliases: None,
			fields: fields
		}
	}

	/// replaces the fields variable with actual values
	pub fn set_fields(&mut self, fields:Vec<Field>) {
		self.fields = fields;
	}
}