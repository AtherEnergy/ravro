
use complex::Named;
use types::Type;

/// Implement doc and other fields
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
	/// Name of the field
	pub name: String,
	/// Type of the field
	pub ty: Type
}

impl Field {
	/// Creates a new field with the given name and avro type
	pub fn new(name: &str, ty: Type) -> Self {
		Field {
			name: name.to_string(),
			ty: ty
		}
	}
}

/// The `Record` represents an Avro Record
#[derive(Debug, PartialEq, Clone)]
pub struct Record {
	/// Represents a fullname of this record
	pub fullname: Named,
	/// Provides documentation to the user of this schema
	pub doc: Option<String>,
	/// Array of strings providing alternate names for this record
	pub aliases: Option<Vec<String>>,
	/// List of fields that this record contains. The writer must ensure that
	/// the field data is written in this order.
	pub fields: Vec<Field>
}

impl Record {
	/// Creates a blank record, and allows gradual build up of its fields
	pub fn builder() -> Record {
		Record {
			fullname: Named::new("record", None, None),
			doc: None,
			aliases: None,
			fields: vec![]
		}
	}
	/// Create a new Record schema given a name, a doc string, and optional fields.
	pub fn new(name: &str, doc: Option<&str>, fields: Vec<Field>) -> Self {
		Record {
			fullname: Named::new(name, doc.map(|s| s.to_string()), None),
			doc: doc.map(|s| s.to_string()),
			aliases: None,
			fields: fields
		}
	}

	/// Sets the name of the avro record
	pub fn set_name(&mut self, name: &str) {
		self.fullname = Named::new(name, None, None);
	}

	/// replaces the fields variable with actual values
	pub fn set_fields(&mut self, fields:Vec<Field>) {
		self.fields = fields;
	}

	/// adds a field to field vec
	pub fn push_field(&mut self, field: Field) {
		self.fields.push(field);
	}
}