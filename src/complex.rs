//! Contains complex avro types declaration such as Records etc.

use types::{Schema, FromAvro};
use serde_json::Value;
use errors::AvroErr;
use std::io::Write;
use codec::{Encoder, Decoder};
use regex::Regex;
use std::io::Read;
use writer::get_schema_util;

lazy_static! {
    static ref NAME_MATCHER: Regex = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*").unwrap();
}

/// Represents `fullname` attribute of a named avro type
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

	fn get_name(&self) -> &str {
		self.name.as_str()	
	}

	fn get_namespace(&self) -> Option<&String> {
		self.namespace.as_ref()
	}

	fn validate(&self) -> Result<(), AvroErr> {
		if !NAME_MATCHER.is_match(&self.name) {
			return Err(AvroErr::InvalidFullname);
		} else if self.namespace.as_ref().map(|c|c.contains(".")).unwrap_or(false) {
			let names = self.namespace.as_ref().map(|s| s.split(".")).unwrap();
			for n in names {
				if !NAME_MATCHER.is_match(n) {
					return Err(AvroErr::InvalidFullname);
				}
			}
			return Ok(());
		} else {
			Err(AvroErr::InvalidFullname)
		}
	}

	/// Retrieves the fullname of the corresponding named type
	pub fn fullname(&self) -> String {
		let namespace = self.namespace.as_ref().unwrap();
		format!("{}.{}", namespace, self.name)
	}
}

#[test]
fn test_fullname_attrib() {
	let named = Named::new("X", Some("org.foo".to_string()), None);
	assert!(named.validate().is_ok());
}

/// This is just to specify if the `field` in a record is meant to be encoded or decoded
#[derive(Clone, PartialEq, Debug)]  
pub enum SchemaVariant {
	/// For encoding
	Encoded(Schema),
	/// For Decoding
	Decoded(FromAvro)
}

impl Encoder for SchemaVariant {
	fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr> {
		if let SchemaVariant::Encoded(ref schm) = *self {
			schm.encode(writer)
		} else {
			unreachable!("encode must be only called on a Encoded variant of any field");
		}
	}
}

/// A field represents the elements of the `fields` attribute of the `RecordSchema`
#[derive(Debug, PartialEq, Clone)]
pub struct Field {
	/// Name of the field in a Record Schema
	name: String,
	/// Optional docs describing the field
	doc: Option<String>,
	/// The Schema of the field
	pub ty: SchemaVariant,
	/// The default value of this field
	default: Option<SchemaVariant>
}

impl Decoder for Field {
	type Out=Field;
    /// Allows decoding a type out of a given Reader
	fn decode<R: Read>(self, reader: &mut R) -> Result<Self::Out, AvroErr> {
		let Field {name, doc, mut ty, default} = self;
		match ty {
			SchemaVariant::Decoded(from_avro) => {
				ty = SchemaVariant::Encoded(from_avro.decode(reader)?);
				Ok(Field { name: name, doc: doc, ty: ty, default: default})
			},
			_ => unreachable!("decode must be only called on a Encoded variant of any field")
		}
	}
}

impl Field {
	/// Create a new field for encoding given its name, schema and an optional doc string.
	pub fn new_for_encoding(name: &str, doc: Option<&str>, ty: Schema) -> Self {
		Field {
			name: name.to_string(),
			doc: doc.map(|s| s.to_owned()),
			ty: SchemaVariant::Encoded(ty),
			default: None
		}
	}
	/// Create a new field for decoding given its name, schema and an optional doc string.
	pub fn new_for_decoding(name: &str, doc: Option<&str>, ty: FromAvro) -> Self {
		Field {
			name: name.to_string(),
			doc: doc.map(|s| s.to_owned()),
			ty: SchemaVariant::Decoded(ty),
			default: None
		}
	}

	/// parses a Record field from a serde_json object
	/// TODO implement this
	pub fn from_json(obj: Value) -> Result<Self, ()> {
		if obj.is_object() {
			let f_name = obj.get("name").unwrap().as_str().unwrap();
			Err(())
		} else {
			Err(())
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
	/// Represents a fullname of this record
	pub fullname: Named,
	// pub doc: Option<String>,
	/// list of fields that this record contains
	pub fields: Vec<Field>
}

impl RecordSchema {
	/// Create a new Record schema given a name, a doc string, and optional fields.
	pub fn new(name: &str, doc: Option<&str>, fields: Vec<Field>) -> Self {
		RecordSchema {
			fullname: Named::new(name, doc.map(|s| s.to_string()), None),
			// doc: doc.map(|s| s.to_string()),
			fields: fields
		}
	}

	/// replaces the fields variable with actual values
	pub fn set_fields(&mut self, fields:Vec<Field>) {
		self.fields = fields;
	}

	/// Creates a RecordSchema out of a `serde_json::Value` object. This RecordSchema can then
	/// be used for decoding the record from the reader.
	// TODO return proper error.
	pub fn from_json(json: &Value) -> Result<RecordSchema, ()> {
		if let Value::Object(ref obj) = *json {
			let rec_name = obj.get("name").ok_or(())?;
			let fields = obj.get("fields").unwrap().as_array().map(|s| s.to_vec()).unwrap();
			let mut fields_vec = vec![];
			for i in &fields {
				assert!(i.is_object());
				let field_type = i.get("type").unwrap();
				let field_name = i.get("name").unwrap().as_str().unwrap();
				let field = Field::new_for_decoding(field_name, None, get_schema_util(field_type));
				fields_vec.push(field);
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
/// An avro complex type akin to enums in most languages 
pub struct EnumSchema {
	name: String,
	symbols: Vec<String>,
	current_val: Option<String>
}

impl EnumSchema {
	// TODO populate values from the schema
	/// Creates a new enum schema from a list of symbols
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

	/// sets the active enum variant
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
fn enum_encode() {
    let symbols = ["CLUB", "DIAMOND", "SPADE"];
    let mut enum_schm = EnumSchema::new("deck_of_cards", &symbols);
    enum_schm.set_value("DIAMOND");
    let mut vec: Vec<u8> = vec![];
    enum_schm.encode(&mut vec);
    let val = FromAvro::Long.decode(&mut vec.as_slice()).unwrap().long_ref();
    assert_eq!(1,  val);
}

#[test]
fn array_encode() {
    use rand::StdRng;
    use rand::Rng;

    let mut encoded_vec = vec![];
    let mut v: Vec<Schema> = vec![];
    let a = Schema::Str("a".to_string());
    let b = Schema::Str("b".to_string());
    let c = Schema::Str("c".to_string());
    v.push(a);
    v.push(b);
    v.push(c);
    let fin = vec![6u8, 2, 97, 2, 98, 2, 99, 0];
    let len = Schema::Array(v).encode(&mut encoded_vec);
    assert_eq!(8, len.unwrap());
    assert_eq!(fin, encoded_vec);
}

#[test]
fn map_encode_decode() {
	use std::collections::BTreeMap;
	use std::str;
    let mut my_map = BTreeMap::new();
    my_map.insert("foo".to_owned(), Schema::Bool(true));
    let my_map = Schema::Map(my_map);
    let mut v = Vec::new();
    let len = my_map.encode(&mut v).unwrap();
    // Check total bytes we encoded
    // Length should be 7 according to:
    // 1) Map key count 1 byte
    // 2) Map string key encoding 4 byte
    // 3) Map value 1 byte
    // 4) End of map marker 1 byte
    assert_eq!(len, 7);
    let decoded_map = FromAvro::Map(Box::new(FromAvro::Bool)).decode(&mut v.as_slice()).unwrap();
    if let Schema::Map(decoded_map) = decoded_map {
        if let &Schema::Bytes(ref b) = decoded_map.get("foo").unwrap() {
            assert_eq!("bar", str::from_utf8(b).unwrap());
        }
    }
}