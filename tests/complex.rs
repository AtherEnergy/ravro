extern crate ravro;
extern crate rand;

use ravro::datafile::{DataWriter, Codecs};

use std::fs::OpenOptions;
use ravro::types::Schema;
use ravro::schema::AvroSchema;

use std::collections::BTreeMap;

use ravro::complex::{RecordSchema, Field};

use rand::StdRng;
use rand::Rng;

pub fn gen_rand_str() -> String {
	let mut std_rng = StdRng::new().unwrap();
	let ascii_iter = std_rng.gen_ascii_chars();
	ascii_iter.take(20).collect()
}

#[test]
fn test_write_map() {
	let schema_file = "tests/schemas/map_schema.avsc";
	let map_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/map_encoded.avro";
	let writer = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(map_schema, writer, Codecs::Snappy).unwrap();
	let mut map = BTreeMap::new();
	map.insert("A".to_owned(), Schema::Double(234.455));
	let _ = data_writer.write(Schema::Map(map));
}

#[test]
fn write_nested_record() {
	let schema_file = "tests/schemas/nested_schema.avsc";
	let rec_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/nested_encoded.avro";
	let writer = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(rec_schema, writer, Codecs::Snappy).unwrap();
	let name_field = Field::new("name", None, Schema::Str(gen_rand_str()));
	let mut map = BTreeMap::new();
	map.insert("SomeData".to_owned(), Schema::Float(234.455));
	let map_field = Field::new("foo", None, Schema::Map(map));
	let inner_rec = RecordSchema::new("id_rec", None, vec![Field::new("id", None, Schema::Long(3i64))]);
	let outer_rec = RecordSchema::new("dashboard_stats", None, vec![name_field, map_field,  Field::new("inner_rec", None, Schema::Record(inner_rec))]);
	let _ = data_writer.write(outer_rec);
}

#[test]
fn test_write_record() {
	let schema_file = "tests/schemas/record_schema.avsc";
	let rec_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/record_encoded.avro";
	let writer = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(rec_schema, writer, Codecs::Null).unwrap();
	let field0 = Field::new("name", None, Schema::Str(gen_rand_str()));
	let field1 = Field::new("canFrame", None, Schema::Long(34534));
	let field2 = Field::new("gps", None, Schema::Long(7673));
	let field3 = Field::new("lsmsensor", None, Schema::Long(2554));
	let mut map = BTreeMap::new();
	map.insert("some".to_string(), Schema::Str("junk".to_string()));
	let field4 = Field::new("map", None, Schema::Map(map));
	let record = RecordSchema::new("dashboard_stats", None, vec![field0, field1, field2, field3, field4]);
	let _ = data_writer.write(record);
}
