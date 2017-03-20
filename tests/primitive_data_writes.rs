extern crate fresh_avro;
extern crate serde_json;

use fresh_avro::datafile::DataWriter;

use std::fs::OpenOptions;
use fresh_avro::types::Schema;
use fresh_avro::schema::AvroSchema;

use std::collections::BTreeMap;

use serde_json::{Value};

use fresh_avro::complex::{RecordSchema, Field};

#[test]
fn test_write_string() {
	let schema_file = "tests/schemas/string_schema.avsc";
	let string_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/string_encoded.avro";
	let mut writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
    let mut data_writer = DataWriter::new(string_schema, writer).unwrap();
    data_writer.write_string("abcd".to_string());
    data_writer.write_string("efgh".to_string());
}

#[test]
fn test_write_long() {
	let schema_file = "tests/schemas/long_schema.avsc";
	let long_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/long_encoded.avro";
	let mut writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(long_schema, writer).unwrap();
	data_writer.write_long(56);
	data_writer.write_long(-234534);
}

#[test]
fn test_write_float() {
	let schema_file = "tests/schemas/float_schema.avsc";
	let float_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/float_encoded.avro";
	let mut writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(float_schema, writer).unwrap();
	data_writer.write_float(534.254);
	data_writer.write_float(367.325344);
}

#[test]
fn test_write_double() {
	let schema_file = "tests/schemas/double_schema.avsc";
	let double_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/double_encoded.avro";
	let mut writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(double_schema, writer).unwrap();
	data_writer.write_double(5334444343.3435345254);
	data_writer.write_double(3675465665544.32533444);
}

#[test]
fn test_write_map() {
	let schema_file = "tests/schemas/map_schema.avsc";
	let map_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/map_encoded.avro";
	let mut writer = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(map_schema, writer).unwrap();
	let mut map = BTreeMap::new();
	map.insert("A".to_owned(), Schema::Double(234.455));
	data_writer.write_map(Schema::Map(map));
}

#[test]
fn test_write_record() {
	let schema_file = "tests/schemas/record_schema.avsc";
	let map_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/record_encoded.avro";
	let mut writer = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(map_schema, writer).unwrap();
	let field0 = Field::new("name", None, Schema::Str("hello".to_string()));
	let field1 = Field::new("canFrame", None, Schema::Long(34534));
	let field2 = Field::new("gps", None, Schema::Long(7673));
	let field3 = Field::new("lsmsensor", None, Schema::Long(2554));
	let mut map = BTreeMap::new();
	map.insert("some".to_string(), Schema::Str("junk".to_string()));
	let field4 = Field::new("map", None, Schema::Map(map));
	let mut record = RecordSchema::new("dashboard_stats", None, vec![field0, field1, field2, field3, field4]);
	data_writer.write_record(record);
}

