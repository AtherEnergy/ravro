extern crate ravro;
extern crate rand;

use ravro::datafile::{DataWriter, Codecs};
use std::fs::OpenOptions;
use ravro::schema::AvroSchema;

#[test]
fn test_write_string() {
	let schema_file = "tests/schemas/string_schema.avsc";
	let string_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/string_encoded.avro";
	let writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
    let mut data_writer = DataWriter::new(string_schema, writer, Codecs::Null).unwrap();
    let _ = data_writer.write("abcd".to_string());
    let _ = data_writer.write("efgh".to_string());
}

#[test]
fn test_write_null() {
	let schema_file = "tests/schemas/null_schema.avsc";
	let null_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/null_encoded.avro";
	let writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
    let mut data_writer = DataWriter::new(null_schema, writer, Codecs::Null).unwrap();
    data_writer.write_null();
    data_writer.write_null();
}

#[test]
fn test_write_long() {
	let schema_file = "tests/schemas/long_schema.avsc";
	let long_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/long_encoded.avro";
	let writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(long_schema, writer, Codecs::Snappy).unwrap();
	let _ = data_writer.write(56);
	let _ = data_writer.write(53654);
}

#[test]
fn test_write_float() {
	let schema_file = "tests/schemas/float_schema.avsc";
	let float_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/float_encoded.avro";
	let writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(float_schema, writer, Codecs::Null).unwrap();
	let _ = data_writer.write(534.254);
	let _ = data_writer.write(367.325344);
}

#[test]
fn test_write_double() {
	let schema_file = "tests/schemas/double_schema.avsc";
	let double_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/double_encoded.avro";
	let writer = OpenOptions::new().write(true)
									   .create(true)
									   .open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(double_schema, writer, Codecs::Null).unwrap();
	let _ = data_writer.write(3.14);
	let _ = data_writer.write(3675465665544.32533444);
}
