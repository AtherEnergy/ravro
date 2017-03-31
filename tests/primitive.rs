extern crate ravro;
extern crate rand;

use ravro::datafile::{DataWriter, Codecs};
use std::fs::OpenOptions;
use ravro::schema::AvroSchema;

#[test]
fn test_write_null() {
	let schema_file = "tests/schemas/null_schema.avsc";
	let null_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/null_encoded.avro";
	let writer = OpenOptions::new().write(true)
								   .create(true)
								   .open(datafile_name).unwrap();
    let mut data_writer = DataWriter::new(null_schema, writer, Codecs::Null).unwrap();
    let _ = data_writer.write_null();
    let _ = data_writer.write_null();
}

#[test]
fn test_bool_encode_decode() {
	let schema_file = "tests/schemas/bool_schema.avsc";
	let bool_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/bool_encoded.avro";
	let writer = OpenOptions::new().write(true)
								   .create(true)
								   .open(datafile_name).unwrap();
    let mut data_writer = DataWriter::new(bool_schema, writer, Codecs::Null).unwrap();
    let _ = data_writer.write(true);
    let _ = data_writer.write(false);
}

#[test]
fn test_int_encode_decode() {
	let schema_file = "tests/schemas/int_schema.avsc";
	let int_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/int_encoded.avro";
	let writer = OpenOptions::new().write(true)
								   .create(true)
								   .open(datafile_name).unwrap();
    let mut data_writer = DataWriter::new(int_schema, writer, Codecs::Null).unwrap();
    let _ = data_writer.write(3454);
    let _ = data_writer.write(567561);
}

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
fn test_write_bytes() {
	let schema_file = "tests/schemas/bytes_schema.avsc";
	let bytes_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/bytes_encoded.avro";
	let writer = OpenOptions::new().write(true)
								   .create(true)
								   .open(datafile_name).unwrap();
    let mut data_writer = DataWriter::new(bytes_schema, writer, Codecs::Null).unwrap();
    let _ = data_writer.write(b"ravro".to_vec());
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
