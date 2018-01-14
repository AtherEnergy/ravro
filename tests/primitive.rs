#![warn(unused_variables, unused_must_use)]

extern crate ravro;

mod common;

use ravro::writer::{DataWriter, Codecs};
use ravro::schema::AvroSchema;


#[test]
fn write_null() {
	let schema_file = "tests/schemas/null_schema.avsc";
	let null_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/null_encoded.avro";
    let mut data_writer = DataWriter::new(null_schema, Codecs::Snappy).unwrap();
	let _ = data_writer.write(());
	let _ = data_writer.write(());
    let _ = data_writer.commit_block();
	data_writer.flush_to_disk(datafile_name);
    assert_eq!(Ok("null\nnull\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn write_bool() {
	let schema_file = "tests/schemas/bool_schema.avsc";
	let bool_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/bool_encoded.avro";
    let mut data_writer = DataWriter::new(bool_schema, Codecs::Snappy).unwrap();
    let _ = data_writer.write(true);
    let _ = data_writer.write(false);
    let _ = data_writer.commit_block();
	data_writer.flush_to_disk(datafile_name);
    assert_eq!(Ok("true\nfalse\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn write_int() {
	let schema_file = "tests/schemas/int_schema.avsc";
	let int_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/int_encoded.avro";
    let mut data_writer = DataWriter::new(int_schema, Codecs::Snappy).unwrap();
    let _ = data_writer.write(3454).unwrap();
    let _ = data_writer.write(567561).unwrap();
    let _ = data_writer.commit_block();
	data_writer.flush_to_disk(datafile_name);
    assert_eq!(Ok("3454\n567561\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn write_string() {
	let schema_file = "tests/schemas/string_schema.avsc";
	let string_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/string_encoded.avro";
    let mut data_writer = DataWriter::new(string_schema, Codecs::Snappy).unwrap();
    let _ = data_writer.write("abcd".to_string());
    let _ = data_writer.write("efgh".to_string());
    let _ = data_writer.commit_block();
	data_writer.flush_to_disk(datafile_name);
    assert_eq!(Ok("\"abcd\"\n\"efgh\"\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn write_bytes() {
	let schema_file = "tests/schemas/bytes_schema.avsc";
	let bytes_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/bytes_encoded.avro";
    let mut data_writer = DataWriter::new(bytes_schema, Codecs::Snappy).unwrap();
    let _ = data_writer.write(b"ravro".to_vec());
    let _ = data_writer.commit_block();
	data_writer.flush_to_disk(datafile_name);
    assert_eq!(Ok("\"ravro\"\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn write_long() {
	use std::fs;
	let _ = fs::remove_file("tests/encoded/long_encoded.avro");
	let schema_file = "tests/schemas/long_schema.avsc";
	let long_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/long_encoded.avro";
	let mut data_writer = DataWriter::new(long_schema, Codecs::Null).unwrap();
	let _ = data_writer.write(1);
	let _ = data_writer.write(2);
	let _ = data_writer.write(3);
	let _ = data_writer.write(4);
	let _ = data_writer.write(5);
	let _ = data_writer.commit_block();
	data_writer.flush_to_disk(datafile_name);
	assert_eq!(Ok("1\n2\n3\n4\n5\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn write_float() {
	let schema_file = "tests/schemas/float_schema.avsc";
	let float_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/float_encoded.avro";
	let mut data_writer = DataWriter::new(float_schema, Codecs::Snappy).unwrap();
	let _ = data_writer.write(54.254f32);
	let _ = data_writer.write(7.325344f32);
	let _ = data_writer.commit_block();
	data_writer.flush_to_disk(datafile_name);
	assert_eq!(Ok("54.254\n7.325344\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn write_double() {
	let schema_file = "tests/schemas/double_schema.avsc";
	let double_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/double_encoded.avro";
	let mut data_writer = DataWriter::new(double_schema, Codecs::Snappy).unwrap();
	let _ = data_writer.write(3.14);
	let _ = data_writer.write(3675465665544.32533444);
	let _ = data_writer.commit_block();
	data_writer.flush_to_disk(datafile_name);
	assert_eq!(Ok("3.14\n3.675465665544325E12\n".to_string()), common::get_java_tool_output(datafile_name));
}
