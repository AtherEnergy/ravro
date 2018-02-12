#![warn(unused_variables, unused_must_use)]

extern crate ravro;

mod common;

use common::test_writer;
use ravro::writer::{AvroWriter, Codec};

#[test]
fn write_null() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/null_schema.avsc";
		let datafile_name = "tests/encoded/null_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(());
		let _ = data_writer.write(());
		let _ = data_writer.commit_block();
		let _ = data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("null\nnull\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_bool() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/bool_schema.avsc";
		let datafile_name = "tests/encoded/bool_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(true);
		let _ = data_writer.write(false);
		let _ = data_writer.commit_block();
		let _ = data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("true\nfalse\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_int() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/int_schema.avsc";
		let datafile_name = "tests/encoded/int_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(3454).unwrap();
		let _ = data_writer.write(567561).unwrap();
		let _ = data_writer.commit_block();
		let _ = data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("3454\n567561\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_string() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/string_schema.avsc";
		let datafile_name = "tests/encoded/string_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write("abcd".to_string());
		let _ = data_writer.write("efgh".to_string());
		let _ = data_writer.commit_block();
		let _ = data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("\"abcd\"\n\"efgh\"\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_bytes() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/bytes_schema.avsc";
		let datafile_name = "tests/encoded/bytes_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(b"ravro".to_vec());
		let _ = data_writer.commit_block();
		let _ = data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("\"ravro\"\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_long() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/long_schema.avsc";
		let datafile_name = "tests/encoded/long_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(1);
		let _ = data_writer.write(2);
		let _ = data_writer.write(3);
		let _ = data_writer.write(4);
		let _ = data_writer.write(5);
		let _ = data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("1\n2\n3\n4\n5\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_float() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/float_schema.avsc";
		let datafile_name = "tests/encoded/float_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(54.254f32);
		let _ = data_writer.write(7.325344f32);
		let _ = data_writer.commit_block();
		let _ = data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("54.254\n7.325344\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_double() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/double_schema.avsc";
		let datafile_name = "tests/encoded/double_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(3.14);
		let _ = data_writer.write(3675465665544.32533444);
		let _ = data_writer.commit_block();
		let _ = data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("3.14\n3.675465665544325E12\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}
