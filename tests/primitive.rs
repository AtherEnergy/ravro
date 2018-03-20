#![warn(unused_variables, unused_must_use)]

extern crate ravro;

mod common;

use common::test_writer;
use ravro::writer::Codec;
use std::fs::OpenOptions;
use std::io::Write;

#[test]
fn write_null() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/null_schema.avsc";
		let datafile_name = "tests/encoded/null_encoded.avro";
		let _ = ::std::fs::remove_dir_all(datafile_name);
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(());
		let _ = data_writer.write(());

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("null\nnull\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_bool() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/bool_schema.avsc";
		let datafile_name = "tests/encoded/bool_encoded.avro";
		let _ = ::std::fs::remove_dir_all(datafile_name);
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(true);
		let _ = data_writer.write(false);

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("true\nfalse\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_int() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/int_schema.avsc";
		let datafile_name = "tests/encoded/int_encoded.avro";
		let _ = ::std::fs::remove_dir_all(datafile_name);
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(3454).unwrap();
		let _ = data_writer.write(567561).unwrap();

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("3454\n567561\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_string() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/string_schema.avsc";
		let datafile_name = "tests/encoded/string_encoded.avro";
		let _ = ::std::fs::remove_dir_all(datafile_name);
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write("abcd".to_string());
		let _ = data_writer.write("efgh".to_string());

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("\"abcd\"\n\"efgh\"\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_bytes() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/bytes_schema.avsc";
		let datafile_name = "tests/encoded/bytes_encoded.avro";
		let _ = ::std::fs::remove_dir_all(datafile_name);
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(b"ravro".to_vec());

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("\"ravro\"\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_long() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/long_schema.avsc";
		let datafile_name = "tests/encoded/long_encoded.avro";
		let _ = ::std::fs::remove_dir_all(datafile_name);
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(4354645765756754_i64);
		let _ = data_writer.write(24564564534_i64);
		let _ = data_writer.write(34543645_i64);
		let _ = data_writer.write(424543543_i64);
		let _ = data_writer.write(534543543_i64);

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("4354645765756754\n24564564534\n34543645\n424543543\n534543543\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_float() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/float_schema.avsc";
		let datafile_name = "tests/encoded/float_encoded.avro";
		let _ = ::std::fs::remove_dir_all(datafile_name);
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(54.254f32);
		let _ = data_writer.write(7.325344f32);

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("54.254\n7.325344\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_double() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/double_schema.avsc";
		let datafile_name = "tests/encoded/double_encoded.avro";
		let _ = ::std::fs::remove_dir_all(datafile_name);
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(3.14);
		let _ = data_writer.write(3675465665544.32533444);

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("3.14\n3.675465665544325E12\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}
