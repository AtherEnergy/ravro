#![warn(unused_variables, unused_must_use)]

extern crate ravro;

use ravro::{AvroWriter, Codec};
use ravro::Type;
use std::collections::HashMap;
use ravro::complex::{Record, Field, Enum};

mod common;
use common::test_writer;
use std::fs::OpenOptions;
use std::io::Write;

#[test]
fn write_map() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let datafile_name = "tests/encoded/map_encoded.avro";
		::std::fs::remove_dir_all(datafile_name);
		let schema_file = "tests/schemas/map_schema.avsc";
		let mut data_writer = test_writer(schema_file, codec);
		let mut map = HashMap::new();
		map.insert("A".to_owned(), Type::Double(234.455));
		let _ = data_writer.write(Type::Map(map.clone()));

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("{\"A\":234.455}\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_nested_record() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/nested_schema.avsc";
		let datafile_name = "tests/encoded/nested_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let name_field = Field::new("name", Type::Str("nested_record_example".to_string()));
		let mut map = HashMap::new();
		map.insert("SomeData".to_owned(), Type::Float(234.455));
		let map_field = Field::new("foo", Type::Map(map));
		let inner_rec = Record::new("id_rec", None, vec![Field::new("id", Type::Long(3i64))]);
		let outer_rec = Record::new("dashboard_stats",
										None,
										vec![name_field, map_field,  Field::new("inner_rec", Type::Record(inner_rec))]);
		let _ = data_writer.write(outer_rec);

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("{\"name\":\"nested_record_example\",\"foo\":{\"SomeData\":234.455},\"inner_rec\":{\"id\":3}}\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_record() {
	for codec in vec![Codec::Null].into_iter() {
		let schema_file = "tests/schemas/record_schema.avsc";
		let datafile_name = "tests/encoded/record_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let field0 = Field::new("name", Type::Str("record_example".to_string()));
		let field1 = Field::new("canFrame", Type::Long(34534));
		let field2 = Field::new("gps", Type::Long(7673));
		let field3 = Field::new("lsmsensor", Type::Long(2554));
		let field4 = Field::new("map", Type::Str("junk".to_string()));
		let record = Record::new("dashboard_stats", None, vec![field0, field1, field2, field3, field4]);
		let _ = data_writer.write(record);

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("{\"name\":\"record_example\",\"canFrame\":34534,\"gps\":7673,\"lsmsensor\":2554,\"map\":\"junk\"}\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_array() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/array_schema.avsc";
		let datafile_name = "tests/encoded/array_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let a: Type = "a".to_string().into();
		let b: Type = "b".to_string().into();
		let c: Type = "c".to_string().into();
		let d: Type = "d".to_string().into();
		let _ = data_writer.write(vec![a,b,c,d]);

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("[\"a\",\"b\",\"c\",\"d\"]\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_enum() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/enum_schema.avsc";
		let datafile_name = "tests/encoded/enum_encoded.avro";
		let mut enum_scm = Enum::new("Foo", &["CLUBS", "SPADE", "DIAMOND"]);
		enum_scm.set_value("DIAMOND");
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(Type::Enum(enum_scm));

    	let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile_name).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("\"DIAMOND\"\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}
