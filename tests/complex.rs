#![warn(unused_variables, unused_must_use)]

extern crate ravro;

use ravro::{AvroWriter, Codec};
use ravro::types::Schema;
use std::collections::BTreeMap;
use ravro::complex::{RecordSchema, Field, EnumSchema};

mod common;
use common::test_writer;

#[test]
fn write_map() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let datafile_name = "tests/encoded/map_encoded.avro";
		let schema_file = "tests/schemas/map_schema.avsc";
		let mut data_writer = test_writer(schema_file, codec);
		let mut map = BTreeMap::new();
		map.insert("A".to_owned(), Schema::Double(234.455));
		let _ = data_writer.write(Schema::Map(map));
		let _ = data_writer.commit_block();
		data_writer.flush_to_disk("tests/encoded/map_encoded.avro");
		assert_eq!(Ok("{\"A\":234.455}\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_nested_record() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/nested_schema.avsc";
		let datafile = "tests/encoded/nested_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let name_field = Field::new_for_encoding("name", None, Schema::Str("nested_record_example".to_string()));
		let mut map = BTreeMap::new();
		map.insert("SomeData".to_owned(), Schema::Float(234.455));
		let map_field = Field::new_for_encoding("foo", None, Schema::Map(map));
		let inner_rec = RecordSchema::new("id_rec", None, vec![Field::new_for_encoding("id", None, Schema::Long(3i64))]);
		let outer_rec = RecordSchema::new("dashboard_stats",
										None,
										vec![name_field, map_field,  Field::new_for_encoding("inner_rec", None, Schema::Record(inner_rec))]);
		let _ = data_writer.write(outer_rec);
		let _ = data_writer.commit_block();
		data_writer.flush_to_disk(datafile);
		assert_eq!(Ok("{\"name\":\"nested_record_example\",\"foo\":{\"SomeData\":234.455},\"inner_rec\":{\"id\":3}}\n".to_string()), common::get_java_tool_output(datafile));
	}
}

#[test]
fn write_record() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/record_schema.avsc";
		let datafile_name = "tests/encoded/record_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let field0 = Field::new_for_encoding("name", None, Schema::Str("record_example".to_string()));
		let field1 = Field::new_for_encoding("canFrame", None, Schema::Long(34534));
		let field2 = Field::new_for_encoding("gps", None, Schema::Long(7673));
		let field3 = Field::new_for_encoding("lsmsensor", None, Schema::Long(2554));
		let mut map = BTreeMap::new();
		map.insert("some".to_string(), Schema::Str("junk".to_string()));
		let field4 = Field::new_for_encoding("map", None, Schema::Map(map));
		let record = RecordSchema::new("dashboard_stats", None, vec![field0, field1, field2, field3, field4]);
		let _ = data_writer.write(record);
		let _ = data_writer.commit_block();
		data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("{\"name\":\"record_example\",\"canFrame\":34534,\"gps\":7673,\"lsmsensor\":2554,\"map\":{\"some\":\"junk\"}}\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_array() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/array_schema.avsc";
		let datafile_name = "tests/encoded/array_encoded.avro";
		let mut data_writer = test_writer(schema_file, codec);
		let a: Schema = "a".to_string().into();
		let b: Schema = "b".to_string().into();
		let c: Schema = "c".to_string().into();
		let d: Schema = "d".to_string().into();
		let _ = data_writer.write(vec![a,b,c,d]);
		let _ = data_writer.commit_block();
		data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("[\"a\",\"b\",\"c\",\"d\"]\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}

#[test]
fn write_enum() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
		let schema_file = "tests/schemas/enum_schema.avsc";
		let datafile_name = "tests/encoded/enum_encoded.avro";
		let mut enum_scm = EnumSchema::new("Foo", &["CLUBS", "SPADE", "DIAMOND"]);
		enum_scm.set_value("DIAMOND");
		let mut data_writer = test_writer(schema_file, codec);
		let _ = data_writer.write(Schema::Enum(enum_scm));
		let _ = data_writer.commit_block();
		data_writer.flush_to_disk(datafile_name);
		assert_eq!(Ok("\"DIAMOND\"\n".to_string()), common::get_java_tool_output(datafile_name));
	}
}
