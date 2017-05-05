extern crate ravro;
extern crate rand;

use ravro::writer::{DataWriter, Codecs};

use std::fs::OpenOptions;
use ravro::types::Schema;
use ravro::schema::AvroSchema;

use std::collections::BTreeMap;

use ravro::complex::{RecordSchema, Field, EnumSchema};

use std::io::Write;
use std::io::Cursor;

mod common;

#[test]
fn test_write_map() {
	let schema_file = "tests/schemas/map_schema.avsc";
	let map_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/map_encoded.avro";
	let mut writer_file = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut writer = Cursor::new(Vec::new());
	let mut data_writer = DataWriter::new(map_schema, &mut writer, Codecs::Snappy).unwrap();
	let mut map = BTreeMap::new();
	map.insert("A".to_owned(), Schema::Double(234.455));
	let _ = data_writer.write(Schema::Map(map));
	let _ = data_writer.commit_block(&mut writer);
	let _ = writer_file.write_all(&writer.into_inner());
	assert_eq!(Ok("{\"A\":234.455}\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn write_nested_record() {
	let schema_file = "tests/schemas/nested_schema.avsc";
	let rec_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/nested_encoded.avro";
	let mut writer_file = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut writer = Cursor::new(Vec::new());
	let mut data_writer = DataWriter::new(rec_schema, &mut writer, Codecs::Snappy).unwrap();
	let name_field = Field::new("name", None, Schema::Str("nested_record_example".to_string()));
	let mut map = BTreeMap::new();
	map.insert("SomeData".to_owned(), Schema::Float(234.455));
	let map_field = Field::new("foo", None, Schema::Map(map));
	let inner_rec = RecordSchema::new("id_rec", None, vec![Field::new("id", None, Schema::Long(3i64))]);
	let outer_rec = RecordSchema::new("dashboard_stats", None, vec![name_field, map_field,  Field::new("inner_rec", None, Schema::Record(inner_rec))]);
	let _ = data_writer.write(outer_rec);
	let _ = data_writer.commit_block(&mut writer);
	let _ = writer_file.write_all(&writer.into_inner());
	assert_eq!(Ok("{\"name\":\"nested_record_example\",\"foo\":{\"SomeData\":234.455},\"inner_rec\":{\"id\":3}}\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn test_write_record() {
	let schema_file = "tests/schemas/record_schema.avsc";
	let rec_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/record_encoded.avro";
	let mut writer_file = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut writer = Cursor::new(Vec::new());
	let mut data_writer = DataWriter::new(rec_schema, &mut writer, Codecs::Null).unwrap();
	let field0 = Field::new("name", None, Schema::Str("record_example".to_string()));
	let field1 = Field::new("canFrame", None, Schema::Long(34534));
	let field2 = Field::new("gps", None, Schema::Long(7673));
	let field3 = Field::new("lsmsensor", None, Schema::Long(2554));
	let mut map = BTreeMap::new();
	map.insert("some".to_string(), Schema::Str("junk".to_string()));
	let field4 = Field::new("map", None, Schema::Map(map));
	let record = RecordSchema::new("dashboard_stats", None, vec![field0, field1, field2, field3, field4]);
	let _ = data_writer.write(record);
	let _ = data_writer.commit_block(&mut writer);
	let _ = writer_file.write_all(&writer.into_inner());
	assert_eq!(Ok("{\"name\":\"record_example\",\"canFrame\":34534,\"gps\":7673,\"lsmsensor\":2554,\"map\":{\"some\":\"junk\"}}\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn test_write_array() {
	let schema_file = "tests/schemas/array_schema.avsc";
	let arr_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/array_encoded.avro";
	let mut writer_file = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut writer = Cursor::new(Vec::new());
	let mut data_writer = DataWriter::new(arr_schema, &mut writer, Codecs::Null).unwrap();
	let a: Schema = "a".to_string().into();
	let b: Schema = "b".to_string().into();
	let c: Schema = "c".to_string().into();
	let d: Schema = "d".to_string().into();
	let _ = data_writer.write(vec![a,b,c,d]);
	let _ = data_writer.commit_block(&mut writer);
	let _ = writer_file.write_all(&writer.into_inner());
	assert_eq!(Ok("[\"a\",\"b\",\"c\",\"d\"]\n".to_string()), common::get_java_tool_output(datafile_name));
}

#[test]
fn test_enum_writes() {
	let schema_file = "tests/schemas/enum_schema.avsc";
	let arr_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/enum_encoded.avro";
	let mut writer_file = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut enum_scm = EnumSchema::new("Foo", &["CLUBS", "SPADE", "DIAMOND"]);
	enum_scm.set_value("DIAMOND");
	let mut writer = Cursor::new(Vec::new());
	let mut data_writer = DataWriter::new(arr_schema, &mut writer, Codecs::Null).unwrap();
	let _ = data_writer.write(Schema::Enum(enum_scm));
	let _ = data_writer.commit_block(&mut writer);
	let _ = writer_file.write_all(&writer.into_inner());
	assert_eq!(Ok("\"DIAMOND\"\n".to_string()), common::get_java_tool_output(datafile_name));
}
