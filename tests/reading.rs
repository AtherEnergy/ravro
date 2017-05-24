extern crate ravro;

use ravro::schema::AvroSchema;
use std::fs::OpenOptions;
use std::io::Cursor;
use ravro::datafile::{DataWriter, Codecs};
use std::io::Write;
use ravro::reader::{AvroReader, BlockReader};
use ravro::types::Schema;
use std::fs::File;

fn create_writer(src_schema: &str, encoded_file: &str, codec: Codecs) -> (DataWriter, File) {
    let schema_file = &format!("tests/schemas/{}", src_schema);
    let string_schema = AvroSchema::from_file(schema_file).unwrap();
    let datafile_name = &format!("tests/encoded/{}", encoded_file);
    let mut writer_file = OpenOptions::new().write(true)
                                   .create(true)
                                   .open(datafile_name).unwrap();
    let mut data_writer = DataWriter::new(string_schema, codec).unwrap();
    (data_writer, writer_file)
}

#[test]
fn reading_string_uncompressed() {
    // Write some data
    let (mut data_writer,
         mut writer_file) = create_writer("string_schema.avsc", "string_for_read.avro", Codecs::Null);
    let _ = data_writer.write("Reading".to_string());
    let _ = data_writer.write("avro".to_string());
    let _ = data_writer.write("string".to_string());
    let _ = data_writer.commit_block();
    let _ = writer_file.write_all(&data_writer.swap_buffer().into_inner());
    // Read that data
    let reader = AvroReader::from_path("tests/encoded/string_for_read.avro").unwrap();
    let mut a: BlockReader<Schema> = reader.iter_block();
    assert_eq!(a.next().unwrap().string_ref(), "Reading".to_string());
    assert_eq!(a.next().unwrap().string_ref(), "avro".to_string());
    assert_eq!(a.next().unwrap().string_ref(), "string".to_string());
    assert_eq!(a.next(), None);
}

#[test]
fn reading_map_uncompressed() {
    let (mut data_writer, mut writer_file) = create_writer("map_schema.avsc", "map_for_read.avro", Codecs::Null);
    data_writer.write(344).unwrap();
    // let _ = data_writer.write("")
}

#[test]
fn reading_bool_uncompressed() {
    let (mut data_writer,
         mut writer_file) = create_writer("bool_schema.avsc", "bool_for_read.avro", Codecs::Null);
    let _ = data_writer.write(true);
    let _ = data_writer.write(false);
    let _ = data_writer.write(false);
    let _ = data_writer.commit_block();
    let _ = writer_file.write_all(&data_writer.swap_buffer().into_inner());

    let reader = AvroReader::from_path("tests/encoded/bool_for_read.avro").unwrap();
    let mut a: BlockReader<Schema> = reader.iter_block();
    assert_eq!(a.next().unwrap().bool_ref(), true);
    assert_eq!(a.next().unwrap().bool_ref(), false);
    assert_eq!(a.next().unwrap().bool_ref(), false);
    assert_eq!(a.next(), None);
}

#[test]
fn reading_bool_compressed() {
    let (mut data_writer,
             mut writer_file) = create_writer("bool_schema.avsc", "bool_for_read_comp.avro", Codecs::Snappy);
    let _ = data_writer.write(true);
    let _ = data_writer.write(false);
    let _ = data_writer.write(false);
    let _ = data_writer.commit_block();
    let _ = writer_file.write_all(&data_writer.swap_buffer().into_inner());

    // Read that data
    let reader = AvroReader::from_path("tests/encoded/bool_for_read_comp.avro").unwrap();
    let mut a: BlockReader<Schema> = reader.iter_block();
    assert_eq!(a.next().unwrap().bool_ref(), true);
    // assert_eq!(a.next().unwrap().bool_ref(), false);
    // assert_eq!(a.next().unwrap().bool_ref(), false);
    // assert_eq!(a.next(), None);
}

#[test]
fn reading_string_compressed() {
    let (mut data_writer,
             mut writer_file) = create_writer("string_schema.avsc", "string_for_read_comp.avro", Codecs::Snappy);
    let _ = data_writer.write("Reading".to_string());
    let _ = data_writer.write("avro".to_string());
    let _ = data_writer.write("string".to_string());
    let _ = data_writer.commit_block();
    let _ = writer_file.write_all(&data_writer.swap_buffer().into_inner());
    // Read that data
    let reader = AvroReader::from_path("tests/encoded/string_for_read_comp.avro").unwrap();
    let mut a: BlockReader<Schema> = reader.iter_block();
    assert_eq!(a.next().unwrap().string_ref(), "Reading".to_string());
    // TODO Fix consequent reads
    // assert_eq!(a.next().unwrap().string_ref(), "avro".to_string());
    // assert_eq!(a.next().unwrap().string_ref(), "string".to_string());
}
