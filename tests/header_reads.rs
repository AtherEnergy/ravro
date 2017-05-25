extern crate ravro;

use ravro::reader::{AvroReader, BlockReader};
use ravro::types::Schema;
use ravro::datafile::DataWriter;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;

#[test]
fn test_header_read() {
    let data_file = "tests/encoded/mapmap_encoded.avro";
    let mut writer = DataWriter::from_file("tests/schemas/mapmap_schema.avsc").unwrap();
    let mut map = BTreeMap::new();
    let mut inner_map = BTreeMap::new();
    inner_map.insert("one".to_string(), Schema::Double(23.));
    map.insert("hello".to_string(), Schema::Map(inner_map));
    writer.write(map);
    writer.commit_block();
    let buf = writer.swap_buffer();
    let mut f = OpenOptions::new().read(true).write(true).create(true).open(data_file).unwrap();
    let _ = f.write_all(buf.into_inner().as_slice());
    let mut reader = AvroReader::from_path("tests/encoded/mapmap_encoded.avro").unwrap();
    let it: BlockReader<Schema> = reader.iter_block();
}

#[test]
fn header_record_read() {
    let mut reader = AvroReader::from_path("tests/encoded/record_encoded.avro").unwrap();
    let it: BlockReader<Schema> = reader.iter_block();
}

// #[test]
// fn compressed_reads_correctly() {
//     let a = 
// }