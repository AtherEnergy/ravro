
// TODO enable when reading is fully implemented

// #![warn(unused_variables, unused_must_use)]

// extern crate ravro;

// use ravro::reader::{AvroReader, BlockReader};
// use ravro::types::Schema;
// use ravro::writer::AvroWriter;
// use std::collections::BTreeMap;

// #[test]
// fn test_header_read() {
//     let mut writer = AvroWriter::from_schema("tests/schemas/mapmap_schema.avsc").unwrap();
//     let mut map = BTreeMap::new();
//     let mut inner_map = BTreeMap::new();
//     inner_map.insert("one".to_string(), Schema::Double(23.));
//     map.insert("hello".to_string(), Schema::Map(inner_map));
//     let _ = writer.write(map);
//     writer.flush_to_disk("tests/encoded/mapmap_encoded.avro");
//     // Read the data
//     let reader = AvroReader::from_path("tests/encoded/mapmap_encoded.avro").unwrap();
//     let mut it: BlockReader<Schema> = reader.iter_block();
//     assert!(it.next().is_some());
// }
