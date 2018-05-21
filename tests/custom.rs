#![warn(unused_variables, unused_must_use)]

extern crate ravro;

use std::collections::BTreeMap;
use ravro::Codec;
mod common;
use common::test_writer;
use ravro::ToRecord;
use std::fs::OpenOptions;
use std::io::Write;

#[test]
fn write_custom_btree_map() {
	for codec in vec![Codec::Null, Codec::Snappy, Codec::Deflate].into_iter() {
        let mut datafile = "tests/encoded/custom_btreemap.avro";
        let schema_file = "tests/schemas/custom_btreemap.avsc";
		let mut data_writer = test_writer(schema_file, codec);
        let mut bmap = BTreeMap::new();
        bmap.insert("a".to_string(), "a_value".to_string());
        bmap.insert("b".to_string(), "23423".to_string());
        bmap.insert("c".to_string(), "1".to_string());

        let record = bmap.to_avro("dummy", data_writer.get_schema()).unwrap();
		let _ = data_writer.write(record);

		let datafile_buffer = data_writer.take_datafile().unwrap();
		let mut open_options = OpenOptions::new().truncate(true).write(true).create(true).open(datafile).unwrap();
    	let _ = open_options.write_all(datafile_buffer.as_slice());
		assert_eq!(Ok("{\"a\":\"a_value\",\"b\":144419,\"c\":true}\n".to_string()), common::get_java_tool_output(datafile));
	}
}