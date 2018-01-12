
extern crate ravro;
extern crate rand;

use ravro::datafile::{DataWriter, Codecs};
use std::fs::OpenOptions;
use ravro::types::Schema;
use ravro::schema::AvroSchema;
use std::collections::BTreeMap;
use ravro::complex::{RecordSchema, Field, EnumSchema};
use std::io::Write;

use rand::{thread_rng, Rng};
use std::str;
use std::process::Command;

type JournalRecord = BTreeMap<String, String>;

pub fn get_java_tool_output(encoded: &str) -> Result<String, ()> {
    let a = Command::new("java")
            .args(&["-jar", "avro-tools-1.8.2.jar", "tojson", encoded])
            .output()
            .expect("failed to execute process");
    str::from_utf8(&a.stdout).map_err(|_| ()).map(|s| s.to_string())
}

fn main() {
	let rec_schema = AvroSchema::from_str(r#"{"type": "array", "items": {"type": "map", "values": "string"}}"#).unwrap();
	let datafile_name = "tests/encoded/journal_record.avro";
	let mut data_writer = DataWriter::new(rec_schema, Codecs::Snappy).unwrap();
    let mut btree_map_one = BTreeMap::new();
    let mut btree_map_two = BTreeMap::new();
    btree_map_two.insert("two_key".to_owned(), "two_val".to_owned());
    btree_map_one.insert("one_key".to_owned(), "one_val".to_owned());
    let mut v = vec![btree_map_one, btree_map_two];
    data_writer.write(v);
    data_writer.commit_block();
    data_writer.commit_block();
    data_writer.flush_to_disk(datafile_name);
    assert_eq!(Ok("[{\"one_key\":\"one_val\"},{\"two_key\":\"two_val\"}]\n".to_string()), get_java_tool_output(datafile_name));
}
