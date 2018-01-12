
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

type JournalRecord = BTreeMap<String, String>;

fn main() {
	let schema_file = "tests/schemas/journal_schema.avsc";
	let rec_schema = AvroSchema::from_file(schema_file).unwrap();
    println!("The schema {:?}", rec_schema);
	let datafile_name = "tests/encoded/journal_record.avro";
	let mut data_writer = DataWriter::new(rec_schema, Codecs::Snappy).unwrap();
    let mut btree_map = BTreeMap::new();
    // let random_key_5 = 
    for i in 0..100000 {
        let key: String = thread_rng().gen_ascii_chars().take(5).collect();
        let value: String = thread_rng().gen_ascii_chars().take(5).collect();
        btree_map.insert(key, value);
    }
    // btree_map.insert("Hello".to_owned(), "World".to_owned());
    // btree_map.insert("Foo".to_owned(), "Bar".to_owned());
    // btree_map.insert("ABC".to_owned(), "DEF".to_owned());
    // btree_map.insert("GHI".to_owned(), "JKL".to_owned());
    data_writer.write(btree_map);
    data_writer.commit_block();
    data_writer.commit_block();
    data_writer.flush_to_disk(datafile_name);
}
