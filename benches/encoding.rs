
#![feature(test)]

extern crate ravro;
extern crate rand;

use ravro::datafile::{DataWriter, Codecs};

use std::fs::OpenOptions;
use ravro::types::Schema;
use ravro::schema::AvroSchema;

use std::collections::BTreeMap;

use ravro::complex::{RecordSchema, Field};

extern crate test;
use test::Bencher;

use rand::StdRng;
use rand::Rng;

pub fn gen_rand_str() -> String {
	let mut std_rng = StdRng::new().unwrap();
	let ascii_iter = std_rng.gen_ascii_chars();
	ascii_iter.take(20).collect()
}

#[bench]
fn bench_write_nested_record(b: &mut Bencher) {
	let schema_file = "tests/schemas/nested_schema.avsc";
	let rec_schema = AvroSchema::from_file(schema_file).unwrap();
	let datafile_name = "tests/encoded/nested_encoded.avro";
	let writer = OpenOptions::new().write(true).create(true).open(datafile_name).unwrap();
	let mut data_writer = DataWriter::new(rec_schema, writer, Codecs::Snappy).unwrap();
	let name_field = Field::new("name", None, Schema::Str(gen_rand_str()));

	let mut map = BTreeMap::new();
	map.insert("Adfwf".to_owned(), Schema::Float(234.455));

	let map_field = Field::new("foo", None, Schema::Map(map));
	let inner_rec = RecordSchema::new("id_rec", None, vec![Field::new("id", None, Schema::Long(3i64))]);
	let outer_rec = RecordSchema::new("dashboard_stats", None, vec![name_field, map_field,  Field::new("inner_rec", None, Schema::Record(inner_rec))]);
	let mut writer_bench = |rec| {data_writer.write(rec)};
    b.iter(|| writer_bench(outer_rec.clone()));
}