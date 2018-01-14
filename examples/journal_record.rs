
extern crate ravro;
extern crate rand;
extern crate loggerv;

use ravro::writer::{DataWriter, Codecs};
use ravro::schema::AvroSchema;
use std::collections::BTreeMap;

use std::str;
use std::process::Command;

pub fn get_java_tool_output(encoded: &str) -> Result<String, ()> {
    let a = Command::new("java")
            .args(&["-jar", "avro-tools-1.8.2.jar", "tojson", encoded])
            .output()
            .expect("failed to execute process");
    str::from_utf8(&a.stdout).map_err(|_| ()).map(|s| s.to_string())
}

// #[macro_export]
// macro_rules! impl_extract {
//     ($on_type:ident, $($type:tt $method_name:ident $variant:path),*) => {
//         impl $on_type {
//             $(pub fn $method_name<'a>(&'a self) -> &'a $type {
//                 println!("hello");
//                 if let &$variant(ref t) = self {
//                     &t
//                 } else {
//                     unreachable!();
//                 }
//             })*
//         }
//     };
// }

// enum MySchema {
//     Int(i32),
//     Str(String)
// }

// // struct MySchema;
// impl_extract!(MySchema, &'a u32 int_ref MySchema::Int);

fn main() {
    loggerv::init_with_verbosity(4).unwrap();
	let rec_schema = AvroSchema::from_str(r#"{"type": "array", "items": {"type": "map", "values": "string"}}"#).unwrap();
	let datafile_name = "tests/encoded/journal_record.avro";
	let mut data_writer = DataWriter::new(rec_schema, Codecs::Snappy).unwrap();
    let mut btree_map_one = BTreeMap::new();
    let mut btree_map_two = BTreeMap::new();
    btree_map_two.insert("two_key".to_owned(), "two_val".to_owned());
    btree_map_one.insert("one_key".to_owned(), "one_val".to_owned());
    let v = vec![btree_map_one, btree_map_two];
    let _ = data_writer.write(v);
    let _ = data_writer.commit_block();
    data_writer.flush_to_disk(datafile_name);
    assert_eq!(Ok("[{\"one_key\":\"one_val\"},{\"two_key\":\"two_val\"}]\n".to_string()), get_java_tool_output(datafile_name));
    
}
