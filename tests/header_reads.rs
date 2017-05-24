extern crate ravro;

use ravro::reader::{AvroReader, BlockReader};
use ravro::types::Schema;

#[test]
fn test_header_read() {
    let mut reader = AvroReader::from_path("tests/encoded/map_encoded.avro").unwrap();
    let it: BlockReader<Schema> = reader.iter_block();
    // println!("{:?}", reader);
}
