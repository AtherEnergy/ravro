
use std::fs::OpenOptions;
use conversion::Decoder;
use writer::SyncMarker;
use schema::AvroSchema;
use writer::Header;

/// `DataWriter` reads an avro data file
pub struct DataReader {
	/// The header parsed from the schema
	pub header: Header,
	/// No of blocks that has been written
	pub block_cnt: u64,
	/// Buffer used to hold in flight data before writing them to an
	/// avro data file
	pub inmemory_buf: Vec<u8>
}

impl DataReader {
	pub fn from_path(path: &str) {
		let mut src_avro = OpenOptions::new().read(true).open(path).unwrap();
		let hdr = Header::new().decode(&mut src_avro);
		let decoded_schema = hdr.unwrap().get_schema();
		print!("{:?}",decoded_schema);
	}
}