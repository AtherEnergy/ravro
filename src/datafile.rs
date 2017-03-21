//! This module declares a DataWriter instance which does actual writing of data
//! through give avro schema

#![allow(dead_code,
		 unused_variables,
		 unused_must_use,
		 unused_imports,
		 unused_mut)]

use std::io::{self, Write, Read};
use std::collections::BTreeMap;

use types::{DecodeValue, Schema};
use codec::{Codec, EncodeErr, DecodeErr};
use rand::thread_rng;
use rand::Rng;

use std::fs::OpenOptions;
use complex::RecordSchema;

use serde_json::{Value, from_reader};
use schema::AvroSchema;
use snap::Writer;
use std::path::Path;
use std::str;


const SYNC_MARKER_SIZE: usize = 16;
const MAGIC_BYTES: [u8;4] = [b'O', b'b', b'j', 1 as u8];

/// Compression codec to use on pre-write.
pub enum Codecs {
	Null,
	Deflate,
	Snappy
}

// Errors specific to this module
// TODO add more variants
err_structs!(AvroWriteErr, UnexpectedSchema);

/// DataWriter reads an avro data file
pub struct DataWriter {
	/// The header parsed from the schema
	pub header: Option<Header>,
	/// The in memory representation of AvroSchema
	pub schema: AvroSchema,
	/// No of blocks that has been written
	pub block_cnt: u64,
	/// A Write instance (default `File`) into which bytes will be written.
	pub writer: Box<Write>,
	/// The sync marker read from the data file header
	pub sync_marker: SyncMarker
}

impl DataWriter {
	/// Creates a new `DataWriter` instance which can be used to write data to the provided `Write` instance
	pub fn new<W>(schema: AvroSchema, mut writer: W) -> Result<Self, String>
	where W: Write + 'static {
		let sync_marker = SyncMarker(gen_sync_marker());
		let header = Header::new(&schema, sync_marker.clone());
		header.encode(&mut writer);
		let schema_obj = DataWriter {
			header: Some(header),
			schema: schema,
			writer: Box::new(writer),
			sync_marker: sync_marker,
			block_cnt: 0
		};

		// TODO add sanity checks that we're dealing with a valid avro file.
		// TODO Seek to end if header is already written

		Ok(schema_obj)
	}

	/// Write a avro long data into the writer instance.
	pub fn write_long(&mut self, long: i64) -> Result<(), AvroWriteErr> {	
		let l = Schema::Long(long);
		if !self.header.is_some() {
			// Write header here
		} else {
			let sync_marker = self.sync_marker.clone();
			let mut v = vec![];
			// TODO make this DRY
			match self.header.as_ref().unwrap().get_meta("avro.codec") {
				Ok(Codecs::Null) => {
					commit_block!(l, sync_marker, v);
					return self.writer.write_all(v.as_slice()).map_err(|_| AvroWriteErr)
				}
				Ok(Codecs::Snappy) => {
					let mut comp_buf = Vec::new();
					let mut snap_writer = Writer::new(comp_buf);
					io::copy(&mut &v[..], &mut snap_writer);
					return self.writer.write_all(&mut snap_writer.into_inner().unwrap())
							   .map_err(|_| AvroWriteErr)
				}				
				Ok(Codecs::Deflate) | _ => unimplemented!(),
			}
		}
		Ok(())
	}

	/// Write a avro double data into the writer instance.
	pub fn write_double(&mut self, double: f64) -> Result<(), AvroWriteErr> {
		let d = Schema::Double(double);
		if self.header.is_some() {
			
		} else {
			let sync_marker = self.sync_marker.clone();
			write_block!(self, d, writer, sync_marker);
		}
		Ok(())
	}

	/// Write a avro float data into the writer instance.
	pub fn write_float(&mut self, float: f32) -> Result<(), AvroWriteErr> {
		let f = Schema::Float(float);
		if self.header.is_some() {
			// write header
		} else {
			let sync_marker = self.sync_marker.clone();
			write_block!(self, f, writer, sync_marker);
		}
		Ok(())
	}

	/// Write a avro string data into the writer instance.
	pub fn write_string(&mut self, string: String) -> Result<(), AvroWriteErr> {
		let l = Schema::Str(string.clone());

		if !self.header.is_some() {
			// 
		} else {
			let sync_marker = self.sync_marker.clone();
			let mut v = vec![];
			match self.header.as_ref().unwrap().get_meta("avro.codec") {
				Ok(Codecs::Null) => {
					commit_block!(l, sync_marker, v);
					return self.writer.write_all(v.as_slice()).map_err(|_| AvroWriteErr)
				}
				Ok(Codecs::Snappy) => {
					let mut comp_buf = Vec::new();
					let mut snap_writer = Writer::new(comp_buf);
					io::copy(&mut &v[..], &mut snap_writer);
					return self.writer.write_all(&mut snap_writer.into_inner().unwrap())
							   .map_err(|_| AvroWriteErr)
				}				
				Ok(Codecs::Deflate) | _ => unimplemented!(),
			}
		}
		Ok(())
	}

	/// Write a avro record data into the writer instance.
	pub fn write_record(&mut self, record: RecordSchema) -> Result<(), AvroWriteErr> {
		if self.header.is_some() {

		} else {
			let sync_marker = self.sync_marker.clone();
			let record_schema = Schema::Record(record);
			write_block!(self, record_schema, writer, sync_marker);
		}
		Ok(())
	}

	pub fn write_map(&mut self, map: Schema) -> Result<(), AvroWriteErr> {
		let sync_marker = self.sync_marker.clone();
		write_block!(self, map, writer, sync_marker);
		Ok(())
	}
}

impl From<EncodeErr> for AvroWriteErr {
	fn from(_: EncodeErr) -> AvroWriteErr {
		AvroWriteErr
	}
}

/// Generates 16 bytes of random sequence which gets assigned as the `SyncMarker` for
/// an avro data file
pub fn gen_sync_marker() -> Vec<u8> {
    let mut vec = [0u8; SYNC_MARKER_SIZE];
    thread_rng().fill_bytes(&mut vec[..]);
    vec.to_vec()
}

/// The avro datafile header.
pub struct Header {
	/// The magic byte sequence which serves as the identifier for a valid Avro data file.
	/// It consists of 3 ASCII bytes `O`,`b`,`j` followed by a 1 encoded as a byte.
	pub magic: [u8; 4],
	/// A Map avro schema which stores important metadata, like `avro.codec` and `avro.schema`.
	pub metadata: Schema,//BTreeMap<String, Schema>,
	/// A unique 16 byte sequence for file integrity when writing avro data to file.
	pub sync_marker: SyncMarker
}

impl Header {
	/// Create a new header from the given schema and the sync marker.
	/// This method prepares a string representation of the schema and
	/// stores it in metadata map.
	pub fn new(schema: &AvroSchema, sync_marker: SyncMarker) -> Self {
		// metadata will be a map Schema
		let mut file_meta = BTreeMap::new(); 
		file_meta.insert("avro.codec".to_owned(), Schema::Bytes("null".as_bytes().to_vec()));
		let json_repr = format!("{}", schema.0);
		file_meta.insert("avro.schema".to_owned(), Schema::Bytes(json_repr.as_bytes().to_vec()));
		Header {
			magic: MAGIC_BYTES,
			metadata: Schema::Map(file_meta),
			sync_marker: sync_marker
		}
	}

	pub fn get_meta(&self, meta_key: &str) -> Result<Codecs, UnexpectedSchema> {
		if let Schema::Map(ref map) = self.metadata {
			let codec = map.get(meta_key);
			if let &Schema::Bytes(ref codec) = codec.unwrap() {
				match str::from_utf8(codec).unwrap() {
					"null" => Ok(Codecs::Null),
					"deflate" => Ok(Codecs::Deflate),
					"snappy" => Ok(Codecs::Snappy),
					_ => Err(UnexpectedSchema)
				}
			} else {
				debug!("Schema should be a string for inner metadata values");
				Err(UnexpectedSchema)
			}
		} else {
			debug!("Schema should be a Map for metadata");
			Err(UnexpectedSchema)
		}
	}
}

impl Codec for Header {
	fn encode<W>(&self, writer: &mut W) -> Result<usize, EncodeErr>
	where W: Write, Self: Sized {
		let mut total_len = self.magic.len();
		writer.write_all(&self.magic).unwrap();
		total_len += self.metadata.encode(writer)?;
		total_len += SYNC_MARKER_SIZE;
		total_len += self.sync_marker.encode(writer)?;
		Ok(total_len)
	}

	fn decode<R>(reader: &mut R, schema_type: DecodeValue) -> Result<Self, DecodeErr>
	where R: Read, Self:Sized {
		unimplemented!();
	}
}

/// A 16 byte sequence for keeping integrity checks when writing data blocks.
/// Each data block is delimited with the sync_marker contained in the datafile header.
#[derive(Debug, Clone)]
pub struct SyncMarker(pub Vec<u8>);
impl Codec for SyncMarker {
	fn encode<W>(&self, writer: &mut W)-> Result<usize, EncodeErr>
	where W: Write {
		writer.write_all(&self.0);
		Ok(SYNC_MARKER_SIZE)
	}

	fn decode<R>(reader: &mut R, schema_type: DecodeValue) -> Result<Self, DecodeErr>
	where R: Read, Self:Sized {
		if let DecodeValue::SyncMarker = schema_type {
			let mut buf = [0u8;16];
			reader.read_exact(&mut buf)
				  .map_err(|_| DecodeErr)?;
			Ok(SyncMarker(buf.to_vec()))
		} else {
			Err(DecodeErr)
		}
	}
}

impl From<EncodeErr> for () {
	fn from(t: EncodeErr) -> () {
		()
	}
}

/// Used to parse an in memory repr of an `.avro` file
/// It can be used to retrieve data back.
struct DataReader {
	header: Option<Header>,
	schema: Option<AvroSchema>
}

// TODO
impl DataReader {
	fn parse_encoded<R: Read>(reader: &mut R) -> impl Codec {
		Schema::Null
	}
}
