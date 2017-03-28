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

use crc::{crc32, Hasher32};
use byteorder::{BigEndian, WriteBytesExt};

use snap::Encoder;
use snap::max_compress_len;
use std::io::Cursor;

const SYNC_MARKER_SIZE: usize = 16;
const MAGIC_BYTES: [u8;4] = [b'O', b'b', b'j', 1 as u8];
// Used when snappy codec is used
const CRC_CHECKSUM_LEN: usize = 4;

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
	pub header: Header,
	/// The in memory representation of AvroSchema
	pub schema: AvroSchema,
	/// No of blocks that has been written
	pub block_cnt: u64,
	/// A Write instance (default `File`) into which bytes will be written.
	pub writer: Box<Write>,
	/// The sync marker read from the data file header
	pub sync_marker: SyncMarker
}

fn get_crc_uncompressed(pre_comp_buf: &Vec<u8>) -> Vec<u8> {
	let crc_checksum = crc32::checksum_ieee(pre_comp_buf);
	let mut checksum_bytes = vec![];
	checksum_bytes.write_u32::<BigEndian>(crc_checksum).unwrap();
	checksum_bytes
}

fn compress_snappy(uncompressed_buffer: Vec<u8>) -> Vec<u8> {
	let mut snapper = Encoder::new();
	let max_comp_len = max_compress_len(uncompressed_buffer.len() as usize);
	let mut compressed_data = vec![0u8; max_comp_len];
	let compress_count = snapper.compress(&*uncompressed_buffer, &mut compressed_data);
	compressed_data.truncate(compress_count.unwrap());
	compressed_data
}

impl DataWriter {
	/// Creates a new `DataWriter` instance which can be used to write data to the provided `Write` instance
	pub fn new<W>(schema: AvroSchema,
				  mut writer: W,
				  codec: Codecs) -> Result<Self, String>
	where W: Write + Read + 'static {
		let sync_marker = SyncMarker(gen_sync_marker());
		let mut header = Header::new(&schema, sync_marker.clone());
		header.set_codec(codec);
		header.encode(&mut writer);
		let schema_obj = DataWriter {
			header: header,
			schema: schema,
			writer: Box::new(writer),
			sync_marker: sync_marker,
			block_cnt: 0
		};
		// TODO add sanity checks that we're dealing with a valid avro file.
		// TODO Seek to end if header is already written
		Ok(schema_obj)
	}

	pub fn skip_header(&mut self) {
		// TODO if header is written should seek to the end for writing
	}

	pub fn write_to_buffer<T: Into<Schema>>(&mut self, schema: T, mut buf: Vec<u8>) -> Result<Cursor<Vec<u8>>, AvroWriteErr> {
		let mut buf = Cursor::new(buf.clone());
		let schema = schema.into();
		let sync_marker = self.sync_marker.clone();
		let mut buffer = vec![];
		match self.header.get_meta("avro.codec") {
			Ok(Codecs::Null) => {
				commit_block!(schema, sync_marker, buffer);
				buf.write_all(&*buffer).map_err(|_| AvroWriteErr)?
			}
			Ok(Codecs::Snappy) => {
				let mut uncompressed_buffer =  vec![];
				schema.encode(&mut uncompressed_buffer);
				let checksum_bytes = get_crc_uncompressed(&uncompressed_buffer);
				let compressed_data = compress_snappy(uncompressed_buffer);
				// Write data count
				Schema::Long(1).encode(&mut buf);
				// Write the serialized byte size
				Schema::Long((compressed_data.len() + CRC_CHECKSUM_LEN) as i64).encode(&mut buf);
				// Write compressed data followed by checksum
				buf.write_all(&*compressed_data);
				buf.write_all(&*checksum_bytes);
				// Write our sync marker
				self.sync_marker.encode(&mut buf);
			}
			Ok(Codecs::Deflate)| _ => unimplemented!()
		}
		Ok(buf)
	}

	pub fn write<T: Into<Schema>>(&mut self, schema: T) -> Result<(), AvroWriteErr> {
		let schema = schema.into();
		let sync_marker = self.sync_marker.clone();
		let mut buffer = vec![];
		match self.header.get_meta("avro.codec") {
			Ok(Codecs::Null) => {
				commit_block!(schema, sync_marker, buffer);
				self.writer.write_all(&*buffer).map_err(|_| AvroWriteErr)?
			}
			Ok(Codecs::Snappy) => {
				let mut uncompressed_buffer =  vec![];
				schema.encode(&mut uncompressed_buffer);
				let checksum_bytes = get_crc_uncompressed(&uncompressed_buffer);
				let compressed_data = compress_snappy(uncompressed_buffer);
				// Write data count
				Schema::Long(1).encode(&mut self.writer);
				// Write the serialized byte size
				Schema::Long((compressed_data.len() + CRC_CHECKSUM_LEN) as i64).encode(&mut self.writer);
				// Write compressed data followed by checksum
				self.writer.write_all(&*compressed_data);
				self.writer.write_all(&*checksum_bytes);
				// Write our sync marker
				self.sync_marker.encode(&mut self.writer);
			}
			Ok(Codecs::Deflate) | _ => unimplemented!(),
		}
		Ok(())
	}
}

impl Into<Schema> for i32 {
	fn into(self) -> Schema {
		Schema::Long(self as i64)
	}
}

impl Into<Schema> for i64 {
	fn into(self) -> Schema {
		Schema::Long(self)
	}
}

impl Into<Schema> for f32 {
	fn into(self) -> Schema {
		Schema::Float(self)
	}
}

impl Into<Schema> for f64 {
	fn into(self) -> Schema {
		Schema::Double(self)
	}
}

impl Into<Schema> for BTreeMap<String, Schema> {
	fn into(self) -> Schema {
		Schema::Map(self)
	}
}

impl Into<Schema> for String {
	fn into(self) -> Schema {
		Schema::Str(self)
	}
}

impl From<EncodeErr> for AvroWriteErr {
	fn from(_: EncodeErr) -> AvroWriteErr {
		AvroWriteErr
	}
}

impl Into<Schema> for RecordSchema {
	fn into(self) -> Schema {
		Schema::Record(self)
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
		let mut file_meta = BTreeMap::new();
		let json_repr = format!("{}", schema.0);
		file_meta.insert("avro.schema".to_owned(), Schema::Bytes(json_repr.as_bytes().to_vec()));
		Header {
			magic: MAGIC_BYTES,
			metadata: Schema::Map(file_meta),
			sync_marker: sync_marker
		}
	}

	pub fn set_codec(&mut self, codec: Codecs) {
		let codec = match codec {
			Codecs::Null => "null",
			Codecs::Deflate => "deflate",
			Codecs::Snappy => "snappy"
		};
		if let Schema::Map(ref mut bmap) = self.metadata {
			let _ = bmap.entry("avro.codec".to_owned()).or_insert(Schema::Bytes(codec.as_bytes().to_vec()));
		} else {
			debug!("Metadata type should be a Schema::Map");
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

/// A DataReader instance is used to parse an Avro data file.
/// Contains various routines to parse each logical section of the data file, such as magic marker,
/// metadatas, and file data blocks.
// TODO It would be good to have idiomatic iterator interface for it, as avro supports streaming
// reads.
struct DataReader;
impl DataReader {
	/// Parses the header
	fn parse_header<R: Read>(&self, mut reader: &mut R) {
		let mut magic_buf = [0u8;4];
		reader.read_exact(&mut magic_buf[..]).unwrap();
		let decoded_magic = str::from_utf8(&magic_buf[..]).unwrap();
		assert_eq!("Obj\u{1}", decoded_magic);
		let map = Schema::decode(&mut reader, DecodeValue::Double).unwrap();
		let sync_marker = SyncMarker::decode(reader, DecodeValue::SyncMarker).unwrap();
		println!("SYNC MARKER {:?}", sync_marker);
		// println!("{:?}", map);
		// TODO
	}

	/// Parses a file data block
	fn parse_block<R: Read>(&self, reader: &mut R) {

	}

	/// Returns how many data we have read, a
	fn get_block_count<R: Read>(&self, reader: &mut R) {
		let mut v = [0u8;1];
		let _ = reader.read_exact(&mut v[..]);
	}

	/// User level api that in-turn calls other parse_* methods inside the DataReader instance.
	fn read<R: Read>(&self, reader: &mut R) {
		// Step 1 Read header
		let header = self.parse_header(reader);
		// should bail out if not a valid avro file
		// Step 2 from header get the schema obj
		// Step 3 match on the schema type:

		let block_count = self.get_block_count(reader);
		// Step 4 iterate till block_count and do below
		// call schema.decode passing in the DecodeValue
	}
}
