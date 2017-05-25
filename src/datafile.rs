//! This module declares a `DataWriter` instance which does actual writing of data
//! through give avro schema

use std::io::{Write, Read};
use std::collections::BTreeMap;

use types::{FromAvro, Schema};
use conversion::{Decoder, Encoder};
use rand::thread_rng;
use rand::Rng;
use complex::RecordSchema;

use schema::AvroSchema;
use std::str;

use crc::crc32;
use byteorder::{BigEndian, WriteBytesExt};

use snap::Encoder as SnapEncoder;
use snap::Decoder as SnapDecoder;
use snap::max_compress_len;

use errors::AvroErr;
use std::io::Cursor;
use serde_json;
use std::path::Path;
use std::mem;
use std::io::Seek;
use std::io::SeekFrom;
use std::fs::OpenOptions;
use serde_json::Value;

const SYNC_MARKER_SIZE: usize = 16;
const MAGIC_BYTES: [u8;4] = [b'O', b'b', b'j', 1 as u8];
const CRC_CHECKSUM_LEN: usize = 4;

/// Compression codec to use before writing to data file.
#[derive(Debug, Clone)]
pub enum Codecs {
	/// No compression
	Null,
	/// Use deflate compression
	Deflate,
	/// Use snappy compression
	Snappy
}

/// `DataWriter` provides api, to write data in an avro data file.
pub struct DataWriter {
	/// The header is used to perform integrity checks on an avro data file and also contains schema information
	pub header: Header,
	/// The avro schema that will be written to this datafile
	pub schema: AvroSchema,
	/// No of blocks that has been written
	pub block_cnt: u64,
	/// The sync marker read from the header of an avro data file
	pub sync_marker: SyncMarker,
	/// Buffer used to hold in flight data before writing them to `master_buffer`
	pub block_buffer: Vec<u8>,
	/// In memory buffer for the avro data file, which can be flushed to disk
	pub master_buffer: Cursor<Vec<u8>>
}

fn get_crc_uncompressed(pre_comp_buf: &[u8]) -> Vec<u8> {
	let crc_checksum = crc32::checksum_ieee(pre_comp_buf);
	let mut checksum_bytes = vec![];
	checksum_bytes.write_u32::<BigEndian>(crc_checksum).unwrap();
	checksum_bytes
}

fn compress_snappy(uncompressed_buffer: &[u8]) -> Vec<u8> {
	let mut snapper = SnapEncoder::new();
	let max_comp_len = max_compress_len(uncompressed_buffer.len() as usize);
	let mut compressed_data = vec![0u8; max_comp_len];
	let compress_count = snapper.compress(&*uncompressed_buffer, &mut compressed_data);
	compressed_data.truncate(compress_count.unwrap());
	compressed_data
}

/// decompress a given buffer using snappy codec
pub fn decompress_snappy(compressed_buffer: &[u8]) -> Vec<u8> {
	let mut snapper = SnapDecoder::new();
	let mut v = vec![0u8;1];
	snapper.decompress_vec(compressed_buffer).unwrap()
}

/// Preference should be to write in-memory
impl DataWriter {
	/// Create a DataWriter from a schema in a file
	pub fn from_file<P: AsRef<Path>>(schema: P) -> Result<Self , AvroErr> {
		let schema = AvroSchema::from_file(schema).unwrap();
		let data_writer = DataWriter::new(schema, Codecs::Null);
		data_writer
	}
	/// Create a DataWriter from a schema provided as string
	pub fn from_str(schema: &str) -> Result<Self , AvroErr> {
		let schema = AvroSchema::from_str(schema).unwrap();
		let data_writer = DataWriter::new(schema, Codecs::Null);
		data_writer
	}

	/// add compression codec for writing data
	pub fn set_codec(&mut self, codec: Codecs) {
		self.header.set_codec(codec)
	}

	/// Creates a new `DataWriter` instance which can be
	/// used to write data to the provided `Write` instance
	pub fn new(schema: AvroSchema, codec: Codecs) -> Result<Self, AvroErr> {
		// if the file already has the magic bytes and,
		// other stuff, then we need to keep the
		let mut master_buffer = Cursor::new(vec![]);
		let sync_marker = SyncMarker(gen_sync_marker());
		let mut header = Header::from_schema(&schema, sync_marker.clone());
		header.set_codec(codec);
		header.encode(&mut master_buffer).map_err(|_| AvroErr::EncodeErr)?;
		let schema_obj = DataWriter {
			header: header,
			schema: schema,
			sync_marker: sync_marker,
			block_cnt: 0,
			block_buffer: vec![],
			master_buffer: master_buffer
		};
		// TODO add sanity checks that we're dealing with a valid avro file.
		// TODO Seek to end if header is already written
		Ok(schema_obj)
	}

	/// checks if an avro data file is valid
	pub fn is_avro_datafile<R: Read + Seek>(buf: &mut R) -> bool {
		let mut magic_bytes = vec![0; 4];
		buf.read_exact(&mut magic_bytes);
		// rewind back to start
		buf.seek(SeekFrom::Start(0));
		MAGIC_BYTES == magic_bytes.as_slice()
	}

	/// Gives the internal master_buffer, so that it can be written to a file
	/// and replaces with a new one. Basically it resets the dat
	pub fn swap_buffer(&mut self) -> Cursor<Vec<u8>> {
		mem::replace(&mut self.master_buffer, Cursor::new(vec![]))
	}

	/// write the data buffer to a file for persistance
	pub fn flush_to_disk<P: AsRef<Path>>(&mut self, file_path: P) {
		let master_buffer = self.swap_buffer();
		let mut f = OpenOptions::new().read(true).write(true).create(true).open(file_path).unwrap();
		f.write_all(master_buffer.into_inner().as_slice());
	}

	fn get_past_header(&mut self) {
		// Allow skipping the header if provided with an already existing avro data file
	}

	/// Commits the written blocks of data to the master buffer
	/// which can then be also written to file
	pub fn commit_block(&mut self) -> Result<(), AvroErr> {
		Schema::Long(self.block_cnt as i64).encode(&mut self.master_buffer)?;
		match self.header.get_codec() {
			Ok(Codecs::Null) => {
				Schema::Long(self.block_buffer.len() as i64).encode(&mut self.master_buffer).unwrap();
				self.master_buffer.write_all(&self.block_buffer).map_err(|_| AvroErr::AvroWriteErr)?;
			}
			Ok(Codecs::Snappy) => {
				let checksum_bytes = get_crc_uncompressed(&self.block_buffer);
				let compressed_data = compress_snappy(&self.block_buffer);
				Schema::Long((compressed_data.len() + CRC_CHECKSUM_LEN) as i64).encode(&mut self.master_buffer)?;
				self.master_buffer.write_all(&*compressed_data).map_err(|_| AvroErr::AvroWriteErr)?;
				self.master_buffer.write_all(&*checksum_bytes).map_err(|_| AvroErr::AvroWriteErr)?;
			}
			Ok(Codecs::Deflate) | _ => unimplemented!()
		}
		self.sync_marker.encode(&mut self.master_buffer).map_err(|_| AvroErr::AvroWriteErr)?;
		self.block_cnt = 0;
		self.block_buffer.clear();
		Ok(())
	}

	/// Writes the provided scheme to its internal buffer. When an instance of DataWriter
	/// goes out of scope the buffer is fully flushed to the provided avro data file.
	pub fn write<T: Into<Schema>>(&mut self,
								  schema: T) -> Result<(), AvroErr> {
		let schema = schema.into();
		self.block_cnt += 1;
		schema.encode(&mut self.block_buffer)?;
		Ok(())
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
#[derive(Debug)]
pub struct Header {
	/// The magic byte sequence which serves as the identifier for a valid Avro data file.
	/// It consists of 3 ASCII bytes `O`,`b`,`j` followed by a 1 encoded as a byte.
	pub magic: [u8; 4],
	/// A Map avro schema which stores important metadata, like `avro.codec` and `avro.schema`.
	pub metadata: Schema,
	/// A unique 16 byte sequence for file integrity when writing avro data to file.
	pub sync_marker: SyncMarker,
}

/// recursive helper for parsing nested schemas
pub fn get_schema_util(s: &Value) -> FromAvro {
	if s.is_object() {
		let schema_type = s.get("type").unwrap().as_str().unwrap();
		return match schema_type {
			"null" => FromAvro::Null,
			"string" => FromAvro::Str,
			"boolean" => FromAvro::Bool,
			"double" => FromAvro::Double,
			"int" => FromAvro::Int,
			"float" => FromAvro::Float,
			"record" => {
				let rec = RecordSchema::from_json(s).unwrap();
				FromAvro::Record(rec)
			}
			"map" => {
				let map_val_schema = s.get("values").unwrap();
				FromAvro::Map(Box::new(get_schema_util(map_val_schema)))
			}
			_ => unimplemented!()
		}
	} else if s.is_array() {
		unimplemented!();
	} else if s.is_string() {
		return match s.as_str().unwrap() {
			"long" => FromAvro::Long,
			"int" => FromAvro::Int,
			"string" => FromAvro::Str,
			"float" => FromAvro::Float,
			"boolean" => FromAvro::Bool,
			"null" => FromAvro::Null,
			"double" => FromAvro::Double,
			_ => unimplemented!()
		}
	} else {
		unimplemented!();
	}
}

impl Header {
	/// Create a new header from the given schema and the sync marker.
	/// This method prepares a string representation of the schema and
	/// stores it in metadata map.
	pub fn from_schema(schema: &AvroSchema, sync_marker: SyncMarker) -> Self {
		let mut avro_meta = BTreeMap::new();
		let json_repr = format!("{}", schema.0);
		avro_meta.insert("avro.schema".to_owned(), Schema::Bytes(json_repr.as_bytes().to_vec()));
		Header {
			magic: MAGIC_BYTES,
			metadata: Schema::Map(avro_meta),
			sync_marker: sync_marker
		}
	}

	/// Creates a new header with default values
	pub fn new() -> Self {
		Header {
			magic: MAGIC_BYTES,
			metadata: Schema::Null,
			sync_marker: SyncMarker(vec![])
		}
	}

	/// Sets the codec to be applied when writing data
	pub fn set_codec(&mut self, codec: Codecs) {
		let codec = match codec {
			Codecs::Null => "null",
			Codecs::Deflate => "deflate",
			Codecs::Snappy => "snappy"
		};
		if let Schema::Map(ref mut bmap) = self.metadata {
			let _ = bmap.entry("avro.codec".to_owned()).or_insert_with(|| Schema::Bytes(codec.as_bytes().to_vec()));
		} else {
			debug!("Metadata type should be a Schema::Map");
		}
	}

	/// Retrieves the schema out of the parsed Header
	/// TODO parse as value, so that other types may also be decoded
	pub fn get_schema(&self) -> Result<FromAvro, ()> {
		let bmap = self.metadata.map_ref();
		let avro_schema = bmap.get("avro.schema").unwrap();
		let schema_bytes = avro_schema.bytes_ref();
		let schema_str = str::from_utf8(schema_bytes).unwrap();
		let s = serde_json::from_str::<Value>(schema_str).unwrap();
		if s.is_object() {
			let schema_type = s.get("type").unwrap().as_str().unwrap();
			match schema_type {
				"string" => return Ok(FromAvro::Str),
				"map" => {
					let map_val_schema = s.get("values").unwrap();
					return Ok(FromAvro::Map(Box::new(get_schema_util(map_val_schema))));
				}
				"record" => {
					let rec = RecordSchema::from_json(&s).unwrap();
					return Ok(FromAvro::Record(rec));
				}
				_ => unimplemented!()
			}
		} else if s.is_array() {
			unimplemented!();
		} else if s.is_string() {
			return match s.as_str().unwrap() {
				"long" => Ok(FromAvro::Long),
				"int" => Ok(FromAvro::Int),
				"string" => Ok(FromAvro::Str),
				"float" => Ok(FromAvro::Float),
				"boolean" => Ok(FromAvro::Bool),
				_ => unimplemented!()
			}
		}

		Err(())
	}

	/// Retrieves the codec out of the parsed Header
	pub fn get_codec(&self) -> Result<Codecs, AvroErr> {
		if let Schema::Map(ref map) = self.metadata {
			let codec = map.get("avro.codec");
			if let Schema::Bytes(ref codec) = *codec.unwrap() {
				match str::from_utf8(codec).unwrap() {
					"null" => Ok(Codecs::Null),
					"deflate" => Ok(Codecs::Deflate),
					"snappy" => Ok(Codecs::Snappy),
					_ => Err(AvroErr::UnexpectedSchema)
				}
			} else {
				debug!("Schema should be a string for inner metadata values");
				Err(AvroErr::UnexpectedSchema)
			}
		} else {
			debug!("Schema should be a Map for metadata");
			Err(AvroErr::UnexpectedSchema)
		}
	}
}

impl Encoder for Header {
	fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr> {
		let mut total_len = self.magic.len();
		writer.write_all(&self.magic).unwrap();
		total_len += self.metadata.encode(writer)?;
		total_len += SYNC_MARKER_SIZE;
		total_len += self.sync_marker.encode(writer)?;
		Ok(total_len)
	}
}

impl Decoder for Header {
	type Out=Self;
	fn decode<R: Read>(self, reader: &mut R) -> Result<Self::Out, AvroErr> {
		let mut magic_buf = [0u8;4];
		reader.read_exact(&mut magic_buf[..]).unwrap();
		let decoded_magic = str::from_utf8(&magic_buf[..]).unwrap();
		assert_eq!("Obj\u{1}", decoded_magic);
		let map_block_count = FromAvro::Long.decode(reader)?;
		let count = i64::from(map_block_count);
		let mut map = BTreeMap::new();
		for _ in 0..count as usize {
			let key = FromAvro::Str.decode(reader)?;
			let a = String::from(key);
			let val = FromAvro::Bytes.decode(reader)?;
			map.insert(a, val);
		}
		let _zero_map_marker = FromAvro::Long.decode(reader)?;
		let sync_marker = SyncMarker(vec![0u8;16]);
		let sync_marker = sync_marker.decode(reader)?;
		let magic_arr = [magic_buf[0], magic_buf[1], magic_buf[2], magic_buf[3]];
		let header = Header {
			magic: magic_arr,
			metadata: Schema::Map(map),
			sync_marker: sync_marker
		};
		Ok(header)
	}
}

/// A 16 byte sequence for keeping integrity checks when writing data blocks.
/// Each data block is delimited with the `sync_marker` contained in the datafile header.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncMarker(pub Vec<u8>);

impl SyncMarker {
	/// Creates a new zeroed buffer which can be used to fill with random ascii bytes
	pub fn new() -> Self {
		SyncMarker(vec![0u8;16])
	}
}

impl Encoder for SyncMarker {
	fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr> {
		writer.write_all(&self.0).map_err(|_| AvroErr::AvroWriteErr)?;
		Ok(SYNC_MARKER_SIZE)
	}
}

impl Decoder for SyncMarker {
	type Out=Self;
	fn decode<R: Read>(mut self, reader: &mut R) -> Result<Self, AvroErr> {
		reader.read_exact(&mut self.0).map_err(|_| AvroErr::DecodeErr)?;
		Ok(self)
	}
}

impl Into<Schema> for i32 {
	fn into(self) -> Schema {
		Schema::Long(self as i64)
	}
}

impl Into<Schema> for () {
	fn into(self) -> Schema {
		Schema::Null
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

impl Into<Schema> for Vec<u8> {
	fn into(self) -> Schema {
		Schema::Bytes(self)
	}
}

impl Into<Schema> for Vec<Schema> {
	fn into(self) -> Schema {
		Schema::Array(self)
	}
}

impl Into<Schema> for RecordSchema {
	fn into(self) -> Schema {
		Schema::Record(self)
	}
}

impl Into<Schema> for bool {
	fn into(self) -> Schema {
		Schema::Bool(self)
	}
}
