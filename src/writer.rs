//! This module declares a `DataWriter` instance which does actual writing of data
//! through give avro schema

use std::io::{Write, Read};
use std::collections::BTreeMap;

use types::{FromAvro, Schema};
use codec::{Decoder, Encoder};
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
use std::mem;
use serde_json;
use std::path::Path;
use std::io::Seek;
use std::io::SeekFrom;
use std::fs::OpenOptions;
use serde_json::Value;

const SYNC_MARKER_SIZE: usize = 16;
const MAGIC_BYTES: [u8;4] = [b'O', b'b', b'j', 1 as u8];
const CRC_CHECKSUM_LEN: usize = 4;

/// Compression codec to use before writing to data file.
#[derive(Debug, Clone)]
pub enum Codec {
	/// No compression
	Null,
	/// Use deflate compression
	Deflate,
	/// Use snappy compression
	Snappy
}

/// Schema tag acts as a sentinel which checks for the schema that is being written to the data file
/// during write calls
#[derive(Debug)]
pub enum SchemaTag {
	/// Null schema
	Null,
	/// Boolean schema
	Boolean,
	/// Int schema
	Int,
	/// Long schema
	Long,
	/// Float schema
	Float,
	/// Double schema
	Double,
	/// Bytes schema
	Bytes,
	/// String schema
	String,
	/// Record schema
	Record,
	/// Enum schema
	Enum,
	/// Array schema
	Array,
	/// Map schema
	Map,
	/// Union schema
	Union,
	/// Fixed schema
	Fixed
}

/// `AvroWriter` provides api, to write data in an avro data file.
pub struct AvroWriter {
	/// The header is used to perform integrity checks on an avro data file and also contains schema information
	pub header: Header,
	/// No of blocks that has been written
	pub block_cnt: u64,
	/// The sync marker read from the header of an avro data file
	pub sync_marker: SyncMarker,
	/// Buffer used to hold in flight data before writing them to `master_buffer`
	pub block_buffer: Vec<u8>,
	/// In memory buffer for the avro data file, which can be flushed to disk
	pub master_buffer: Cursor<Vec<u8>>,
	/// This schema tag acts as a sentinel which shecks for the schema that is being written to the data file
	pub tag: SchemaTag

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
	SnapDecoder::new().decompress_vec(compressed_buffer).unwrap()
}

impl AvroWriter {
	/// Create a AvroWriter from a schema in a file
	pub fn from_file<P: AsRef<Path>>(schema: P) -> Result<Self , AvroErr> {
		let schema = AvroSchema::from_file(schema).unwrap();
		let data_writer = Self::new(schema, Codec::Null);
		data_writer
	}
	/// Create a DataWriter from a schema provided as string
	pub fn from_str(schema: &str) -> Result<Self , AvroErr> {
		let schema = AvroSchema::from_str(schema).unwrap();
		let data_writer = Self::new(schema, Codec::Null);
		data_writer
	}

	/// add compression codec for writing data
	pub fn set_codec(&mut self, codec: Codec) {
		self.header.set_codec(codec)
	}

	/// Creates a new `DataWriter` instance which can be
	/// used to write data to the provided `Write` instance
	pub fn new(schema: AvroSchema, codec: Codec) -> Result<Self, AvroErr> {
		let mut master_buffer = Cursor::new(vec![]);
		let sync_marker = SyncMarker(gen_sync_marker());
		let mut header = Header::from_schema(&schema, sync_marker.clone());
		header.set_codec(codec);
		header.encode(&mut master_buffer).map_err(|_| AvroErr::EncodeErr)?;
		let tag = schema.into();
		let writer = AvroWriter {
			header: header,
			sync_marker: sync_marker,
			block_cnt: 0,
			block_buffer: vec![],
			master_buffer: master_buffer,
			tag: tag
		};
		// TODO add sanity checks that we're dealing with a valid avro file.
		// TODO Seek to end if header is already written
		Ok(writer)
	}

	/// checks if an avro data file is valid
	pub fn is_avro_datafile<R: Read + Seek>(buf: &mut R) -> bool {
		let mut magic_bytes = vec![0; 4];
		let _ = buf.read_exact(&mut magic_bytes);
		// rewind back to start
		let _ = buf.seek(SeekFrom::Start(0));
		MAGIC_BYTES == magic_bytes.as_slice()
	}

	/// Writes the data buffer to a file for persistance
	pub fn flush_to_disk<P: AsRef<Path>>(&mut self, file_path: P) {
		// reach to the end skip to the bloc
		let master_buffer = mem::replace(&mut self.master_buffer, Cursor::new(vec![]));
		let mut f = OpenOptions::new().read(true).write(true).create(true).open(file_path).unwrap();
		let _ = f.write_all(master_buffer.into_inner().as_slice());
	}

	// TODO implement get past header

	/// Commits the written blocks of data to the master buffer
	/// compression_happens at block level.
	pub fn commit_block(&mut self) -> Result<(), AvroErr> {
		Schema::Long(self.block_cnt as i64).encode(&mut self.master_buffer)?;
		match self.header.get_codec() {
			Ok(Codec::Null) => {
				Schema::Long(self.block_buffer.len() as i64).encode(&mut self.master_buffer).unwrap();
				self.master_buffer.write_all(&self.block_buffer).map_err(|_| AvroErr::AvroWriteErr)?;
			}
			Ok(Codec::Snappy) => {
				let checksum_bytes = get_crc_uncompressed(&self.block_buffer);
				let compressed_data = compress_snappy(&self.block_buffer);
				Schema::Long((compressed_data.len() + CRC_CHECKSUM_LEN) as i64).encode(&mut self.master_buffer)?;
				self.master_buffer.write_all(&*compressed_data).map_err(|_| AvroErr::AvroWriteErr)?;
				self.master_buffer.write_all(&*checksum_bytes).map_err(|_| AvroErr::AvroWriteErr)?;
			}
			Ok(Codec::Deflate) | _ => unimplemented!()
		}
		self.sync_marker.encode(&mut self.master_buffer).map_err(|_| AvroErr::AvroWriteErr)?;
		self.block_cnt = 0;
		self.block_buffer.clear();
		Ok(())
	}

	/// Returns an in memory representation of written avro data
	pub fn swap_buffer(&mut self) -> Cursor<Vec<u8>> {
		mem::replace(&mut self.master_buffer, Cursor::new(vec![]))
	}

	/// Writes the provided data to a block buffer. This write constitutes the content
	/// of the current block. Clients can configure the number of items in the block.
	/// Its only on calling commit_block that the block buffer gets written to master buffer
	/// along with any compression(if specified).
	pub fn write<T: Into<Schema>>(&mut self, schema: T) -> Result<(), AvroErr> {
		let schema = schema.into();
		match (&schema, &self.tag) {
			(&Schema::Null, &SchemaTag::Null) |
			(&Schema::Bool(_), &SchemaTag::Boolean) |
			(&Schema::Int(_), &SchemaTag::Int) |
			(&Schema::Long(_), &SchemaTag::Long) |
			// Int and Long are encoded in same way
			(&Schema::Long(_), &SchemaTag::Int) |
			(&Schema::Int(_), &SchemaTag::Long) |
			(&Schema::Float(_), &SchemaTag::Float) |
			(&Schema::Double(_), &SchemaTag::Double) |
			(&Schema::Bytes(_), &SchemaTag::Bytes) |
			(&Schema::Str(_), &SchemaTag::String) |
			(&Schema::Record(_), &SchemaTag::Record) |
			(&Schema::Enum(_), &SchemaTag::Enum) |
			(&Schema::Array(_), &SchemaTag::Array) |
			(&Schema::Map(_), &SchemaTag::Map) |
			(&Schema::Fixed, &SchemaTag::Fixed) => {}
			_ => return Err(AvroErr::UnexpectedSchema)
			// TODO implement union
		}
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
	/// The magic byte sequence serves as the identifier for a valid Avro data file.
	/// It consists of 3 ASCII bytes `O`,`b`,`j` followed by a 1 encoded as a byte.
	pub magic: [u8; 4],
	/// A Map avro schema which stores important metadata, like `avro.codec` and `avro.schema`.
	pub metadata: Schema,
	/// A unique 16 byte sequence for file integrity when writing avro data to file.
	pub sync_marker: SyncMarker,
}

fn parse_from_avro(s: &str, parent_json: &Value) -> FromAvro {
	match s {
		"null" => FromAvro::Null,
		"string" => FromAvro::Str,
		"boolean" => FromAvro::Bool,
		"double" => FromAvro::Double,
		"int" => FromAvro::Int,
		"float" => FromAvro::Float,
		"record" => {
			let rec = RecordSchema::from_json(parent_json).unwrap();
			FromAvro::Record(rec)
		}
		"map" => {
			let map_val_schema = parent_json.get("values").unwrap();
			FromAvro::Map(Box::new(get_schema_util(map_val_schema)))
		}
		_ => unimplemented!()
	}
}

impl Header {
	/// Creates a header using the schema parsed from an avsc file
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
	pub fn set_codec(&mut self, codec: Codec) {
		let codec = match codec {
			Codec::Null => "null",
			Codec::Deflate => "deflate",
			Codec::Snappy => "snappy"
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
		match s {
			Value::Object(ref map) => {
				if let Some(&Value::String(ref inner_str)) = s.get("type") {
					match inner_str.as_str() {
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
				} else {
					unimplemented!()
				}
			}
			Value::String(ref inner_str) => {
				return Ok(parse_from_avro(inner_str, &s));
			}
			Value::Array(_) | _ => unimplemented!() 
		}
	}

	/// Retrieves the codec out of the parsed Header
	pub fn get_codec(&self) -> Result<Codec, AvroErr> {
		if let Schema::Map(ref map) = self.metadata {
			let codec = map.get("avro.codec");
			if let Schema::Bytes(ref codec) = *codec.unwrap() {
				match str::from_utf8(codec).unwrap() {
					"null" => Ok(Codec::Null),
					"deflate" => Ok(Codec::Deflate),
					"snappy" => Ok(Codec::Snappy),
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

/// Recursive helper for parsing nested schemas
pub fn get_schema_util(s: &Value) -> FromAvro {
	return match *s {
		Value::Object(ref obj) => {
			if let Some(&Value::String(ref inner_str)) = obj.get("type") {
				parse_from_avro(&inner_str, s)
			} else {
				panic!("Expected type attribute to be as a Json string");
			}
		}
		Value::String(ref inner_str) => parse_from_avro(&inner_str, s),
		Value::Array(_) | _ => unimplemented!("Schema not yet implemented")
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

impl Into<Schema> for Vec<BTreeMap<String, String>> {
	fn into(self) -> Schema {
		let schema_vec: Vec<Schema> = self.into_iter().map(|e| e.into()).collect();
		schema_vec.into()
	}
}

impl Into<Schema> for BTreeMap<String, String> {
	fn into(self) -> Schema {
		let mut converted_map: BTreeMap<String, Schema> = BTreeMap::new();
		for (k,v) in self {
			converted_map.insert(k, Schema::Str(v));
		}
		Schema::Map(converted_map)
	}
}