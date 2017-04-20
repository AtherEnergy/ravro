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
use snap::max_compress_len;

use errors::AvroErr;
use std::io::Cursor;
use serde_json;

const SYNC_MARKER_SIZE: usize = 16;
const MAGIC_BYTES: [u8;4] = [b'O', b'b', b'j', 1 as u8];
const CRC_CHECKSUM_LEN: usize = 4;

/// Compression codec to use before writing to data file.
#[derive(Debug, Clone)]
pub enum Codecs {
	Null,
	Deflate,
	Snappy
}

/// `DataWriter` reads an avro data file
pub struct DataWriter {
	/// The header parsed from the schema
	pub header: Header,
	/// The in memory representation of AvroSchema
	pub schema: AvroSchema,
	/// No of blocks that has been written
	pub block_cnt: u64,
	/// The sync marker read from the header of an avro data file
	pub sync_marker: SyncMarker,
	/// Buffer used to hold in flight data before writing them to an
	/// avro data file
	pub inmemory_buf: Vec<u8>
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

impl DataWriter {
	/// Creates a new `DataWriter` instance which can be
	/// used to write data to the provided `Write` instance
	pub fn new(schema: AvroSchema,
				  mut writer: &mut Cursor<Vec<u8>>,
				  codec: Codecs) -> Result<Self, AvroErr> {
		let sync_marker = SyncMarker(gen_sync_marker());
		let mut header = Header::from_schema(&schema, sync_marker.clone());
		header.set_codec(codec);
		header.encode(&mut writer).map_err(|_| AvroErr::EncodeErr)?;
		let schema_obj = DataWriter {
			header: header,
			schema: schema,
			sync_marker: sync_marker,
			block_cnt: 0,
			inmemory_buf: vec![]
		};
		// TODO add sanity checks that we're dealing with a valid avro file.
		// TODO Seek to end if header is already written
		Ok(schema_obj)
	}

	pub fn skip_header(&mut self) {
		// TODO if header is written should seek to the end for writing
	}

	pub fn commit_block(&mut self, mut writer: &mut Cursor<Vec<u8>>) -> Result<(), AvroErr> {
		match self.header.get_codec() {
			Ok(Codecs::Null) => {
				Schema::Long(self.block_cnt as i64).encode(&mut writer).unwrap();
				Schema::Long(self.inmemory_buf.len() as i64).encode(&mut writer).unwrap();
				writer.write_all(&self.inmemory_buf).map_err(|_| AvroErr::AvroWriteErr)?;
				self.sync_marker.encode(&mut writer).map_err(|_| AvroErr::AvroWriteErr)?;
			}
			Ok(Codecs::Snappy) => {
				let checksum_bytes = get_crc_uncompressed(&self.inmemory_buf);
				let compressed_data = compress_snappy(&self.inmemory_buf);
				Schema::Long(self.block_cnt as i64).encode(&mut writer)?;
				Schema::Long((compressed_data.len() + CRC_CHECKSUM_LEN) as i64).encode(&mut writer)?;
				writer.write_all(&*compressed_data).map_err(|_| AvroErr::AvroWriteErr)?;
				writer.write_all(&*checksum_bytes).map_err(|_| AvroErr::AvroWriteErr)?;
				self.sync_marker.encode(&mut writer).map_err(|_| AvroErr::AvroWriteErr)?;
			}
			Ok(Codecs::Deflate) | _ => unimplemented!()
		}
		Ok(())
	}

	/// Writes the provided scheme to its internal buffer. When an instance of DataWriter
	/// goes out of scope the buffer is fully flushed to the provided avro data file.
	pub fn write<T: Into<Schema>>(&mut self,
								  schema: T) -> Result<(), AvroErr> {
		let schema = schema.into();
		self.block_cnt += 1;
		schema.encode(&mut self.inmemory_buf)?;
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
	pub sync_marker: SyncMarker
}

impl Header {
	/// Create a new header from the given schema and the sync marker.
	/// This method prepares a string representation of the schema and
	/// stores it in metadata map.
	pub fn from_schema(schema: &AvroSchema, sync_marker: SyncMarker) -> Self {
		let mut file_meta = BTreeMap::new();
		let json_repr = format!("{}", schema.0);
		file_meta.insert("avro.schema".to_owned(), Schema::Bytes(json_repr.as_bytes().to_vec()));
		Header {
			magic: MAGIC_BYTES,
			metadata: Schema::Map(file_meta),
			sync_marker: sync_marker
		}
	}

	pub fn new() -> Self {
		Header {
			magic: [0,0,0,0],
			metadata: Schema::Null,
			sync_marker: SyncMarker(vec![])
		}
	}

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

	pub fn get_schema(&self) -> Result<FromAvro, ()> {
		let bmap = self.metadata.map_ref();
		let avro_schema = bmap.get("avro.schema").unwrap();
		let schema_bytes = avro_schema.bytes_ref();
		let schema_str = str::from_utf8(schema_bytes).unwrap();
		let s = serde_json::from_str::<String>(schema_str).unwrap();
		return match s.as_str() {
			"long" => Ok(FromAvro::Long),
			"int" => Ok(FromAvro::Int),
			"string" => Ok(FromAvro::Str),
			"float" => Ok(FromAvro::Float),
			_ => unimplemented!()
		}
	}

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
#[derive(Debug, Clone)]
pub struct SyncMarker(pub Vec<u8>);

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