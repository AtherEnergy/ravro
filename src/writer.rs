//! This module declares a `DataWriter` instance which does actual writing of data
//! through give avro schema

use std::io::{Write, Read};
use std::collections::HashMap;

use types::Type;
use codec::{Decoder, Encoder};
use rand::thread_rng;
use rand::Rng;
use complex::Record;

use schema::AvroSchema;
use complex::Field;
use std::str;

use crc::crc32;
use byteorder::{BigEndian, WriteBytesExt};

use snap::Encoder as SnapEncoder;
use snap::Decoder as SnapDecoder;

use errors::AvroErr;
use std::io::Cursor;
use std::mem;
use std::path::Path;
use serde_json::Value;
use flate2::Compression;
use flate2::write::DeflateEncoder;
use std::fmt::Debug;
use std::error::Error;

use std::collections::BTreeMap;

const SYNC_MARKER_SIZE: usize = 16;
const MAGIC_BYTES: [u8;4] = [b'O', b'b', b'j', 1 as u8];
const CRC_CHECKSUM_LEN: usize = 4;

fn get_crc_uncompressed(pre_comp_buf: &[u8]) -> Vec<u8> {
	let crc_checksum = crc32::checksum_ieee(pre_comp_buf);
	let mut checksum_bytes = vec![];
	checksum_bytes.write_u32::<BigEndian>(crc_checksum).unwrap();
	checksum_bytes
}

fn compress_snappy(uncompressed_buffer: &[u8]) -> Vec<u8> {
	let mut snapper = SnapEncoder::new();
	snapper.compress_vec(uncompressed_buffer).expect("Snappy: Failed to compress data")
}

fn compress_deflate(uncompressed_buffer: &[u8]) -> Vec<u8> {
	let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
	e.write(uncompressed_buffer).unwrap();
	e.finish().expect("Deflate: Failed to compress data")
}

#[allow(dead_code)]
fn decompress_snappy(compressed_buffer: &[u8]) -> Vec<u8> {
	SnapDecoder::new().decompress_vec(compressed_buffer).unwrap()
}

/// Compression codec to use before writing to data file.
#[derive(Debug, Clone, Copy)]
pub enum Codec {
	/// No compression
	Null,
	/// Use deflate compression
	Deflate,
	/// Use snappy compression
	Snappy
}

/// Type tag acts as a sentinel which checks for the schema that is being written to the data file
/// during write calls
#[derive(Debug, PartialEq, Clone)]
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
	header: Header,
	/// No of blocks that has been written
	block_count: i64,
	/// Buffer used to hold in flight data before writing them to `master_buffer`
	block_buffer: Vec<u8>,
	/// In memory buffer for the avro data file which can be flushed to disk or returned
	/// to user by take_datafile method
	master_buffer: Cursor<Vec<u8>>,
	/// This schema tag acts as a sentinel which checks for the schema that is being written to the data file
	tag: SchemaTag,
	/// the codec to be used
	codec: Codec
}

/// Builder for AvroWriter, allows setting up schema and codecs
pub struct WriterBuilder {
	schema: AvroSchema,
	codec: Codec
}

impl WriterBuilder {
	/// Sets the codec to be used when writing avro data
	pub fn set_codec(&mut self, codec: Codec) {
		self.codec = codec;
	}

	/// creates an AvroWriter instance
	pub fn build(self) -> Result<AvroWriter, AvroErr> {
		AvroWriter::new(self.schema, self.codec)
	}

	/// Create a AvroWriter from a schema in a file
	pub fn from_schema<P: AsRef<Path> + Debug>(schema: P) -> Result<WriterBuilder, AvroErr> {
		let schema = AvroSchema::from_file(schema)?;
		let writer_builder = WriterBuilder {
			schema: schema,
			codec: Codec::Null
		};
		Ok(writer_builder)
	}

	/// Create a DataWriter from a schema provided as string
	pub fn from_str(schema: &str) -> Result<WriterBuilder, AvroErr> {
		let schema = AvroSchema::from_str(schema)?;
		let writer_builder = WriterBuilder {
			schema: schema,
			codec: Codec::Null
		};
		Ok(writer_builder)
	}
}

impl AvroWriter {

	/// Creates a new `DataWriter` instance which can be
	/// used to write data to the provided `Write` instance
	/// It writes the avro data header and gets the buffer ready for incoming data writes 
	fn new(schema: AvroSchema, codec: Codec) -> Result<Self, AvroErr> {
		let mut master_buffer = Cursor::new(vec![]);
		let sync_marker = SyncMarker(gen_sync_marker());
		let mut header = Header::from_schema(&schema, sync_marker.clone());
		header.append_codec(codec);
		header.encode(&mut master_buffer)?;
		let tag = schema.into();
		let writer = AvroWriter {
			header: header,
			block_count: 0,
			block_buffer: vec![],
			master_buffer: master_buffer,
			tag: tag,
			codec: codec
		};
		Ok(writer)
	}

	/// Retrieves a reference to the avro schema
	pub fn get_schema(&self) -> &AvroSchema {
		&self.header.schema
	}

	/// Gives the avro data file as a vector of bytes
	/// replacing it with a new one ready for next stream of data.
	/// This can then be used to either send over RPC or flush to disk
	pub fn take_datafile(&mut self) -> Result<Vec<u8>, AvroErr> {
		self.commit_block()?;
		let written_datafile = mem::replace(&mut self.master_buffer, Cursor::new(vec![]));
		// Is there a reason for failure the second time ?
		self.header.encode(&mut self.master_buffer).expect("Failed to re-encode header on new datafile");
		Ok(written_datafile.into_inner())
	}

	// TODO implement get past header
	/// Commits the written blocks of data to the master buffer. Compression_happens at block level.
	pub fn commit_block(&mut self) -> Result<(), AvroErr> {
		Type::Long(self.block_count as i64).encode(&mut self.master_buffer)?;
		match self.codec {
			Codec::Null => {
				Type::Long(self.block_buffer.len() as i64).encode(&mut self.master_buffer).unwrap();
				self.master_buffer.write_all(&self.block_buffer).map_err(|_| AvroErr::AvroWriteErr)?;
			}
			Codec::Snappy => {
				let checksum_bytes = get_crc_uncompressed(&self.block_buffer);
				let compressed_data = compress_snappy(&self.block_buffer);
				Type::Long((compressed_data.len() + CRC_CHECKSUM_LEN) as i64).encode(&mut self.master_buffer)?;
				self.master_buffer.write_all(&*compressed_data).map_err(|_| AvroErr::AvroWriteErr)?;
				self.master_buffer.write_all(&*checksum_bytes).map_err(|_| AvroErr::AvroWriteErr)?;
			}
			Codec::Deflate => {
				let checksum_bytes = get_crc_uncompressed(&self.block_buffer);
				let compressed_data = compress_deflate(&self.block_buffer);
				Type::Long((compressed_data.len() + CRC_CHECKSUM_LEN) as i64).encode(&mut self.master_buffer)?;
				self.master_buffer.write_all(&*compressed_data).map_err(|_| AvroErr::AvroWriteErr)?;
				self.master_buffer.write_all(&*checksum_bytes).map_err(|_| AvroErr::AvroWriteErr)?;
			}
		}
		self.header.sync_marker.encode(&mut self.master_buffer).map_err(|_| AvroErr::AvroWriteErr)?;
		self.block_count = 0;
		self.block_buffer.clear();
		Ok(())
	}

	/// Returns the in-memory buffer of written avro data
	pub fn swap_buffer(&mut self) -> Vec<u8> {
		mem::replace(&mut self.master_buffer, Cursor::new(vec![])).into_inner()
	}

	/// Writes the provided data to a block buffer. This write constitutes the content
	/// of the current block. Clients can configure the number of items in the block.
	/// Its only on calling commit_block that the block buffer gets written to master buffer
	/// along with any compression(if specified).
	pub fn write<T: Into<Type>>(&mut self, schema: T) -> Result<(), AvroErr> {
		let schema = schema.into();
		match (&schema, &self.tag) {
			(&Type::Null, &SchemaTag::Null) |
			(&Type::Bool(_), &SchemaTag::Boolean) |
			(&Type::Int(_), &SchemaTag::Int) |
			(&Type::Long(_), &SchemaTag::Long) |
			// Int and Long are encoded in same way
			(&Type::Long(_), &SchemaTag::Int) |
			(&Type::Int(_), &SchemaTag::Long) |
			(&Type::Float(_), &SchemaTag::Float) |
			(&Type::Double(_), &SchemaTag::Double) |
			(&Type::Bytes(_), &SchemaTag::Bytes) |
			(&Type::Str(_), &SchemaTag::String) |
			(&Type::Record(_), &SchemaTag::Record) |
			(&Type::Enum(_), &SchemaTag::Enum) |
			(&Type::Array(_), &SchemaTag::Array) |
			(&Type::Map(_), &SchemaTag::Map) |
			(&Type::Fixed, &SchemaTag::Fixed) => {}
			_ => return Err(AvroErr::UnexpectedSchema)
			// TODO implement union
		}
		self.block_count += 1;
		schema.encode(&mut self.block_buffer)?;
		// The approximate number of uncompressed bytes to write in each block
		// TODO should be user configurable ?
		// From java impl: https://github.com/apache/avro/blob/5c270dad2a281f4e70fb8c8a657d93a0cc72b7a8/lang/java/avro/src/main/java/org/apache/avro/file/DataFileWriter.java#L101
		// Valid values range from 32 to 2^30
   		// Suggested values are between 2K and 2M
		if self.block_count == 4096 {
			self.commit_block()?;
		}
		Ok(())
	}
}

fn gen_sync_marker() -> Vec<u8> {
    let mut vec = [0u8; SYNC_MARKER_SIZE];
    thread_rng().fill_bytes(&mut vec[..]);
    vec.to_vec()
}

fn get_schema_tag(s: &str, _parent_json: &Value) -> SchemaTag {
	match s {
		"null" => SchemaTag::Null,
		"string" => SchemaTag::String,
		"boolean" => SchemaTag::Boolean,
		"double" => SchemaTag::Double,
		"int" => SchemaTag::Int,
		"float" => SchemaTag::Float,
		"record" => {
			// TODO use this when we are implementing reader
			// let rec = Record::from_json(parent_json).unwrap();
			// FromAvro::Record(rec)
			SchemaTag::Record
		}
		"map" => {
			// TODO as above
			// let map_val_schema = parent_json.get("values").unwrap();
			// FromAvro::Map(Box::new(get_schema_util(map_val_schema)))
			SchemaTag::Map
		}
		_ => unimplemented!()
	}
}

/// Recursive helper for parsing nested schemas
pub fn get_schema_util(s: &Value) -> SchemaTag {
	return match *s {
		Value::Object(ref obj) => {
			if let Some(&Value::String(ref inner_str)) = obj.get("type") {
				get_schema_tag(&inner_str, s)
			} else {
				panic!("Expected type attribute to be as a Json string");
			}
		}
		Value::String(ref inner_str) => get_schema_tag(&inner_str, s),
		Value::Array(_) => SchemaTag::Array,
		ref other => unreachable!(format!("Invalid schema: {}", other))
	}
}

/// The avro datafile header
#[derive(Debug)]
pub struct Header {
	/// The magic byte sequence serves as the identifier for a valid Avro data file.
	/// It consists of 3 ASCII bytes `O`,`b`,`j` followed by a 1 encoded as a byte.
	pub magic: [u8; 4],
	/// A Map avro schema which stores important metadata, like `avro.codec` and `avro.schema`.
	pub metadata: Type,
	/// A unique 16 byte sequence for file integrity when writing avro data to file.
	pub sync_marker: SyncMarker,
	/// The schema
	pub schema: AvroSchema
}

impl Header {
	/// Creates a header using the schema parsed from an avsc file
	pub fn from_schema(schema: &AvroSchema, sync_marker: SyncMarker) -> Self {
		let mut avro_meta = HashMap::new();
		
		let json_repr = match schema {
			&AvroSchema::Primitive(ref v) | &AvroSchema::Complex(ref v) => v,
		};
		let json_repr = format!("{}", json_repr);
		avro_meta.insert("avro.schema".to_owned(), Type::Bytes(json_repr.as_bytes().to_vec()));
		Header {
			magic: MAGIC_BYTES,
			metadata: Type::Map(avro_meta),
			sync_marker: sync_marker,
			schema: schema.clone()
		}
	}

	fn append_codec(&mut self, codec: Codec) {
		let codec = match codec {
			Codec::Null => "null",
			Codec::Deflate => "deflate",
			Codec::Snappy => "snappy"
		};
		if let Type::Map(ref mut bmap) = self.metadata {
			bmap.insert("avro.codec".to_string(), Type::Bytes(codec.as_bytes().to_vec()));
		} else {
			debug!("Metadata type should be a Type::Map (HashMap<K, V>)");
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

// impl Decoder for Header {
// 	type Out=Self;
// 	fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
// 		let mut magic_buf = [0u8;4];
// 		reader.read_exact(&mut magic_buf[..]).unwrap();
// 		let decoded_magic = str::from_utf8(&magic_buf[..]).unwrap();
// 		if decoded_magic != "Obj\u{1}" {
// 			return Err(AvroErr::UnexpectedData)
// 		}
// 		let map_block_count = i64::decode(reader)?;
// 		let count = i64::from(map_block_count);
// 		let mut map = HashMap::new();
// 		for _ in 0..count as usize {
// 			let key = String::decode(reader)?;
// 			let a = String::from(key);
// 			let val = Vec::<u8>::decode(reader)?;
// 			map.insert(a, Type::Bytes(val));
// 		}
// 		let _zero_map_marker = i64::decode(reader)?;
// 		let sync_marker = SyncMarker::decode(reader)?;
// 		let magic_arr = [magic_buf[0], magic_buf[1], magic_buf[2], magic_buf[3]];
// 		let header = Header {
// 			magic: magic_arr,
// 			metadata: Type::Map(map),
// 			sync_marker: sync_marker,

// 		};
// 		Ok(header)
// 	}
// }

/// A 16 byte sequence for keeping integrity checks when writing data blocks.
/// Each data block is delimited with the `sync_marker` contained in the datafile header.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncMarker(Vec<u8>);

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
	fn decode<R: Read>(reader: &mut R) -> Result<Self, AvroErr> {
		let mut sync_marker = SyncMarker(vec![0u8;16]);
		reader.read_exact(&mut sync_marker.0).map_err(|e| AvroErr::DecodeErr(e.description().to_string()))?;
		Ok(sync_marker)
	}
}

impl Into<Type> for i32 {
	fn into(self) -> Type {
		Type::Long(self as i64)
	}
}

impl Into<Type> for () {
	fn into(self) -> Type {
		Type::Null
	}
}

impl Into<Type> for i64 {
	fn into(self) -> Type {
		Type::Long(self)
	}
}

impl Into<Type> for f32 {
	fn into(self) -> Type {
		Type::Float(self)
	}
}

impl Into<Type> for f64 {
	fn into(self) -> Type {
		Type::Double(self)
	}
}

impl Into<Type> for HashMap<String, Type> {
	fn into(self) -> Type {
		Type::Map(self)
	}
}

impl Into<Type> for String {
	fn into(self) -> Type {
		Type::Str(self)
	}
}

impl Into<Type> for Vec<u8> {
	fn into(self) -> Type {
		Type::Bytes(self)
	}
}

impl Into<Type> for Vec<Type> {
	fn into(self) -> Type {
		Type::Array(self)
	}
}

impl Into<Type> for Record {
	fn into(self) -> Type {
		Type::Record(self)
	}
}

impl Into<Type> for bool {
	fn into(self) -> Type {
		Type::Bool(self)
	}
}

impl Into<Type> for Vec<HashMap<String, String>> {
	fn into(self) -> Type {
		let schema_vec: Vec<Type> = self.into_iter().map(|e| e.into()).collect();
		schema_vec.into()
	}
}

impl Into<Type> for HashMap<String, String> {
	fn into(self) -> Type {
		let mut converted_map: HashMap<String, Type> = HashMap::new();
		for (k,v) in self.into_iter() {
			converted_map.insert(k, Type::Str(v));
		}
		Type::Map(converted_map)
	}
}

impl<'a> Into<Type> for HashMap<&'a str, &'a str> {
	fn into(self) -> Type {
		let mut converted_map: HashMap<String, Type> = HashMap::new();
		for (k,v) in self.into_iter() {
			converted_map.insert(k.to_string(), Type::Str(v.to_string()));
		}
		Type::Map(converted_map)
	}
}

impl<'a> Into<Type> for Vec<HashMap<&'a str, &'a str>> {
	fn into(self) -> Type {
		let schema_vec: Vec<Type> = self.into_iter().map(|e| e.into()).collect();
		schema_vec.into()
	}
}

/// Interface to convert a custom rust type to a recor
pub trait ToRecord {
    /// Performs the conversion
    fn to_avro(&self, type_name: &str, schema: &AvroSchema) -> Result<Record, AvroErr>;
}

fn failed_parsing(data_value: &str, field_name: &str, type_name: &str) -> String {
    format!("Failed to parse {:?} as long for field name {:?} on channel {:?}", data_value, field_name, type_name)
}

impl ToRecord for BTreeMap<String, String> {
    fn to_avro(&self, type_name: &str, schema: &AvroSchema) -> Result<Record, AvroErr> {
        let mut record = Record::builder();
        record.set_name(type_name);
        let field_pairs = schema.record_field_pairs().ok_or(AvroErr::InvalidSchema(schema.clone().into()))?;

        for field in field_pairs {
            // It's an error to not have fields which are mentioned in schema
            // otherwise extra fields are ignored
            let field_name = field.name();
            if !self.contains_key(field_name) {
                let err_reason = format!("Expected field `{}` not found; which is present in schema on channel {:?}",
                    field_name,
                    type_name);
                return Err(AvroErr::AvroConversionFailed(err_reason));
            }

            let data_value = &self[field.name()];
            // NOTE: Only primitve types as a `field` are supported as of now
            let avro_field_type = match field.type_str() {
                "string" => Field::new(&field_name, Type::Str(data_value.to_string())),
                "float" => Field::new(&field_name, Type::Float(
			        data_value.parse::<f32>().map_err(|_|{
						AvroErr::AvroConversionFailed(failed_parsing(data_value,
                                                                     field_name,
                                                                     type_name))
					})?
		        )),
        		"double" => Field::new(&field_name, Type::Double(
			        data_value.parse::<f64>().map_err(|_|{
						AvroErr::AvroConversionFailed(failed_parsing(data_value,
                                                                     field_name,
                                                                     type_name))
					})?
		        )),
                "int" => Field::new(&field_name, Type::Int(
                    if let Ok(parsed) = i32::from_str_radix(&data_value, 16) {
                        parsed
                    } else if data_value.starts_with("0x") {
                        // try parsing as a hexadecimal prefixed with a 
                        i32::from_str_radix(&data_value[2..], 16).map_err(|_| {
							AvroErr::AvroConversionFailed(failed_parsing(data_value,
                                                                     field_name,
                                                                     type_name))
						})?
                    } else {
						// try parsing normally
						data_value.parse::<i32>().map_err(|_| {
							AvroErr::AvroConversionFailed(failed_parsing(data_value,
                                                                     field_name,
                                                                     type_name))
						})?
                    }
		        )),
                "long" => Field::new(&field_name, Type::Long(
                    if let Ok(parsed) = u64::from_str_radix(&data_value, 16) {
                        parsed as i64
                    } else if data_value.starts_with("0x") {
                        i64::from_str_radix(&data_value[2..], 16).map_err(|_| {
							AvroErr::AvroConversionFailed(failed_parsing(data_value,
                                                                     field_name,
                                                                     type_name))
						})?
                    } else {
                        data_value.parse::<i64>().map_err(|_| {
							AvroErr::AvroConversionFailed(failed_parsing(data_value,
                                                                     field_name,
                                                                     type_name))
						})? as i64
                    }
                )),
                "boolean" => {
                    let field = match data_value.as_ref() {
                        "1" => Field::new(&field_name, Type::Bool(true)),
                        "0" => Field::new(&field_name, Type::Bool(false)),
                        _ => return Err(AvroErr::AvroConversionFailed(failed_parsing(data_value, field_name, type_name)))
                    };
                    field
                }
                other_type => return Err(AvroErr::AvroConversionFailed(format!("Avro schema conversion not yet implemented for: {:?} on channel {:?}", other_type, type_name)))
            };
            record.push_field(avro_field_type);
        }
        Ok(record)
    }
}
