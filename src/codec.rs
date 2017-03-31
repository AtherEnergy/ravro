//! Contains traits for encoding and decoding between avro types

use std::io::Write;
use std::io::Read;
use types::DecodeValue;
use errors::AvroErr;

pub trait Codec {
	fn encode<W>(&self, writer: &mut W) -> Result<usize, AvroErr>
	where W: Write;
	fn decode<R>(reader: &mut R, schema_type: DecodeValue) -> Result<Self, AvroErr>
	where Self: Sized, R: Read;
}
