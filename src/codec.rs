//! Contains traits for doing encoding and decoding between avro types

use std::io::Write;
use std::io::Read;
use types::DecodeValue;

// TODO add more error variants
err_structs!(EncodeErr, DecodeErr);

pub trait Codec {
	fn encode<W>(&self, writer: &mut W) -> Result<usize, EncodeErr> where W: Write;
	fn decode<R>(reader: &mut R, schema_type: DecodeValue) -> Result<Self, DecodeErr> where Self: Sized, R: Read;
}
