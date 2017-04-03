//! Contains traits for encoding and decoding between avro types

use std::io::Write;
use std::io::Read;
use errors::AvroErr;

pub trait Encoder {
	fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr>;
}

pub trait Decoder {
	type Out;
	fn decode<R: Read>(self, reader: &mut R) -> Result<Self::Out, AvroErr>;
}
