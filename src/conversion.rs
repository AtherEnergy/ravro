//! Contains traits for encoding and decoding between avro types

use std::io::Write;
use std::io::Read;
use errors::AvroErr;

pub trait Encoder {
	fn encode<W>(&self, writer: &mut W) -> Result<usize, AvroErr> where W: Write;
}

pub trait Decoder {
	type Out;
	fn decode<R>(self, reader: &mut R) -> Result<Self::Out, AvroErr> where Self: Sized, R: Read;
}