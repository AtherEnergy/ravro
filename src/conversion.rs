//! Contains traits for encoding and decoding between avro types

use std::io::Write;
use std::io::Read;
use errors::AvroErr;

/// The Encoder trait provides methods for decoding data from an avro data file.
pub trait Encoder {
    /// Allows encoding a given type to a writer
	fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr>;
}

/// The Decoder provides methods for decoding data from an avro data file.
pub trait Decoder {
    /// The type that must be decoded out of the reader 
	type Out;
    /// Allows decoding a type out of a given Reader
	fn decode<R: Read>(self, reader: &mut R) -> Result<Self::Out, AvroErr>;
}
