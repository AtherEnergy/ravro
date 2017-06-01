
use std::io::Error;

// TODO expand this module to have more descriptive and detailed errors

/// Errors variants in Avro
#[derive(Debug)]
pub enum AvroErr {
    /// Avro file read errors
    AvroWriteErr,
    /// A unexpected schema was encountered
    UnexpectedSchema,
    /// Avro file read errors
    AvroReadErr,
    /// Error encountered while encoding avro data file
    EncodeErr,
    /// Error encountered while decoding avro data file
    DecodeErr,
    /// A named complex type does not confirm to a valid full name as defined in spec
    InvalidFullname,
    /// Variant which corresponds to Avro file read/write errors.
    AvroIOErr,
    /// An unexpected data was parsed.
    UnexpectedData,
    /// An unexpected codec was detected.
    UnexpectedCodec
}

impl From<Error> for AvroErr {
    fn from(val: Error) -> Self {
        AvroErr::AvroIOErr
    }
}

/// Wraps a Result containing an avro specific error.
pub type AvroResult<T> = Result<T, AvroErr>;
