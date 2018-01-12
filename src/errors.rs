
use std::io::Error as StdError;

// TODO expand this module to have more descriptive and detailed errors

use failure::Error;

/// The error enum wraps all kinds of errors during serialization/deserialization
#[derive(Debug, Fail)]
pub enum AvroErr {
    /// Avro file read errors
    #[fail(display = "Failed to write avro data")]
    AvroWriteErr,
    /// A unexpected schema was encountered
    #[fail(display = "An unexpected schema was encountered")]
    UnexpectedSchema,
    /// Avro file read errors
    #[fail(display = "Failed to read avro data")]
    AvroReadErr,
    /// Error encountered while encoding avro data file
    #[fail(display = "Error encountered while encoding avro data file")]
    EncodeErr,
    /// Error encountered while decoding avro data file
    #[fail(display = "Error encountered while decoding avro data file")]
    DecodeErr,
    /// A named complex type does not confirm to a valid full name as defined in spec
    #[fail(display = "A named complex type does not confirm to a valid full name as defined in spec")]
    InvalidFullname,
    /// Variant which corresponds to Avro file read/write errors.
    #[fail(display = "Avro I/O error")]
    AvroIOErr,
    /// An unexpected data was parsed.
    #[fail(display = "An unexpected data was parsed")]
    UnexpectedData,
    /// An unexpected codec was detected.
    #[fail(display = "An unexpected codec was detected")]
    UnexpectedCodec
}

impl From<StdError> for AvroErr {
    fn from(val: StdError) -> Self {
        AvroErr::AvroIOErr
    }
}

impl From<Error> for AvroErr {
    fn from(val: Error) -> Self {
        AvroErr::AvroIOErr
    }
}

/// Wraps a Result containing an avro specific error.
pub type AvroResult<T> = Result<T, AvroErr>;
