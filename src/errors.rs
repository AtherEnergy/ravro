
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
    #[fail(display = "Error encountered while encoding avro data file: {}", _0)]
    EncodeErr(String),
    /// Error encountered while decoding avro data file
    #[fail(display = "Error encountered while decoding avro data file: {}", _0)]
    DecodeErr(String),
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

/// The error enum wraps all kinds of errors during parsing of schema_declaration
#[derive(Debug, Fail)]
pub enum SchemaParseErr {
    /// Not a valid schema declaration
    #[fail(display = "Not a valid schema declaration")]
    InvalidSchema,
    /// Schema file not found
    #[fail(display = "Could not find schema file")]
    NotFound
}

impl From<StdError> for AvroErr {
    fn from(_val: StdError) -> Self {
        AvroErr::AvroIOErr
    }
}

impl From<Error> for AvroErr {
    fn from(_val: Error) -> Self {
        AvroErr::AvroIOErr
    }
}

impl From<String> for AvroErr {
    fn from(_val: String) -> Self {
        AvroErr::UnexpectedSchema
    }
}
