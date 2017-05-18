
use std::io::Error;

// Errors variants in Avro

#[derive(Debug)]
pub enum AvroErr {
    AvroWriteErr,
    UnexpectedSchema,
    AvroReadErr,
    EncodeErr,
    DecodeErr,
    FullnameErr,
    AvroIOErr,
    UnexpectedData
}

impl From<Error> for AvroErr {
    fn from(val: Error) -> Self {
        AvroErr::AvroIOErr
    }
}

pub type AvroResult<T> = Result<T, AvroErr>;
