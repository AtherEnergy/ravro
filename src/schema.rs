//! Contains declaration of a struct repr of the Schema type

use std::fs::OpenOptions;
use std::path::Path;
use serde_json::{Value, from_reader, from_str};
use std::str;
use errors::AvroErr;

// fn parse_schema(json_value: &Value) -> 

/// `AvroSchema` represents user provided schema file which gets parsed as a json object.
pub struct AvroSchema(pub Value);
impl AvroSchema {
    /// Create a AvroSchema from a given file `Path`.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, AvroErr> {
        OpenOptions::new().read(true)
                          .open(path)
                          .map_err(|_| AvroErr::InvalidUserSchema)
                          .and_then(|f| {
           from_reader(f).map_err(|_| AvroErr::InvalidUserSchema).map(|m| AvroSchema(m))
        })
    }

    /// Create a AvroSchema from a schema as a string
    pub fn from_str(schema: &str) -> Result<Self, AvroErr> {
        from_str(schema).map_err(|_| AvroErr::InvalidUserSchema).map(|m| AvroSchema(m))
    }

    /// Returns an optional string slice for the schema.
    pub fn as_str(&self) -> Option<&str> {
        self.0.as_str()
    }
}


#[cfg(test)]
mod tests {
    use writer::Header;
    use std::fs::OpenOptions;
    use conversion::Decoder;
    #[test]
    fn test_parse_header() {
        let mut f = OpenOptions::new()
            .read(true)
            .open("tests/encoded/double_encoded.avro")
            .unwrap();
        let magic_buf = [0u8; 4];
        let header = Header::new().decode(&mut f);
        assert!(header.is_ok());
    }
}
