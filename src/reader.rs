
use std::io::Read;
use std::path::Path;
use std::fs::OpenOptions;
use datafile::{Header, SyncMarker, DataWriter, Codecs};
use conversion::Decoder;
use types::Schema;
use std::fs::File;
use std::marker::PhantomData;
use types::FromAvro;

use errors::AvroResult;
use std::fmt::Debug;
use std::io::Write;
use schema::AvroSchema;
use std::io::Cursor;

pub struct AvroReader {
    header: Header,
    buffer: File,
    block_count: u64
}

#[derive(Debug)]
pub struct DataReader<T> {
    sync: SyncMarker,
    stream: File,
    schema: FromAvro,
    block_count: u64,
    parsed_data: PhantomData<T>
}

impl<T: From<Schema>> Iterator for DataReader<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.block_count == 0 {
            return None;
        } else {
            match self.schema {
                FromAvro::Double => {
                    FromAvro::Double.decode(&mut self.stream).ok().map(|d| {
                        self.block_count -= 1;
                        d.into()
                    })
                },
                FromAvro::Long => {
                    FromAvro::Long.decode(&mut self.stream).ok().map(|d| {
                        self.block_count -= 1;
                        d.into()
                    })
                },
                FromAvro::Str => {
                    FromAvro::Str.decode(&mut self.stream).ok().map(|d| {
                        self.block_count -= 1;
                        d.into()
                    })
                    
                },
                _ => None
            }
        }
    }
}

impl AvroReader {
    pub fn from_path<R: AsRef<Path>>(path: R) -> AvroResult<Self> {
        let mut reader = OpenOptions::new().read(true).open(path)?;
        let header = Header::new().decode(&mut reader)?; 
        let block_count = FromAvro::Long.decode(&mut reader)?;
        // TODO buf_len is unused as of now
        let buf_len = FromAvro::Long.decode(&mut reader)?;
        let avro_reader = AvroReader {
            header: header,
            buffer: reader,
            block_count: block_count.long_ref() as u64
        };
        Ok(avro_reader)
    }

    pub fn iter<T>(self) -> DataReader<T> {
        let reader = self.buffer;
        let schema = self.header.get_schema().unwrap();
        let Header {magic, metadata, sync_marker} = self.header;
        DataReader {
            stream: reader,
            sync: sync_marker,
            schema: schema,
            parsed_data: PhantomData,
            block_count: self.block_count
        }
    }
}

#[test]
fn reading_string() {
    // Write some data
    let schema_file = "tests/schemas/string_schema.avsc";
    let string_schema = AvroSchema::from_file(schema_file).unwrap();
    let datafile_name = "tests/encoded/string_for_read.avro";
    let mut writer_file = OpenOptions::new().write(true)
                                   .create(true)
                                   .open(datafile_name).unwrap();
    let mut writer = Cursor::new(Vec::new());
    let mut data_writer = DataWriter::new(string_schema, &mut writer, Codecs::Null).unwrap();
    let _ = data_writer.write("Reading".to_string());
    let _ = data_writer.write("from".to_string());
    let _ = data_writer.write("avro".to_string());
    let _ = data_writer.write("string".to_string());
    let _ = data_writer.commit_block(&mut writer);
    let _ = writer_file.write_all(&writer.into_inner());

    // Read that data
    let reader = AvroReader::from_path("tests/encoded/string_for_read.avro").unwrap();
    let mut a: DataReader<Schema> = reader.iter();
    assert_eq!(a.next().unwrap().string_ref(), "Reading".to_string());
    assert_eq!(a.next().unwrap().string_ref(), "from".to_string());
    assert_eq!(a.next().unwrap().string_ref(), "avro".to_string());
    assert_eq!(a.next().unwrap().string_ref(), "string".to_string());
    assert_eq!(a.next(), None);
}
