
use std::io::Read;
use std::path::Path;
use std::fs::OpenOptions;
use datafile::{Header, SyncMarker, DataWriter, Codecs, decompress_snappy};
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

use byteorder::{BigEndian, ReadBytesExt};

pub struct AvroReader {
    header: Header,
    buffer: File,
    block_count: u64
}

#[derive(Debug)]
pub struct BlockReader<T> {
    sync: SyncMarker,
    stream: File,
    schema: FromAvro,
    block_count: u64,
    parsed_data: PhantomData<T>,
    codec: Codecs
}

impl<T: From<Schema>> BlockReader<T> {
    fn decode_block_stream(&mut self) -> Option<T> {
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

    fn decode_compressed_block_stream(&mut self) -> Option<T> {
        let compressed_data_len_plus_cksum = FromAvro::Long.decode(&mut self.stream).unwrap().long_ref();
        let compressed_data_len = compressed_data_len_plus_cksum - 4;
        let mut compressed_buf = vec![0u8;compressed_data_len as usize];
        let decompressed_data = decompress_snappy(compressed_buf.as_slice());
        // TODO allow continuous reads of Schema here
        // TODO fix reading the remaining elements
        let mut cksum_buf = vec![0u8;2];
        self.stream.read_exact(&mut cksum_buf);
        let cksum = cksum_buf.as_slice().read_u16::<BigEndian>().unwrap();
        // for now just implemented for a string
        FromAvro::Str.decode(&mut self.stream).ok().map(|d|d.into())
    }
}

impl<T: From<Schema>> Iterator for BlockReader<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.block_count == 0 {
            let end_marker = SyncMarker::new().decode(&mut self.stream).unwrap();
            if !(end_marker == self.sync) {
                println!("Possible data corruption! Sync markers do not match");
            }
            return None;
        } else {
            match self.codec {
                Codecs::Null => {
                    self.decode_block_stream()
                },
                Codecs::Snappy => self.decode_compressed_block_stream(),
                Codecs::Deflate => unimplemented!()
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
        // let buf_len = FromAvro::Long.decode(&mut reader)?;
        let avro_reader = AvroReader {
            header: header,
            buffer: reader,
            block_count: block_count.long_ref() as u64
        };
        Ok(avro_reader)
    }

    pub fn iter_block<T>(self) -> BlockReader<T> {
        let mut reader = self.buffer;
        let schema = self.header.get_schema().unwrap();
        let codec = self.header.get_codec().unwrap();
        if let Codecs::Null = codec {
            // Read the serialized buffer size if no codec, as in case of codec present
            // we do it in our iter calls
            // TODO this is unused. use it
            let ser_data_sz = FromAvro::Long.decode(&mut reader);
        }
        let Header {magic, metadata, sync_marker} = self.header;
        BlockReader {
            stream: reader,
            sync: sync_marker,
            schema: schema,
            parsed_data: PhantomData,
            block_count: self.block_count,
            codec: codec
        }
    }
}

#[test]
fn reading_string_uncompressed() {
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
    let _ = data_writer.write("avro".to_string());
    let _ = data_writer.write("string".to_string());
    let _ = data_writer.commit_block(&mut writer);
    let _ = writer_file.write_all(&writer.into_inner());

    // Read that data
    let reader = AvroReader::from_path("tests/encoded/string_for_read.avro").unwrap();
    let mut a: BlockReader<Schema> = reader.iter_block();
    assert_eq!(a.next().unwrap().string_ref(), "Reading".to_string());
    assert_eq!(a.next().unwrap().string_ref(), "avro".to_string());
    assert_eq!(a.next().unwrap().string_ref(), "string".to_string());
    assert_eq!(a.next(), None);
}

#[test]
fn reading_string_compressed() {
    // Write some data
    let schema_file = "tests/schemas/string_schema.avsc";
    let string_schema = AvroSchema::from_file(schema_file).unwrap();
    let datafile_name = "tests/encoded/string_for_read_comp.avro";
    let mut writer_file = OpenOptions::new().write(true)
                                   .create(true)
                                   .open(datafile_name).unwrap();
    let mut writer = Cursor::new(Vec::new());
    let mut data_writer = DataWriter::new(string_schema, &mut writer, Codecs::Snappy).unwrap();
    let _ = data_writer.write("Reading".to_string());
    // let _ = data_writer.write("avro".to_string());
    // let _ = data_writer.write("string".to_string());
    let _ = data_writer.commit_block(&mut writer);
    let _ = writer_file.write_all(&writer.into_inner());
    // Read that data
    let reader = AvroReader::from_path("tests/encoded/string_for_read_comp.avro").unwrap();
    let mut a: BlockReader<Schema> = reader.iter_block();
    assert_eq!(a.next().unwrap().string_ref(), "Reading".to_string());
}

