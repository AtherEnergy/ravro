
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

/// Allows reading from an avro data file
#[derive(Debug)]
pub struct AvroReader {
    header: Header,
    buffer: File,
    block_count: u64
}

#[derive(Debug)]
/// The iterator implementation for iterating over blocks of avro data file
pub struct BlockReader<T> {
    sync: SyncMarker,
    stream: File,
    schema: FromAvro,
    block_count: u64,
    parsed_data: PhantomData<T>,
    codec: Codecs
}

// TODO figure out why the next call results in failing of data reading

impl<T: From<Schema>> BlockReader<T> {
    fn demultiplex_schema(&mut self) -> Option<T> {
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
            FromAvro::Bool => {
                FromAvro::Bool.decode(&mut self.stream).ok().map(|d| {
                    self.block_count -= 1;
                    d.into()
                })
            },
            _ => None
        }
    }

    fn decode_block_stream(&mut self) -> Option<T> {
        self.demultiplex_schema()
    }

    fn decode_compressed_block_stream(&mut self) -> Option<T> {
        let compressed_data_len_plus_cksum = FromAvro::Long.decode(&mut self.stream).unwrap().long_ref();
        let compressed_data_len = compressed_data_len_plus_cksum - 4;
        let mut compressed_buf = vec![0u8;compressed_data_len as usize];
        let decompressed_data = decompress_snappy(compressed_buf.as_slice());
        // TODO allow continuous reads of Schema here
        // TODO fix reading the remaining elements
        let mut cksum_buf = vec![0u8; 2];
        self.stream.read_exact(&mut cksum_buf);
        let cksum = cksum_buf.as_slice().read_u16::<BigEndian>().unwrap();
        self.demultiplex_schema()
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
                Codecs::Null => self.decode_block_stream(),
                Codecs::Snappy => self.decode_compressed_block_stream(),
                Codecs::Deflate => unimplemented!()
            }
            
        }
    }
}

impl AvroReader {
    /// Create a avro reader from an existing data file
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

    /// Returns an interator for data blocks in an avro data file
    pub fn iter_block<T>(self) -> BlockReader<T> {
        let mut reader = self.buffer;
        let schema = self.header.get_schema().unwrap();
        print!("BLOCK SCHEMA {:?}", schema);
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

