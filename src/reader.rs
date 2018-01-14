
use std::io::Read;
use std::path::Path;
use std::fs::OpenOptions;
use writer::{Header, SyncMarker, Codec, decompress_snappy};
use codec::Decoder;
use types::Schema;
use std::fs::File;
use std::marker::PhantomData;
use types::FromAvro;
use errors::AvroResult;
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
/// Design: A BlockReader represents a data block in avro and it implements the Iterator trait. So it
/// is provide convenient api for users to just write a for loop to read data blocks from an avro file.
/// It abstracts away the details of reading both uncompressed and compresssed writes.
/// TODO write more on it.
pub struct BlockReader<T> {
    sync: SyncMarker,
    stream: File,
    schema: FromAvro,
    block_count: u64,
    parsed_data: PhantomData<T>,
    codec: Codec,
    decompressed_data: Option<Cursor<Vec<u8>>>,
}

impl<T: From<Schema>> BlockReader<T> {
    fn decode_block(&mut self) -> Option<T> {
        self.schema.clone().decode(&mut self.stream).ok().map(|d| {
            self.block_count -= 1;
            d.into()
        })
    }

    fn decode_compressed_block(&mut self) -> Option<T> {
        if self.decompressed_data.is_some() {
            let mut data = self.decompressed_data.take().unwrap();
            let decoded = self.schema.clone().decode(&mut data).ok().map(|d| {
                self.block_count -= 1;
                d.into()
            });
            self.decompressed_data = Some(data);
            decoded
        } else {
            let compressed_data_len_plus_cksum = FromAvro::Long.decode(&mut self.stream).unwrap().long_ref();
            let compressed_data_len = compressed_data_len_plus_cksum - 4;
            
            let mut compressed_buf = vec![0u8; compressed_data_len as usize];
            let _ = self.stream.read_exact(&mut compressed_buf);
            let decompressed_data = decompress_snappy(compressed_buf.as_slice());
            let mut cksum_buf = vec![0u8; 4];
            let _ = self.stream.read_exact(&mut cksum_buf);
            let cksum = cksum_buf.as_slice().read_u32::<BigEndian>().unwrap();
            let mut cursored_data = Cursor::new(decompressed_data);
            let decoded = self.schema.clone().decode(&mut cursored_data).ok().map(|d| {
                self.block_count -= 1;
                d.into()
            });
            self.decompressed_data = Some(cursored_data);
            decoded
        }
        
    }
}

impl<T: From<Schema>> Iterator for BlockReader<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.block_count == 0 {
            let end_marker = SyncMarker::new().decode(&mut self.stream).unwrap();
            if !(end_marker == self.sync) {
                error!("Possible data corruption! Sync markers do not match");
            }
            return None;
        } else {
            match self.codec {
                Codec::Null => self.decode_block(),
                Codec::Snappy => self.decode_compressed_block(),
                Codec::Deflate => unimplemented!()
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
        let codec = self.header.get_codec().unwrap();
        if let Codec::Null = codec {
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
            codec: codec,
            decompressed_data: None
        }
    }
}
