//! Contains definitions of various primitive avro types.

use std::io::{Read, Write};
pub use std::str::Bytes as StdBytes;
use codec::{Codec, EncodeErr, DecodeErr};
use std::collections::{BTreeMap, HashMap};
use std::mem;
use serde_json::Value;
use std::str;
use complex::RecordSchema;

#[derive(Debug, PartialEq, Clone)]
pub enum Schema {
    Null,
    Bool(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    Str(String),
    Map(BTreeMap<String, Schema>),
    Record(RecordSchema)
}

// TODO use it in decoder
// The DecodeValue depicts the current data to be parsed.
#[derive(Debug, Clone)]
pub enum DecodeValue {
    Null,
    Bool,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    Str,
    Map(Box<DecodeValue>),
    Record(RecordSchema),
    SyncMarker
}

impl Codec for String {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, EncodeErr> {
        let mut total_len = 0;
        let strlen = self.chars().count();
        total_len += Schema::Long(strlen as i64).encode(writer)?;
        let bytes = self.clone().into_bytes();
        total_len += bytes.len();
        writer.write_all(bytes.as_slice());
        Ok(total_len)
    }
    fn decode<R: Read>(reader: &mut R, schema_type: DecodeValue) -> Result<Self, DecodeErr> {
        let mut len_buf = vec![0u8; 1];
        reader.read_exact(&mut len_buf).unwrap();
        let strlen = Schema::decode(&mut len_buf.as_slice(), DecodeValue::Long).unwrap();
        if let Schema::Long(strlen) = strlen {
            let mut str_buf = vec![0u8; strlen as usize];
            let _ = reader.read_exact(&mut str_buf).unwrap();
            let st = str::from_utf8(str_buf.as_slice()).unwrap().to_string();
            Ok(st)
        } else {
            Err(DecodeErr)
        }
    }
}

// Internally keep the 
impl Codec for Schema {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, EncodeErr> {
        match self {
            &Schema::Null => {
                writer.write_all(&[0x00]);
                Ok(1)
            }
            &Schema::Bool(val) => {
                if val {
                    writer.write_all(&[0x01]).map_err(|_| EncodeErr)?;
                } else {
                    writer.write_all(&[0x00]).map_err(|_| EncodeErr)?;
                }
                Ok(1)
            }
            &Schema::Int(val) => encode_var_len(writer, zig_zag(val as i64)),
            &Schema::Long(val) => encode_var_len(writer, zig_zag(val)),
            &Schema::Float(val) => {
                let mut buf: [u8; 4] = unsafe { mem::transmute(val) };
                writer.write_all(&mut buf);
                Ok(4)
            }
            &Schema::Double(val) => {
                let mut buf: [u8; 8] = unsafe { mem::transmute(val) };
                writer.write_all(&mut buf);
                Ok(8)
            }
            &Schema::Bytes(ref bytes) => {
                let mut total_len = 0;
                let byte_len = Schema::Long(bytes.len() as i64);
                total_len += byte_len.encode(writer)?;
                total_len += bytes.len();
                let len = writer.write_all(&bytes);
                Ok(total_len)
            }
            &Schema::Str(ref s) => s.encode(writer),
            &Schema::Record(ref schema) => {
                 let mut total_len = 0;
                 for i in &schema.fields {
                     total_len += i.encode(writer).map_err(|_| EncodeErr)?;
                 }
                 Ok(total_len)
            }
            &Schema::Map(ref bmap) => {
                let mut total_len = 0;
                let block_len = Schema::Long(bmap.keys().len() as i64);
                total_len += block_len.encode(writer)?;
                for i in bmap.keys().zip(bmap.values()) {
                    total_len += i.0.encode(writer)?;
                    total_len += i.1.encode(writer)?;
                }
                // Mark the end of map type
                total_len += Schema::Long(0i64).encode(writer)?;
                Ok(total_len)
            }
        }
    }

    fn decode<R: Read>(reader: &mut R, mut schema_type: DecodeValue) -> Result<Self, DecodeErr> {
        match schema_type {
            DecodeValue::Null => {
                match reader.bytes().next() {
                    Some(Ok(0x00)) => Ok(Schema::Null),
                    _ => Err(DecodeErr)
                }
            }
            DecodeValue::Bool => {
                match reader.bytes().next() {
                    Some(Ok(0x00)) => Ok(Schema::Bool(false)),
                    Some(Ok(0x01)) => Ok(Schema::Bool(true)),
                    _ => Err(DecodeErr)
                }
            }
            DecodeValue::Long => {
                decode_var_len_u64(reader)
                .map(|b| decode_zig_zag(b))
                .map_err(|_| DecodeErr)
                .map(|d| Schema::Long(d))
            }
            DecodeValue::Float => {
                let mut a = [0u8; 4];
                reader.read_exact(&mut a);
                Ok(Schema::Float(unsafe { mem::transmute(a) }))
            }
            DecodeValue::Double => {
                let mut a = [0u8;8];
                reader.read_exact(&mut a);
                Ok(Schema::Double(unsafe { mem::transmute(a) }))
            }
            DecodeValue::Bytes => {
                let mut len_buf = vec![0u8; 1];
                let _ = reader.read_exact(&mut len_buf).unwrap();
                let bytes_len_decoded = Schema::decode(&mut len_buf.as_slice(), DecodeValue::Long).unwrap();
                if let Schema::Long(bytes_len_decoded) = bytes_len_decoded {
                    let mut data_buf = vec![0u8; bytes_len_decoded as usize];
                    reader.read_exact(&mut data_buf);
                    let byte = Schema::Bytes(data_buf.to_vec());
                    Ok(byte)
                } else {
                    Err(DecodeErr)
                }
            }
            DecodeValue::Str => {
                let mut len_buf = vec![0u8; 1];
                reader.read_exact(&mut len_buf).map_err(|_| DecodeErr)?;
                let strlen = Schema::decode(&mut len_buf.as_slice(), DecodeValue::Long).unwrap();
                if let Schema::Long(strlen) = strlen {
                    let mut str_buf = vec![0u8; strlen as usize];
                    let _ = reader.read_exact(&mut str_buf).unwrap();
                    let st = Schema::Str(str::from_utf8(str_buf.as_slice()).unwrap().to_string());
                    Ok(st)
                } else {
                    Err(DecodeErr)
                }
            }
            DecodeValue::Record(r) => {
                unimplemented!();
            },
            DecodeValue::Int => {
                decode_var_len_u64(reader)
                .map(|b| decode_zig_zag(b))
                .map_err(|_| DecodeErr)
                .map(|d| Schema::Int(d as i32))
            },
            DecodeValue::Map(val_schema) => {
                let mut map = BTreeMap::new();
                let mut v = vec![0u8; 1];
                reader.read_exact(&mut v);
                let sz = Schema::decode(&mut v.as_slice(), DecodeValue::Long).unwrap();
                if let Schema::Long(sz) = sz {
                    for i in 0..sz {
                        let decoded_key = Schema::decode(reader, DecodeValue::Str).unwrap();
                        let decoded_val = Schema::decode(reader, *val_schema.clone()).unwrap();
                        if let Schema::Str(s) = decoded_key {
                            map.insert(s, decoded_val);
                        }
                    }
                    Ok(Schema::Map(map))
                } else {
                    Err(DecodeErr)
                }
            }
            DecodeValue::SyncMarker => {
                warn!("Sync markers have their seperate Codec implementation");
                Err(DecodeErr)
            }
        }
    }
}

#[test]
fn test_float_encode_decode() {
    let mut vec = vec![];
    let f = Schema::Float(0.0);
    f.encode(&mut vec);
    assert_eq!(&vec, &b"\x00\x00\x00\x00");

    let mut v = vec![];
    let f = Schema::Float(3.14);
    f.encode(&mut v);
    assert_eq!(Schema::decode(&mut v.as_slice(), DecodeValue::Float).unwrap(), Schema::Float(3.14));
}

#[test]
fn test_int_encode_decode() {

}

#[test]
fn test_null_encode_decode() {
    let mut total_bytes = 0;
    // Null encoding
    let mut v = vec![];
    let null = Schema::Null;
    total_bytes += null.encode(&mut v).unwrap();
    assert_eq!(&[0x00], v.as_slice());

    // Null decoding
    let decoded_null = Schema::decode(&mut v.as_slice(), DecodeValue::Null).unwrap();
    assert_eq!(decoded_null, Schema::Null);
    assert_eq!(1, total_bytes);
}

#[test]
fn test_bool_encode_decode() {
    let mut total_bytes = 0;
    let b = Schema::Bool(true);
    let mut v = Vec::new();
    b.encode(&mut v);
    assert_eq!(&v, &[1]);
    let mut v = vec![];
    let b = Schema::Bool(false);
    total_bytes += b.encode(&mut v).unwrap();
    assert_eq!(Schema::Bool(false), Schema::decode(&mut v.as_slice(), DecodeValue::Bool).unwrap());
    assert_eq!(1, total_bytes);
}

#[test]
fn test_bytes_encode_decode() {
    let mut v: Vec<u8> = vec![];
    let bytes = Schema::Bytes(b"some".to_vec());
    bytes.encode(&mut v);
    assert_eq!([8, 's' as u8, 'o' as u8,'m' as u8,'e' as u8].to_vec(), v);

    let decoded = Schema::decode(&mut v.as_slice(), DecodeValue::Bytes).unwrap();
    if let Schema::Bytes(b) = decoded {
           assert_eq!(b, b"some");
    }
}

fn zig_zag(num: i64) -> u64 {
    if num < 0 {
        !((num as u64) << 1)
    } else {
        (num as u64) << 1
    }
}

#[test]
fn test_zigzag_encoding() {
    assert_eq!(zig_zag(0), 0);
    assert_eq!(zig_zag(-1), 1);
    assert_eq!(zig_zag(-3), 5);
    assert_eq!(zig_zag(3), 6);
    assert_eq!(zig_zag(-50),99);
    assert_eq!(zig_zag(50),100);
    assert_eq!(zig_zag(i64::min_value()), 0xFFFFFFFF_FFFFFFFF);
    assert_eq!(zig_zag(i64::max_value()), 0xFFFFFFFF_FFFFFFFE);
}

fn encode_var_len<W>(writer: &mut W, mut num: u64) -> Result<usize, EncodeErr>
where W: Write {
    let mut write_cnt = 0;
    loop {
        let mut b = (num & 0b0111_1111) as u8;
        num >>= 7;
        if num == 0 {
            writer.write(&[b]).map_err(|_| EncodeErr)?;
            write_cnt += 1;
            break;
        }
        b |= 0b1000_0000;
        writer.write(&[b]).map_err(|_| EncodeErr)?;
        write_cnt += 1;
    }
    Ok(write_cnt)
}

#[test]
fn test_var_len_encoding() {
    let mut vec = vec![];

    encode_var_len(&mut vec, 3);
    assert_eq!(&vec, &b"\x03");
    vec.clear();

    encode_var_len(&mut vec, 128);
    assert_eq!(&vec, &b"\x80\x01");
    vec.clear();

    encode_var_len(&mut vec, 130);
    assert_eq!(&vec, &b"\x82\x01");
    vec.clear();

    encode_var_len(&mut vec, 944261);
    assert_eq!(&vec, &b"\x85\xD1\x39");
    vec.clear();

}

pub fn decode_zig_zag(num: u64) -> i64 {
    if num & 1 == 1 {
        !(num >> 1) as i64
    } else {
        (num >> 1) as i64
    }
}

pub fn decode_var_len_u64<R: Read>(reader: &mut R) -> Result<u64, DecodeErr> {
    let mut num = 0;
    let mut i = 0;
    loop {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf).map_err(|_| DecodeErr)?;
        if i >= 9 && buf[0] & 0b1111_1110 != 0 {
            return Err(DecodeErr);
        }
        num |= (buf[0] as u64 & 0b0111_1111) << (i * 7);
        if buf[0] & 0b1000_0000 == 0 {
            break;
        }
        i += 1;
    }
    Ok(num)
}

#[test]
fn test_long_encode_decode() {
    let to_encode = vec![Schema::Long(100), Schema::Long(-100), Schema::Long(1000), Schema::Long(-1000)];
    let mut total_bytes = 0;
    for v in to_encode {
        let mut e: Vec<u8> = Vec::new();
        total_bytes += v.encode(&mut e).unwrap();
        let d = Schema::decode(&mut e.as_slice(), DecodeValue::Long).unwrap();
        assert_eq!(v, d);
    }
    assert_eq!(8, total_bytes);
}

#[test]
fn test_map_encode_decode() {
    use std::io::Read;
    let mut my_map = BTreeMap::new();
    my_map.insert("foo".to_owned(), Schema::Bool(true));
    let my_map = Schema::Map(my_map);
    let mut v = Vec::new();
    let len = my_map.encode(&mut v).unwrap();
    // Check total bytes we encoded
    // Length should be 7 according to:
    // 1) Map key count 1 byte
    // 2) Map string key encoding 4 byte
    // 3) Map value 1 byte
    // 4) End of map marker 1 byte
    assert_eq!(len, 7);
    let decoded_map = Schema::decode(&mut v.as_slice(), DecodeValue::Map(Box::new(DecodeValue::Bool))).unwrap();
    if let Schema::Map(decoded_map) = decoded_map {
        if let &Schema::Bytes(ref b) = decoded_map.get("foo").unwrap() {
            assert_eq!("bar", str::from_utf8(b).unwrap());
        }
    }
}

#[test]
fn test_str_encode_decode() {
    let mut v = vec![];
    let b = Schema::Str("foo".to_string());
    let len = b.encode(&mut v).unwrap();
    if let Schema::Str(v) = Schema::decode(&mut v.as_slice(), DecodeValue::Str).unwrap() {
        assert_eq!("foo".to_string(), v);
    }
    assert_eq!(4, len);
}
