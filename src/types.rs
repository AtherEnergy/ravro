//! Contains definitions of various avro types.

use std::io::{Read, Write};
use std::mem;
use std::str;
use std::collections::HashMap;
use complex::Record;
use errors::AvroErr;
use codec::{Encoder, Decoder};
use complex::Enum;

fn zig_zag(num: i64) -> u64 {
    if num < 0 {
        !((num as u64) << 1)
    } else {
        (num as u64) << 1
    }
}

fn encode_var_len<W: Write>(writer: &mut W, mut num: u64) -> Result<usize, AvroErr> {
    let mut write_cnt = 0;
    loop {
        let mut b = (num & 0b0111_1111) as u8;
        num >>= 7;
        if num == 0 {
            writer.write_all(&[b]).map_err(|_| AvroErr::EncodeErr)?;
            write_cnt += 1;
            break;
        }
        b |= 0b1000_0000;
        writer.write_all(&[b]).map_err(|_| AvroErr::EncodeErr)?;
        write_cnt += 1;
    }
    Ok(write_cnt)
}

/// Decodes a variable length encoded u64 from the given reader
fn decode_var_len_u64<R: Read>(reader: &mut R) -> Result<u64, AvroErr> {
    let mut num = 0;
    let mut i = 0;
    loop {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf).map_err(|_| AvroErr::DecodeErr)?;
        if i >= 9 && buf[0] & 0b1111_1110 != 0 {
            return Err(AvroErr::DecodeErr);
        }
        num |= (buf[0] as u64 & 0b0111_1111) << (i * 7);
        if buf[0] & 0b1000_0000 == 0 {
            break;
        }
        i += 1;
    }
    Ok(num)
}

/// Decodes a long or int from a zig zag encoded unsigned long 
fn decode_zig_zag(num: u64) -> i64 {
    if num & 1 == 1 {
        !(num >> 1) as i64
    } else {
        (num >> 1) as i64
    }
}

/// An enum containing all valid Type types in the Avro spec
#[derive(Debug, PartialEq, Clone)]
pub enum Type {
    /// Null avro type
    Null,
    /// Bool avro type
    Bool(bool),
    /// Int avro type
    Int(i32),
    /// Long avro type
    Long(i64),
    /// Float avro type
    Float(f32),
    /// Double avro type
    Double(f64),
    /// Bytes avro type
    Bytes(Vec<u8>),
    /// String avro type
    Str(String),
    /// Map avro type
    Map(HashMap<String, Type>),
    /// Record avro type
    Record(Record),
    /// Array avro type
    Array(Vec<Type>),
    /// Enum avro type
    Enum(Enum),
    /// Fixed avro type
    Fixed,
    /// Union avro type
    Union
}

// These methods are meant to be called only in contexts where we know before hand
// what rust type we are pulling out of a Type.
impl Type {
    /// Extracts a BTreeMap<_,_> out of Avro Type
    pub fn map_ref<'a>(&'a self) -> &'a HashMap<String, Type> {
        if let &Type::Map(ref bmap) = self {
            bmap
        } else {
            unreachable!();
        }
    }

    /// Extracts a bytes slice out of Avro Type
    pub fn bytes_ref<'a>(&'a self) -> &'a [u8] {
        if let &Type::Bytes(ref byte_vec) = self {
            byte_vec
        } else {
            unreachable!();
        }
    }

    /// Extracts a long out of Avro Type
    pub fn long_ref(&self) -> i64 {
        if let &Type::Long(l) = self {
            l
        } else {
            unreachable!();
        }
    }

    /// Extracts a long out of Avro Type
    pub fn int_ref(&self) -> i32 {
        if let &Type::Int(l) = self {
            l
        } else {
            unreachable!();
        }
    }

    /// Extracts a float out of Avro Type
    pub fn float_ref(&self) -> f32 {
        if let &Type::Float(f) = self {
            f
        } else {
            unreachable!();
        }
    }

    /// Extracts a double out of Avro Type
    pub fn double_ref(&self) -> f64 {
        if let &Type::Double(d) = self {
            d
        } else {
            unreachable!();
        }
    }
    /// Extracts a boolean out of Avro Type
    pub fn bool_ref(&self) -> bool {
        if let &Type::Bool(b) = self {
            b
        } else {
            unreachable!();
        }
    }

    /// Extracts a String out of Avro Type
    pub fn string_ref(&self) -> String {
        if let &Type::Str(ref s) = self {
            s.to_string()
        } else {
            unreachable!();
        }
    }
}

impl Decoder for i64 {
	type Out=i64;
	fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
		decode_var_len_u64(reader).map(decode_zig_zag).map_err(|_| AvroErr::DecodeErr)
	}
}

impl Decoder for i32 {
	type Out=i32;
	fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
		decode_var_len_u64(reader).map(decode_zig_zag).map(|a| a as i32).map_err(|_| AvroErr::DecodeErr)
	}
}

impl Decoder for () {
    type Out=();
    fn decode<R: Read>(_reader: &mut R) -> Result<Self::Out, AvroErr> {
        Ok(())
    }
}

impl Decoder for bool {
    type Out=bool;
    fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
        match reader.bytes().next() {
            Some(Ok(0x00)) => Ok(false),
            Some(Ok(0x01)) => Ok(true),
            _ => Err(AvroErr::DecodeErr)
        }
    }
}

impl Decoder for Vec<u8> {
    type Out=Vec<u8>;
    fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
        let bytes_len_decoded = i64::decode(reader)?;
        let mut data_buf = vec![0u8; bytes_len_decoded as usize];
        reader.read_exact(&mut data_buf).map_err(|_| AvroErr::AvroReadErr)?;
        Ok(data_buf.to_vec())
    }
}

impl Decoder for f32 {
    type Out=f32;
    fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
        let mut a = [0u8; 4];
        reader.read_exact(&mut a).map_err(|_| AvroErr::DecodeErr)?;
        Ok(unsafe { mem::transmute(a) })
    }
}

impl Decoder for f64 {
    type Out=f64;
    fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
        let mut a = [0u8; 8];
        reader.read_exact(&mut a).map_err(|_| AvroErr::DecodeErr)?;
        Ok(unsafe { mem::transmute(a) })
    }
}

impl Decoder for HashMap<String, String> {
    type Out=HashMap<String, String>;
    fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
        let mut map = HashMap::new();
        let sz = i64::decode(reader).unwrap();
        for _ in 0..sz {
            let decoded_key = String::decode(reader).unwrap();
            let decoded_val = String::decode(reader).unwrap();
            map.insert(decoded_key, decoded_val);
        }
        Ok(map)
    }
}

impl Decoder for String {
    type Out=Self;
    fn decode<R: Read>(reader: &mut R) -> Result<Self::Out, AvroErr> {
        let strlen = i64::decode(reader).unwrap();
        let mut str_buf = vec![0u8; strlen as usize];
        reader.read_exact(&mut str_buf).map_err(|_| AvroErr::DecodeErr)?;
        let st = str::from_utf8(str_buf.as_slice()).unwrap().to_string();
        Ok(st)
    }
}

impl Encoder for String {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr> {
        let mut total_len = 0;
        let strlen = self.chars().count();
        total_len += Type::Long(strlen as i64).encode(writer)?;
        let bytes = self.clone().into_bytes();
        total_len += bytes.len();
        writer.write_all(bytes.as_slice()).map_err(|_| AvroErr::EncodeErr)?;
        Ok(total_len)
    }
}

impl Encoder for Type {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr> {
        match *self {
            Type::Null => Ok(0),
            Type::Bool(val) => {
                if val {
                    writer.write_all(&[0x01]).map_err(|_| AvroErr::EncodeErr)?;
                } else {
                    writer.write_all(&[0x00]).map_err(|_| AvroErr::EncodeErr)?;
                }
                Ok(1)
            }
            Type::Int(val) => encode_var_len(writer, zig_zag(val as i64)),
            Type::Long(val) => encode_var_len(writer, zig_zag(val)),
            Type::Float(val) => {
                let buf: [u8; 4] = unsafe { mem::transmute(val) };
                writer.write_all(&buf).map_err(|_| AvroErr::EncodeErr)?;
                Ok(4)
            }
            Type::Double(val) => {
                let buf: [u8; 8] = unsafe { mem::transmute(val) };
                writer.write_all(&buf).map_err(|_| AvroErr::AvroWriteErr)?;
                Ok(8)
            }
            Type::Bytes(ref bytes) => {
                let mut total_len = 0;
                let byte_len = Type::Long(bytes.len() as i64);
                total_len += byte_len.encode(writer)?;
                total_len += bytes.len();
                let _ = writer.write_all(bytes);
                Ok(total_len)
            }
            Type::Str(ref s) => s.encode(writer),
            Type::Record(ref rec) => {
                 let mut total_len = 0;
                 for i in &rec.fields {
                     total_len += i.ty.encode(writer).map_err(|_| AvroErr::EncodeErr)?;
                 }
                 Ok(total_len)
            }
            Type::Map(ref bmap) => {
                let mut total_len = 0;
                let block_len = Type::Long(bmap.keys().len() as i64);
                total_len += block_len.encode(writer)?;
                for i in bmap.keys().zip(bmap.values()) {
                    total_len += i.0.encode(writer)?;
                    total_len += i.1.encode(writer)?;
                }
                // Mark the end of map type
                total_len += Type::Long(0i64).encode(writer)?;
                Ok(total_len)
            }
            Type::Array(ref arr) => {
                let mut total_len = 0;
                let block_len = Type::Long(arr.len() as i64);
                total_len += block_len.encode(writer)?;
                for i in arr {
                    total_len += i.encode(writer)?;
                }
                total_len += Type::Long(0).encode(writer)?;
                Ok(total_len)
            }
            Type::Enum(ref enum_type) => {
                enum_type.encode(writer)
            }
            Type::Fixed | Type::Union => unimplemented!(),
        }
    }
}

#[test]
fn test_float_encode_decode() {
    let mut vec = vec![];
    let f = Type::Float(0.0);
    let _ = f.encode(&mut vec);
    assert_eq!(&vec, &b"\x00\x00\x00\x00");

    let mut v = vec![];
    let f = Type::Float(3.14);
    let _ = f.encode(&mut v);
    assert_eq!(f32::decode(&mut v.as_slice()).unwrap(), 3.14);
}

#[test]
fn test_null_encode_decode() {
    let mut total_bytes = 0;
    let mut v = vec![];
    let null = Type::Null;
    total_bytes += null.encode(&mut v).unwrap();
    assert_eq!(0, v.as_slice().len());
    let decoded_null = <()>::decode(&mut v.as_slice()).unwrap();
    assert_eq!(decoded_null, ());
    assert_eq!(0, total_bytes);
}

#[test]
fn test_bool_encode_decode() {
    let mut total_bytes = 0;
    let b = Type::Bool(true);
    let mut v = Vec::new();
    let _ = b.encode(&mut v);
    assert_eq!(&v, &[1]);
    let mut v = vec![];
    let b = Type::Bool(false);
    total_bytes += b.encode(&mut v).unwrap();
    assert_eq!(false, bool::decode(&mut v.as_slice()).unwrap());
    assert_eq!(1, total_bytes);
}

#[test]
fn test_bytes_encode_decode() {
    let mut v: Vec<u8> = vec![];
    let bytes = Type::Bytes(b"some".to_vec());
    let _ = bytes.encode(&mut v);
    assert_eq!([8, 's' as u8, 'o' as u8,'m' as u8,'e' as u8].to_vec(), v);

    let decoded_bytes = Vec::<u8>::decode(&mut v.as_slice()).unwrap();
    assert_eq!(decoded_bytes, b"some");
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

#[test]
fn test_var_len_encoding() {
    let mut vec = vec![];

    assert_eq!(1, encode_var_len(&mut vec, 3).unwrap());
    assert_eq!(&vec, &b"\x03");
    vec.clear();

    assert_eq!(2, encode_var_len(&mut vec, 128).unwrap());
    assert_eq!(&vec, &b"\x80\x01");
    vec.clear();

    assert_eq!(2, encode_var_len(&mut vec, 130).unwrap());
    assert_eq!(&vec, &b"\x82\x01");
    vec.clear();

    assert_eq!(3, encode_var_len(&mut vec, 944261).unwrap());
    assert_eq!(&vec, &b"\x85\xD1\x39");
    vec.clear();

}

#[test]
fn test_long_encode_decode() {
    let to_encode = vec![100, -100, 1000, -1000];
    let mut total_bytes = 0;
    for v in to_encode {
        let mut e: Vec<u8> = Vec::new();
        total_bytes += Type::Long(v).encode(&mut e).unwrap();
        let d = i64::decode(&mut e.as_slice()).unwrap();
        assert_eq!(v, d);
    }
    assert_eq!(8, total_bytes);
}

#[test]
fn test_str_encode_decode() {
    let mut v = vec![];
    let b = Type::Str("foo".to_string());
    let len = b.encode(&mut v).unwrap();
    let v = String::decode(&mut v.as_slice()).unwrap();
    assert_eq!("foo".to_string(), v);
    assert_eq!(4, len);
}
