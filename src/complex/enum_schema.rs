
// pub struct EnumSchema;

use errors::AvroErr;
use codec::Encoder;
use types::Schema;
use std::io::Write;

#[derive(Clone, PartialEq, Debug)]
/// An avro complex type akin to enums in most languages 
pub struct EnumSchema {
	name: String,
	symbols: Vec<String>,
	current_val: Option<String>
}

impl EnumSchema {
	// TODO populate values from the schema
	/// Creates a new enum schema from a list of symbols
	pub fn new(name: &str, symbols: &[&'static str]) -> Self {
		let mut v = Vec::new();

		for i in 0..symbols.len() {
			v.push(symbols[i].to_string());
		}
		EnumSchema {
			name:name.to_string(),
			symbols: v,
			current_val: None
		}
	}

	/// sets the active enum variant
	pub fn set_value(&mut self, val: &str) {
		self.current_val = Some(val.to_string());
	}
}

impl Encoder for EnumSchema {
	fn encode<W: Write>(&self, writer: &mut W) -> Result<usize, AvroErr> {
		if let Some(ref current_val) = self.current_val {
			let idx = self.symbols.iter().position(|it| it == current_val).unwrap();
			let int: Schema = (idx as i64).into();
			int.encode(writer)
		} else {
			Err(AvroErr::EncodeErr)
		}
	}
}

// TODO implement decoding
// impl Decoder for EnumSchema {
// 	type Out=Self;
// 	fn decode<R: Read>(self, reader: &mut R) -> Result<Self::Out, AvroErr> {
// 		let sym_idx = FromAvro::Long.decode(reader).unwrap().long_ref();
// 		let resolved_val = self.symbols[sym_idx as usize].clone();
// 		let schema = EnumSchema { name: self.name,
// 								  symbols: self.symbols,
// 								  current_val: Some(resolved_val)
// 		};
// 		Ok(schema)
// 	}
// }

#[test]
fn enum_encode() {
    let symbols = ["CLUB", "DIAMOND", "SPADE"];
    let mut enum_schm = EnumSchema::new("deck_of_cards", &symbols);
    enum_schm.set_value("DIAMOND");
    let mut vec: Vec<u8> = vec![];
    let  _ = enum_schm.encode(&mut vec);
    let val = FromAvro::Long.decode(&mut vec.as_slice()).unwrap().long_ref();
    assert_eq!(1,  val);
}