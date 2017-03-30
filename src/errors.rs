
// Errors variants in Avro

#[derive(Debug)]
pub enum AvroErr {
	AvroWriteErr,
	UnexpectedSchema,
	AvroReadErr,
	EncodeErr,
	DecodeErr
}
