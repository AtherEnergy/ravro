
extern crate ravro;

use std::process::Command;
use std::str;
use self::ravro::AvroWriter;
use self::ravro::Codec;

pub fn test_writer(schema_file: &str, codec: Codec) -> AvroWriter {
	let mut data_writer = AvroWriter::from_schema(schema_file).unwrap();
	data_writer.set_codec(codec);
	let data_writer = data_writer.build().unwrap();
	data_writer
}

pub fn get_java_tool_output(encoded: &str) -> Result<String, ()> {
    let a = Command::new("java")
            .args(&["-jar", "avro-tools-1.8.2.jar", "tojson", encoded])
            .output()
            .expect("failed to execute process");
    str::from_utf8(&a.stdout).map_err(|_| ()).map(|s| s.to_string())
}
