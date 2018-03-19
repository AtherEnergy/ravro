
extern crate ravro;

use ravro::AvroSchema;

#[test]
fn test_parse_schema() {
	let scm_path = "tests/schemas/record_schema.avsc";
	let s = AvroSchema::from_file(scm_path).unwrap();
	let fields = s.record_field_pairs().unwrap();
	assert!(fields[0] == ("name".to_string(), "string".to_string()));
	assert!(fields[1] == ("canFrame".to_string(), "long".to_string()));
	assert!(fields[2] == ("gps".to_string(), "long".to_string()));
	assert!(fields[3] == ("lsmsensor".to_string(), "long".to_string()));
	assert!(fields[4] == ("map".to_string(), "string".to_string()));
}
