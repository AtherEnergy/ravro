
extern crate ravro;

use ravro::AvroSchema;
use ravro::schema::SchemaField;

#[test]
fn test_parse_schema() {
	let scm_path = "tests/schemas/record_schema.avsc";
	let s = AvroSchema::from_file(scm_path).unwrap();
	let fields = s.record_field_pairs().unwrap();

	let mut name_string = SchemaField::new("name", "string");
	name_string.set_default(Some("bike"));
	let canframe_long = SchemaField::new("canFrame", "long");
	let gps_long = SchemaField::new("gps", "long");
	let lsmsensor_long = SchemaField::new("lsmsensor", "long");
	assert!(fields[0] == name_string);
	assert!(fields[1] == canframe_long);
	assert!(fields[2] == gps_long);
	assert!(fields[3] == lsmsensor_long);
}
