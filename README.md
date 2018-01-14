
## ravro - A pure rust implementation of the Avro Spec. [WIP]

[![Build Status](https://travis-ci.org/Ather-Energy/ravro.svg?branch=master)](https://travis-ci.org/Ather-Energy/ravro)

Implementation Status:

- [X] Spec compliant Header encoding.

- [X] Encoding (Primitive Types)

- [X] Encoding (Complex Types - Record, Array, Maps); TODO others

- [X] Snappy compression support

- [X] A unified DataWriter interface to write arbitrary rust types to avro data file.

- [X] Decoding Header

- [ ] Decoding (Primitive Types)

- [ ] Decoding (Complex Types)

Getting Started

ravro is ~~available on crates.io~~. It is recommended to look there for the newest released version, as well as links to the newest builds of the docs.

At the point of the last update of this README, the latest published version could be used like this:

Add the following dependency to your Cargo manifest...

[dependencies]
ravro = "0.1"

...and see the docs for how to use it.

### Examples

```rust
// test example taken from `/tests/primitive.rs`
let schema_file = "tests/schemas/bool_schema.avsc";
let bool_schema = AvroSchema::from_file(schema_file).unwrap();
let datafile_name = "tests/encoded/bool_encoded.avro";
let mut data_writer = DataWriter::new(bool_schema, Codecs::Snappy).unwrap();
let _ = data_writer.write(true);
let _ = data_writer.write(false);
let _ = data_writer.commit_block();
data_writer.flush_to_disk(datafile_name);
assert_eq!(Ok("true\nfalse\n".to_string()), common::get_java_tool_output(datafile_name));
```

## License

ravro is licensed under the terms of the MIT License or the Apache License 2.0, at your choosing.

Code of Conduct

Contribution to the `ravro` crate is organized under the terms of the Contributor Covenant, the maintainer of failure, @creativcoder, promises to intervene to uphold that code of conduct.