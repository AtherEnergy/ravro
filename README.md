
## ravro - A pure rust implementation of the Avro Spec. [WIP]

[![Build Status](https://travis-ci.org/Ather-Energy/ravro.svg?branch=master)](https://travis-ci.org/Ather-Energy/ravro)

Implementation Status:

- [X] Encoding (Primitive Types)

- [X] Encoding (Complex Types - Record, Array, Maps); TODO others

- [X] Snappy compression support

- [X] A unified DataWriter interface to write arbitrary rust types to avro data file.

- [ ] Decoding Header

- [ ] Decoding (Primitive Types)

- [ ] Decoding (Complex Types)

Getting Started

ravro is ~~available on crates.io~~. It is recommended to look there for the newest released version, as well as links to the newest builds of the docs.

Add the following dependency to your Cargo manifest...

```
[dependencies]
ravro = { git = "https://github.com/AtherEnergy/ravro" }
```

...and see the docs (yet to be published) for how to use it.

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

## Running tests

We currently use [avro-tools.jar](https://mvnrepository.com/artifact/org.apache.avro/avro-tools/1.8.2) to get written .avro data
output and assert against its output. This is until we implement decoding of .avro files.
So the tool needs to be downloaded for tests to run.
To install the tool issue: `./install_test_util.sh` and then run:

`cargo test`

## License

ravro is licensed under the terms of the MIT License or the Apache License 2.0, at your choosing.

Code of Conduct

Contribution to the `ravro` crate is organized under the terms of the Contributor Covenant, the maintainer of ravro, @creativcoder, promises to intervene to uphold that code of conduct.