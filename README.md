
## ravro - A pure rust implementation of the Avro Spec. [WIP]

### NOTE:[Deprecated. This is undergoing a major rewrite with Serde support]

[![Build Status](https://travis-ci.org/Ather-Energy/ravro.svg?branch=master)](https://travis-ci.org/Ather-Energy/ravro)

Implementation Status:

- [X] Binary Encoding
- [ ] Json Encoding
- [X] Writer interface 
    * [Primitives](https://avro.apache.org/docs/1.8.1/spec.html#schema_primitive) - null, boolean, int, long, float, byte, double, string
    * [Complex](https://avro.apache.org/docs/1.8.1/spec.html#schema_complex) - Records (support recursive types in its fields is in the works), Enums, Arrays, Maps. TODO(unions, fixed)

- [X] Supported codecs: `null`, `deflate`, `snappy` are all supported.
- [ ] Reader interface (Implementation is on hold until Writer API and performance stability.)
- [ ] RPC related implementations.


Getting Started

Add the following dependency to your Cargo manifest...

```
[dependencies]
ravro = { git = "https://github.com/AtherEnergy/ravro" }
```

### Examples

```rust
// test example taken from `/tests/primitive.rs`
let schema_file = "tests/schemas/bool_schema.avsc";
let bool_schema = AvroSchema::from_file(schema_file).unwrap();
let datafile_name = "tests/encoded/bool_encoded.avro";
let mut data_writer = DataWriter::new(bool_schema, Codec::Deflate).unwrap();
let _ = data_writer.write(true);
let _ = data_writer.write(false);
let avro_encoded_buffer: Vec<u8> = data_writer.take_datafile().unwrap();
// `avro_encoded_buffer` can now be write to file or streamed over RPC

```

## Running tests

We currently use [avro-tools.jar](https://mvnrepository.com/artifact/org.apache.avro/avro-tools/1.8.2) to get `.avro` data
output and assert against it. This is until we implement decoding of `.avro` files.
So the tool needs to be downloaded for tests to run.
To install the tool issue: `./install_test_util.sh` and then run:

`cargo test` to run the test suite.

## License

`ravro` is licensed under the terms of the MIT License or the Apache License 2.0, at your choosing.

Code of Conduct

Contribution to the `ravro` crate is organized under the terms of the Contributor Covenant, the maintainer of ravro, @creativcoder, promises to intervene to uphold that code of conduct.
