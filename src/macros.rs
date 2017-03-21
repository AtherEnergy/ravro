//! Just a bunch of macros to be used

/// This macro helps different avro types to be written as data blocks
/// TODO this may be deprecated
macro_rules! write_block {
	($self_:ident, $scm:ident, $writer:ident, $sync:ident) => ({
		// The count of data rows. TODO parametrize this on the macro
		let data_count = Schema::Long(1);
		// write data rows count
		data_count.encode(&mut $self_.$writer);
		// create in memory buffer for encoding into
		let mut buf = Vec::new();
		// encode the type
		let _ = $scm.encode(&mut buf)?;
		// write the serialized data length in the writer
		let _ = Schema::Long(buf.len() as i64).encode(&mut $self_.$writer);
		// Finally write the buf to the writer
		$self_.$writer.write_all(&mut buf);
		// Then write the sync marker
		$sync.encode(&mut $self_.$writer);
	})
}

/// helper to compress a buffer.
macro_rules! commit_block {
	($scm:ident, $sync:ident, $buf:ident) => ({
		let data_count = Schema::Long(1);
		data_count.encode(&mut $buf);
		let mut data_buf = vec![];
		let _ = $scm.encode(&mut data_buf)?;
		let _ = Schema::Long(data_buf.len() as i64).encode(&mut $buf);
		$buf.write_all(&mut data_buf);
		$sync.encode(&mut $buf);
	})
}

macro_rules! err_structs {
	($($x:ident),+) => (
		$(#[derive(Debug, PartialEq)]pub struct $x;)+
	)
}