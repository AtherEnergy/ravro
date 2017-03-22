//! Just a bunch of macros to be used

/// Helper macro to write a File data block
macro_rules! commit_block {
	($scm:ident, $sync:ident, $buf:ident) => ({
		let data_count = Schema::Long(1);
		// write Data block count
		data_count.encode(&mut $buf);
		// create a buffer
		let mut data_buf = vec![];
		// encode the schema
		let _ = $scm.encode(&mut data_buf)?;
		// encode count of serialized buffer
		let _ = Schema::Long(data_buf.len() as i64).encode(&mut $buf);
		// write everything to our master buffer
		$buf.write_all(&mut data_buf);
		// finally write the sync marker to master buffer
		$sync.encode(&mut $buf);
	})
}

/// Macro to create error structs
/// TODO maybe replace them with error chain
macro_rules! err_structs {
	($($x:ident),+) => (
		$(#[derive(Debug, PartialEq)]pub struct $x;)+
	)
}