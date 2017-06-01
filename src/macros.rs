//! Contains helpful macros

/// Macro to create error structs
/// TODO maybe replace them with error chain
macro_rules! err_structs {
	($($x:ident),+) => (
		$(#[derive(Debug, PartialEq)]pub struct $x;)+
	)
}
