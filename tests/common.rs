
extern crate ravro;
use std::process::Command;
use std::str;

pub fn get_java_tool_output(encoded: &str) -> Result<String, ()> {
    let a = Command::new("java")
            .args(&["-jar", "avro-tools-1.8.1.jar", "tojson", encoded])
            .output()
            .expect("failed to execute process");
    str::from_utf8(&a.stdout).map_err(|_| ()).map(|s| s.to_string())
}
