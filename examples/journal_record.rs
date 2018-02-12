
extern crate ravro;
extern crate rand;
extern crate loggerv;

use ravro::{AvroWriter, Codec};
use ravro::complex::RecordSchema;
use std::collections::BTreeMap;

use std::str;
use std::process::Command;

pub fn get_java_tool_output(encoded: &str) -> Result<String, ()> {
    let a = Command::new("java")
            .args(&["-jar", "avro-tools-1.8.2.jar", "tojson", encoded])
            .output()
            .expect("failed to execute process");
    str::from_utf8(&a.stdout).map_err(|_| ()).map(|s| s.to_string())
}

fn main() {
    loggerv::init_with_verbosity(4).unwrap();
    let mut journal_map = BTreeMap::new();
    journal_map.insert("__CURSOR".to_string(),"s=da3cd5105928482eb363538512851454;i=938;b=5c5d886fc74a447d87b6ace9ffa2a1d0;m=39e294170;t=562cd094f54e8;x=f5a11d40db22a4c0".to_string());
    journal_map.insert("__REALTIME_TIMESTAMP".to_string(),"1516007647565032".to_string());
    journal_map.insert("__MONOTONIC_TIMESTAMP".to_string(), "15538405744".to_string());
    journal_map.insert("_BOOT_ID".to_string(), "5c5d886fc74a447d87b6ace9ffa2a1d0".to_string());
    journal_map.insert("PRIORITY".to_string(), "6".to_string());
    journal_map.insert("SYSLOG_FACILITY".to_string(), "3".to_string());
    journal_map.insert("_TRANSPORT".to_string(), "journal".to_string());
    journal_map.insert("_CAP_EFFECTIVE".to_string(), "0".to_string());
    journal_map.insert("_MACHINE_ID".to_string(), "80dc3b4810634e679da9c0a1f71583ea".to_string());
    journal_map.insert("_HOSTNAME".to_string(), "autobot".to_string());
    journal_map.insert("_SYSTEMD_SLICE".to_string(), "system.slice".to_string());
    journal_map.insert("CODE_FILE".to_string(), "../src/resolve/resolved-link.c".to_string());
    journal_map.insert("CODE_LINE".to_string(), "593".to_string());
    journal_map.insert("CODE_FUNCTION".to_string(), "link_set_dns_server".to_string());
    journal_map.insert("SYSLOG_IDENTIFIER".to_string(), "systemd-resolved".to_string());
    journal_map.insert("MESSAGE".to_string(), "Switching to DNS server 8.8.8.8 for interface wlp3s0.".to_string());
    journal_map.insert("_PID".to_string(), "1212".to_string());
    journal_map.insert("_UID".to_string(), "102".to_string());
    journal_map.insert("_GID".to_string(), "104".to_string());
    journal_map.insert("_COMM".to_string(), "systemd-resolve".to_string());
    journal_map.insert("_EXE".to_string(), "/lib/systemd/systemd-resolved".to_string());
    journal_map.insert("_CMDLINE".to_string(), "/lib/systemd/systemd-resolved".to_string());
    journal_map.insert("_SYSTEMD_CGROUP".to_string(), "/system.slice/systemd-resolved.service".to_string());
    journal_map.insert("_SYSTEMD_UNIT".to_string(), "systemd-resolved.service".to_string());
    journal_map.insert("_SYSTEMD_INVOCATION_ID".to_string(), "73fe80fed7644905a740d3e6ed703a40".to_string());
    journal_map.insert("_SOURCE_REALTIME_TIMESTAMP".to_string(), "1516007647563129".to_string());

	let datafile_name = "tests/encoded/journal_record.avro";
    let schema_path = "examples/journal_schema.avsc";
    let mut data_writer = AvroWriter::from_schema(schema_path).unwrap();
    data_writer.set_codec(Codec::Deflate);
    let mut data_writer = data_writer.build().unwrap();
    for _ in  0..1000 {
        let record = RecordSchema::from_map("dashbord_stats", None, journal_map.clone());
        let _ = data_writer.write(record);
    }
    let _ = data_writer.commit_block();
    data_writer.flush_to_disk(datafile_name);
    println!("{}", get_java_tool_output(datafile_name).unwrap());
}
