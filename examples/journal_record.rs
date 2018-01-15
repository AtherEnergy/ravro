
extern crate ravro;
extern crate rand;
extern crate loggerv;

use ravro::{AvroWriter, Codec};
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
    journal_map.insert("__CURSOR","s=da3cd5105928482eb363538512851454;i=938;b=5c5d886fc74a447d87b6ace9ffa2a1d0;m=39e294170;t=562cd094f54e8;x=f5a11d40db22a4c0");
    journal_map.insert("__REALTIME_TIMESTAMP","1516007647565032");
    journal_map.insert("__MONOTONIC_TIMESTAMP", "15538405744");
    journal_map.insert("_BOOT_ID", "5c5d886fc74a447d87b6ace9ffa2a1d0");
    journal_map.insert("PRIORITY", "6");
    journal_map.insert("SYSLOG_FACILITY", "3");
    journal_map.insert("_TRANSPORT", "journal");
    journal_map.insert("_CAP_EFFECTIVE", "0");
    journal_map.insert("_MACHINE_ID", "80dc3b4810634e679da9c0a1f71583ea");
    journal_map.insert("_HOSTNAME", "autobot");
    journal_map.insert("_SYSTEMD_SLICE", "system.slice");
    journal_map.insert("CODE_FILE", "../src/resolve/resolved-link.c");
    journal_map.insert("CODE_LINE", "593");
    journal_map.insert("CODE_FUNCTION", "link_set_dns_server");
    journal_map.insert("SYSLOG_IDENTIFIER", "systemd-resolved");
    journal_map.insert("MESSAGE", "Switching to DNS server 8.8.8.8 for interface wlp3s0.");
    journal_map.insert("_PID", "1212");
    journal_map.insert("_UID", "102");
    journal_map.insert("_GID", "104");
    journal_map.insert("_COMM", "systemd-resolve");
    journal_map.insert("_EXE", "/lib/systemd/systemd-resolved");
    journal_map.insert("_CMDLINE", "/lib/systemd/systemd-resolved");
    journal_map.insert("_SYSTEMD_CGROUP", "/system.slice/systemd-resolved.service");
    journal_map.insert("_SYSTEMD_UNIT", "systemd-resolved.service");
    journal_map.insert("_SYSTEMD_INVOCATION_ID", "73fe80fed7644905a740d3e6ed703a40");
    journal_map.insert("_SOURCE_REALTIME_TIMESTAMP", "1516007647563129");
	let datafile_name = "tests/encoded/journal_record.avro";
	let mut data_writer = AvroWriter::from_str(r#"{"type": "array", "items": {"type": "map", "values": "string"}}"#).unwrap();
    data_writer.set_codec(Codec::Snappy);
    let mut v = vec![];
    for _ in  0..100_000 {
        v.push(journal_map.clone());
    }
    let _ = data_writer.write(v);
    let _ = data_writer.commit_block();
    data_writer.flush_to_disk(datafile_name);
    // println!("{}", get_java_tool_output(datafile_name).unwrap());
}
