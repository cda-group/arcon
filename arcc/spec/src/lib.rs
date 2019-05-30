#![allow(non_camel_case_types)]

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

extern crate failure;

use failure::Fail;
use std::fs::File;

#[derive(Debug, Fail)]
#[fail(display = "Loading spec err: `{}`", msg)]
pub struct SpecError {
    msg: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArcSpec {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ir: String,
}

impl ArcSpec {
    pub fn load(path: &str) -> Result<ArcSpec, failure::Error> {
        let file = File::open(path).map_err(|e| SpecError { msg: e.to_string() })?;

        let spec: ArcSpec =
            serde_json::from_reader(file).map_err(|e| SpecError { msg: e.to_string() })?;
        Ok(spec)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<ArcSpec, failure::Error> {
        let spec: ArcSpec =
            serde_json::from_slice(bytes).map_err(|e| SpecError { msg: e.to_string() })?;
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Result;
    use tempfile::NamedTempFile;
    use std::io::{self, Write};

    static ARC_SPEC_JSON: &str = r#"
        {
            "id": "some_id",
            "target": "x86-64-unknown-linux-gnu",
            "ir": "|x: i32, y: i32| x + y"
        }"#;

    #[test]
    fn arc_spec_string_test() {
        let arc_spec: Result<ArcSpec> = serde_json::from_str(ARC_SPEC_JSON);
        assert_eq!(arc_spec.is_ok(), true);
    }

    #[test]
    fn arc_spec_bytes_test() {
        let raw = ARC_SPEC_JSON.as_bytes();
        let arc_spec = ArcSpec::from_bytes(raw);
        assert_eq!(arc_spec.is_ok(), true);
    }

    #[test]
    fn arc_spec_file_test() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{}", ARC_SPEC_JSON).unwrap();
        let arc_spec = ArcSpec::load(file.path().to_str().unwrap());
        assert_eq!(arc_spec.is_ok(), true);
    }
}
