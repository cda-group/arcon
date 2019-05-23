#![allow(non_camel_case_types)]

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[macro_use]
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
    pub code: String,
}

impl ArcSpec {
    pub fn load(path: &str) -> Result<ArcSpec, failure::Error> {
        let file = File::open(path).map_err(|e| SpecError { msg: e.to_string() })?;

        let spec: ArcSpec =
            serde_json::from_reader(file).map_err(|e| SpecError { msg: e.to_string() })?;
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Result;

    #[test]
    fn arc_spec_test() {
        let arc_spec_json = r#"
        {
            "id": "some_id",
            "target": "x86-64-unknown-linux-gnu",
            "code": "|x: i32, y: i32| x + y"
        }"#;

        let arc_spec: Result<ArcSpec> = serde_json::from_str(arc_spec_json);
        println!("{:?}", arc_spec);
        assert_eq!(arc_spec.is_ok(), true);
    }

}
