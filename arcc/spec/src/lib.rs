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
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub id: u32,
    pub node_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weld_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_type: Option<String>,
    #[serde(default = "parallelism")]
    pub parallelism: u32,
    #[serde(default = "forward")]
    pub channel_strategy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_channels: Option<Vec<Channel>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_channels: Option<Vec<Channel>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Channel {
    pub id: u32,
    /// Kompact Port or Actorpath
    pub channel_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_path: Option<ActorPath>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActorPath {
    pub id: String,
    /// 127.0.0.1:2000
    pub addr: String,
}

/// Default Serde Implementations for `ArcSpec`
fn forward() -> String {
    String::from("Forward")
}

fn parallelism() -> u32 {
    1
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
    use std::io::Write;
    use tempfile::NamedTempFile;

    static ARC_SPEC_JSON: &str = r#"
        {
            "id": "some_id",
            "target": "x86-64-unknown-linux-gnu",
            "nodes": [
                {
                    "id": 1,
                    "node_type": "Source",
                    "input_type": "i64",
                    "output_type": "i64",
                    "parallelism": 1,
                    "channel_strategy": "forward",
                    "output_channels": [
                        {
                            "id": 2,
                            "channel_type": "port"
                        }
                    ]
                },
                {
                    "id": 2,
                    "node_type": "StreamTask",
                    "weld_code": "|x: i64| x + 5",
                    "input_type": "i64",
                    "output_type": "i64",
                    "parallelism": 1,
                    "channel_strategy": "forward",
                    "input_channels": [
                        {
                            "id": 1,
                            "channel_type": "port"
                        }
                    ],
                    "output_channels": [
                        {
                            "id": 3,
                            "channel_type": "port"
                        }
                    ]
                },
                {
                    "id": 3,
                    "node_type": "Sink",
                    "input_type": "i64",
                    "parallelism": 1,
                    "channel_strategy": "forward",
                    "input_channels": [
                        {
                            "id": 2,
                            "channel_type": "port"
                        }
                    ]
                }
            ]
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
