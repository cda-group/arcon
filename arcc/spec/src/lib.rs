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
    pub id: String,
    pub node_type: NodeType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weld_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_type: Option<String>,
    #[serde(default = "parallelism")]
    pub parallelism: u32,
    #[serde(default = "forward")]
    pub channel_strategy: ChannelStrategy,
    pub predecessor: Option<Channel>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodeType {
    Source(SourceType),
    Sink(SinkType),
    StreamTask,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SourceType {
    Socket { host: String, port: u32 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SinkType {
    Socket {
        host: String,
        port: u32,
    },
    /// A debug Sink that simply prints out received elements
    Debug,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ChannelStrategy {
    Forward,
    Broadcast,
    Shuffle,
    RoundRobin,
    KeyBy,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Channel {
    pub id: String,
    pub channel_type: ChannelType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ChannelType {
    Local,
    Remote { id: String, addr: String },
}

/// Default Serde Implementations for `ArcSpec`

fn forward() -> ChannelStrategy {
    ChannelStrategy::Forward
}

fn parallelism() -> u32 {
    1
}

impl ArcSpec {
    pub fn load(path: &str) -> Result<ArcSpec, SpecError> {
        let file = File::open(path).map_err(|e| SpecError { msg: e.to_string() })?;
        serde_json::from_reader(file).map_err(|e| SpecError { msg: e.to_string() })
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<ArcSpec, SpecError> {
        serde_json::from_slice(bytes).map_err(|e| SpecError { msg: e.to_string() })
    }

    pub fn from_string(input: &str) -> Result<ArcSpec, SpecError> {
        serde_json::from_str(input).map_err(|e| SpecError { msg: e.to_string() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    static ARC_SPEC_JSON: &str = r#"
        {
            "id": "some_id",
            "target": "x86-64-unknown-linux-gnu",
            "nodes": [
                {
                    "id": "node_1",
                    "node_type": {
                        "Source": {
                            "Socket": { "host": "localhost", "port": 1337}
                        }
                    },
                    "input_type": "i64",
                    "output_type": "i64",
                    "parallelism": 1,
                    "channel_strategy": "Forward"
                },
                {
                    "id": "node_2",
                    "node_type": "StreamTask",
                    "weld_code": "|x: i64| x + 5",
                    "input_type": "i64",
                    "output_type": "i64",
                    "parallelism": 1,
                    "channel_strategy": "Forward",
                    "predecessor": {
                            "id": "node_1",
                            "channel_type": "Local"
                    }
                },
                {
                    "id": "node_3",
                    "node_type": {
                        "Sink": "Debug"
                    },
                    "input_type": "i64",
                    "parallelism": 1,
                    "channel_strategy": "Forward",
                    "predecessor": {
                            "id": "node_2",
                            "channel_type": "Local"
                    }
                }
            ]
        }"#;

    #[test]
    fn arc_spec_string_test() {
        let arc_spec: ArcSpec = serde_json::from_str(ARC_SPEC_JSON).unwrap();
        assert_eq!(arc_spec.id, "some_id");
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
