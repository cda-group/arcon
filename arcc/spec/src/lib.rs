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
    #[serde(default = "release")]
    pub mode: CompileMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target: String,
    #[serde(default = "system_addr")]
    pub system_addr: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub id: String,
    #[serde(default = "parallelism")]
    pub parallelism: u32,
    pub kind: NodeKind,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodeKind {
    Source(Source),
    Sink(Sink),
    Task(Task),
    Window(Window),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Window {
    #[serde(default = "forward")]
    pub channel_strategy: ChannelStrategy,
    pub successors: Vec<ChannelKind>,
    pub predecessor: String,
    pub assigner: WindowAssigner,
    pub window_kind: WindowKind,
    pub window_function: WindowFunction,
    pub time_kind: TimeKind,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WindowKind {
    Keyed { kind: KeyKind },
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WindowAssigner {
    Tumbling { length: u64 },
    Sliding { length: u64, slide: u64 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WindowFunction {
    pub input_type: String,
    pub output_type: String,
    pub builder: String,
    pub udf: String,
    pub materialiser: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TimeKind {
    Event { slack: u64 },
    Processing,
    Ingestion,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KeyKind {
    Struct { id: String },
    Primitive,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Source {
    pub source_type: String,
    #[serde(default = "forward")]
    pub channel_strategy: ChannelStrategy,
    pub successors: Vec<ChannelKind>,
    pub kind: SourceKind,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SourceKind {
    Socket { host: String, port: u32 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sink {
    pub sink_type: String,
    pub predecessor: String,
    pub kind: SinkKind,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SinkKind {
    Socket {
        host: String,
        port: u32,
    },
    /// A debug Sink that simply prints out received elements
    Debug,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
    pub input_type: String,
    pub output_type: String,
    pub weld_code: String,
    pub predecessor: String,
    #[serde(default = "forward")]
    pub channel_strategy: ChannelStrategy,
    pub successors: Vec<ChannelKind>,
    pub kind: TaskKind,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TaskKind {
    FlatMap,
    Filter,
    Map,
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
pub enum ChannelKind {
    Local { id: String },
    Remote { id: String, addr: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CompileMode {
    Debug,
    Release,
}

/// Default Serde Implementations for `ArcSpec`

fn forward() -> ChannelStrategy {
    ChannelStrategy::Forward
}

fn release() -> CompileMode {
    CompileMode::Release
}

fn system_addr() -> String {
    String::from("127.0.0.1:2000")
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
                    "parallelism": 1,
                    "kind": {
                        "Source": {
                            "source_type": "u32",
                            "successors": [
                                {
                                    "Local": {
                                        "id": "node_2"
                                    }
                                }
                            ],
                            "kind": {
                                "Socket": { "host": "localhost", "port": 1337}
                            }
                        }
                    }
                },
                {
                    "id": "node_2",
                    "parallelism": 1,
                    "kind": {
                        "Task": {
                            "input_type": "u32",
                            "output_type": "u32",
                            "weld_code": "|x: u32| x + u32(5)",
                            "channel_strategy": "Forward",
                            "predecessor": "node_1",
                            "successors": [
                                {
                                    "Local": {
                                        "id": "node_3"
                                    }
                                }
                            ],
                            "kind": "Map"
                        }
                    }
                },
                {
                    "id": "node_3",
                    "parallelism": 1,
                    "kind": {
                        "Window": {
                            "predecessor": "node_2",
                            "successors" : [
                                {
                                    "Local": {
                                        "id": "node_4"
                                    }
                                }
                            ],
                            "window_function": {
                                "input_type": "u32",
                                "output_type": "i64",
                                "builder": "|| appender[u32]",
                                "udf": "|x: u32, y: appender[u32]| merge(y, x)",
                                "materialiser": "|y: appender[u32]| len(result(y))"
                            },
                            "window_kind": "All",
                            "time_kind": "Processing",
                            "assigner": {
                                "Tumbling" : {
                                    "length": 2000
                                }
                            }
                        }
                    }
                },
                {
                    "id": "node_4",
                    "parallelism": 1,
                    "kind": {
                        "Sink": {
                            "sink_type": "i64",
                            "predecessor": "node_3",
                            "kind": "Debug"
                        }
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
