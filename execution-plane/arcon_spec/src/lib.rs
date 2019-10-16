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
pub struct ArconSpec {
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
    #[serde(default = "timestamp_extractor")]
    pub timestamp_extractor: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub id: u32,
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
    pub predecessor: u32,
    pub assigner: WindowAssigner,
    pub window_kind: WindowKind,
    pub window_function: WindowFunction,
    pub time_kind: TimeKind,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WindowKind {
    Keyed,
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WindowAssigner {
    Tumbling { length: u64 },
    Sliding { length: u64, slide: u64 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WindowFunction {
    pub input_type: Type,
    pub output_type: Type,
    pub builder_type: Type,
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
pub struct Source {
    pub source_type: Type,
    #[serde(default = "forward")]
    pub channel_strategy: ChannelStrategy,
    pub successors: Vec<ChannelKind>,
    pub kind: SourceKind,
    #[serde(default = "utf8")]
    pub format: Format,
    #[serde(default = "source_rate")]
    pub rate: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SourceKind {
    Socket {
        addr: String,
        #[serde(default = "udp")]
        kind: SocketKind,
    },
    LocalFile {
        path: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sink {
    pub sink_type: Type,
    pub predecessor: u32,
    pub kind: SinkKind,
    #[serde(default = "utf8")]
    pub format: Format,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SinkKind {
    Socket {
        addr: String,
        #[serde(default = "udp")]
        kind: SocketKind,
    },
    /// A debug Sink that simply prints out received elements
    Debug,
    LocalFile {
        path: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SocketKind {
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "udp")]
    Udp,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
    pub input_type: Type,
    pub output_type: Type,
    pub weld_code: String,
    pub predecessor: u32,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum CompileMode {
    #[serde(rename = "debug")]
    Debug,
    #[serde(rename = "release")]
    Release,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Format {
    CSV,
    JSON,
    UTF8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Type {
    Scalar(Scalar),
    Vector {
        elem_ty: Box<Type>,
    },
    Struct {
        id: String,
        key: Option<u32>,
        decoder: Option<String>,
        field_tys: Vec<Type>,
    },
    Appender {
        elem_ty: Box<Type>,
    },
    Merger {
        elem_ty: Box<Type>,
        op: MergeOp,
    },
    DictMerger {
        key_ty: Box<Type>,
        val_ty: Box<Type>,
        op: MergeOp,
    },
    GroupMerger {
        key_ty: Box<Type>,
        val_ty: Box<Type>,
    },
    VecMerger {
        elem_ty: Box<Type>,
        op: MergeOp,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Scalar {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Bool,
    Unit,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MergeOp {
    #[serde(rename = "+")]
    Plus,
    #[serde(rename = "*")]
    Mult,
    #[serde(rename = "max")]
    Max,
    #[serde(rename = "min")]
    Min,
}

/// Default Serde Implementations for `ArconSpec`

fn forward() -> ChannelStrategy {
    ChannelStrategy::Forward
}

fn release() -> CompileMode {
    CompileMode::Release
}

// NOTE: this currently assumes seconds.
//       will be reworked later on..
fn source_rate() -> u64 {
    10
}

fn timestamp_extractor() -> u32 {
    0
}

fn udp() -> SocketKind {
    SocketKind::Udp
}

fn utf8() -> Format {
    Format::UTF8
}

fn system_addr() -> String {
    String::from("127.0.0.1:2000")
}

fn parallelism() -> u32 {
    1
}

impl ArconSpec {
    pub fn load(path: &str) -> Result<ArconSpec, SpecError> {
        let file = File::open(path).map_err(|e| SpecError { msg: e.to_string() })?;
        serde_json::from_reader(file).map_err(|e| SpecError { msg: e.to_string() })
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<ArconSpec, SpecError> {
        serde_json::from_slice(bytes).map_err(|e| SpecError { msg: e.to_string() })
    }

    pub fn from_string(input: &str) -> Result<ArconSpec, SpecError> {
        serde_json::from_str(input).map_err(|e| SpecError { msg: e.to_string() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    static ARCON_SPEC_JSON: &str = r#"
        {
            "id": "some_id",
            "target": "x86-64-unknown-linux-gnu",
            "nodes": [
                {
                    "id": 1,
                    "parallelism": 1,
                    "kind": {
                        "Source": {
                            "source_type": {
                                "Scalar": "U32"
                            },
                            "successors": [
                                {
                                    "Local": {
                                        "id": "node_2"
                                    }
                                }
                            ],
                            "kind": {
                                "Socket": { "addr": "127.0.0.1:3002", "kind": "udp"}
                            }
                        }
                    }
                },
                {
                    "id": 2,
                    "parallelism": 1,
                    "kind": {
                        "Task": {
                            "input_type": {
                                "Scalar": "U32"
                            },
                            "output_type": {
                                "Scalar": "U32"
                            },
                            "weld_code": "|x: u32| x + u32(5)",
                            "channel_strategy": "Forward",
                            "predecessor": 1,
                            "successors": [
                                {
                                    "Local": {
                                        "id": "node3"
                                    }
                                }
                            ],
                            "kind": "Map"
                        }
                    }
                },
                {
                    "id": 3,
                    "parallelism": 1,
                    "kind": {
                        "Window": {
                            "predecessor": 2,
                            "successors" : [
                                {
                                    "Local": {
                                        "id": "node4"
                                    }
                                }
                            ],
                            "window_function": {
                                "input_type": {
                                    "Scalar": "U32"
                                },
                                "output_type": {
                                    "Scalar": "I64"
                                },
                                "builder_type": {
                                    "Appender": {
                                        "elem_ty": {
                                            "Scalar": "U32"
                                        }
                                    }
                                },
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
                    "id": 4,
                    "parallelism": 1,
                    "kind": {
                        "Sink": {
                            "sink_type": {
                                "Scalar": "I64"
                            },
                            "predecessor": 3,
                            "kind": "Debug"
                        }
                    }
                }
            ]
        }"#;

    #[test]
    fn arc_spec_string_test() {
        let arcon_spec: ArconSpec = serde_json::from_str(ARCON_SPEC_JSON).unwrap();
        assert_eq!(arcon_spec.id, "some_id");
    }

    #[test]
    fn arc_spec_bytes_test() {
        let raw = ARCON_SPEC_JSON.as_bytes();
        let arcon_spec = ArconSpec::from_bytes(raw);
        assert_eq!(arcon_spec.is_ok(), true);
    }

    #[test]
    fn arc_spec_file_test() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{}", ARCON_SPEC_JSON).unwrap();
        let arcon_spec = ArconSpec::load(file.path().to_str().unwrap());
        assert_eq!(arcon_spec.is_ok(), true);
    }
}
