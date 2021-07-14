use crate::{data::ArconType, error::source::SourceError};
use arcon_state::backend::serialization::protobuf;

pub trait SourceSchema: Send + Sync + Clone + 'static {
    type Data: ArconType;

    fn from_bytes(&self, bytes: &[u8]) -> Result<Self::Data, SourceError>;
}

#[cfg(feature = "serde_json")]
#[derive(Clone)]
pub struct JsonSchema<IN>
where
    IN: ArconType + ::serde::de::DeserializeOwned,
{
    _marker: std::marker::PhantomData<IN>,
}

#[cfg(feature = "serde_json")]
impl<IN> Default for JsonSchema<IN>
where
    IN: ArconType + ::serde::de::DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "serde_json")]
impl<IN> JsonSchema<IN>
where
    IN: ArconType + ::serde::de::DeserializeOwned,
{
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(feature = "serde_json")]
impl<IN> SourceSchema for JsonSchema<IN>
where
    IN: ArconType + ::serde::de::DeserializeOwned,
{
    type Data = IN;

    fn from_bytes(&self, bytes: &[u8]) -> Result<Self::Data, SourceError> {
        let s = std::str::from_utf8(bytes).map_err(|err| SourceError::Parse {
            msg: err.to_string(),
        })?;

        match serde_json::from_str(s) {
            Ok(data) => Ok(data),
            Err(err) => Err(SourceError::Schema {
                msg: err.to_string(),
            }),
        }
    }
}

#[derive(Clone)]
pub struct ProtoSchema<IN>
where
    IN: ArconType,
{
    _marker: std::marker::PhantomData<IN>,
}

impl<IN> Default for ProtoSchema<IN>
where
    IN: ArconType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<IN> ProtoSchema<IN>
where
    IN: ArconType,
{
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<IN> SourceSchema for ProtoSchema<IN>
where
    IN: ArconType,
{
    type Data = IN;

    fn from_bytes(&self, bytes: &[u8]) -> Result<Self::Data, SourceError> {
        match protobuf::deserialize(bytes) {
            Ok(data) => Ok(data),
            Err(err) => Err(SourceError::Schema {
                msg: err.to_string(),
            }),
        }
    }
}
