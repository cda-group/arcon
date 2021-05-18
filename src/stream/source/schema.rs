use crate::data::ArconType;
use arcon_state::backend::serialization::protobuf;

pub trait SourceSchema: Send + Sync + Clone + 'static {
    type Data: ArconType;

    fn from_bytes(&self, bytes: &[u8]) -> Option<Self::Data>;
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

    fn from_bytes(&self, bytes: &[u8]) -> Option<Self::Data> {
        match serde_json::from_str(std::str::from_utf8(&bytes).unwrap()) {
            Ok(data) => Some(data),
            Err(_) => None,
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

    fn from_bytes(&self, bytes: &[u8]) -> Option<Self::Data> {
        match protobuf::deserialize(bytes) {
            Ok(data) => Some(data),
            Err(_) => None,
        }
    }
}
