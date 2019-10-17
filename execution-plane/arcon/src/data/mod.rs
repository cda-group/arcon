use crate::error::ArconResult;
use crate::macros::*;
use crate::messages::protobuf::messages::ArconNetworkMessage;
use crate::messages::protobuf::*;
use crate::streaming::task::NodeID;
use arcon_messages::protobuf::ArconNetworkMessage_oneof_payload::*;
use serde::de::{DeserializeOwned, Deserializer, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::*;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;

/// Type that can be passed through the Arcon runtime
pub trait ArconType: Sync + Send + Clone + Copy + Debug + Serialize + DeserializeOwned {}

/// Wrapper for unifying passing of Elements and other stream messages (watermarks)
#[derive(Clone, Debug, Copy)]
pub enum ArconEvent<A: 'static + ArconType> {
    Element(ArconElement<A>),
    Watermark(Watermark),
    Epoch(Epoch),
}

#[derive(Clone, Debug)]
pub struct ArconMessage<A: 'static + ArconType> {
    pub event: ArconEvent<A>,
    pub sender: NodeID,
}

// Convenience methods, "message factories"
impl<A: 'static + ArconType> ArconMessage<A> {
    pub fn watermark(timestamp: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            event: ArconEvent::<A>::Watermark(Watermark { timestamp }),
            sender,
        }
    }
    pub fn epoch(epoch: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            event: ArconEvent::<A>::Epoch(Epoch { epoch }),
            sender,
        }
    }
    pub fn element(data: A, timestamp: Option<u64>, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            event: ArconEvent::Element(ArconElement { data, timestamp }),
            sender,
        }
    }
}

/// Watermark
#[derive(Clone, Debug, Copy)]
pub struct Watermark {
    pub timestamp: u64,
}

impl Watermark {
    pub fn new(timestamp: u64) -> Self {
        Watermark { timestamp }
    }
}

/// Epoch
#[derive(Clone, Debug, Copy)]
pub struct Epoch {
    pub epoch: u64,
}

impl Epoch {
    pub fn new(epoch: u64) -> Self {
        Epoch { epoch }
    }
}

impl<A: 'static + ArconType> ArconMessage<A> {
    pub fn to_remote(&self) -> ArconResult<ArconNetworkMessage> {
        match self.event {
            ArconEvent::Element(e) => {
                let serialised_event: Vec<u8> = bincode::serialize(&e.data).map_err(|e| {
                    arcon_err_kind!("Failed to serialise event with err {}", e.to_string())
                })?;

                let timestamp = e.timestamp.unwrap_or(0);
                // TODO: Add Sender info to the ArconNetworkMessage
                Ok(create_element(serialised_event, timestamp))
            }
            ArconEvent::Watermark(w) => {
                let mut msg = ArconNetworkMessage::new();
                let mut msg_watermark = messages::Watermark::new();
                msg_watermark.set_timestamp(w.timestamp);
                msg.set_watermark(msg_watermark);
                msg.set_sender(self.sender.id);
                Ok(msg)
            }
            ArconEvent::Epoch(e) => {
                let mut msg = ArconNetworkMessage::new();
                let mut msg_checkpoint = messages::Checkpoint::new();
                msg_checkpoint.set_epoch(e.epoch);
                msg.set_checkpoint(msg_checkpoint);
                msg.set_sender(self.sender.id);
                Ok(msg)
            }
        }
    }

    pub fn from_remote(message: ArconNetworkMessage) -> ArconResult<Self> {
        let sender = message.get_sender();
        let payload = message.payload.unwrap();
        match payload {
            element(e) => {
                let data: A = bincode::deserialize(e.get_data()).map_err(|e| {
                    arcon_err_kind!("Failed to deserialise event with err {}", e.to_string())
                })?;
                Ok(ArconMessage::<A>::element(
                    data,
                    Some(e.get_timestamp()),
                    sender.into(),
                ))
            }
            watermark(w) => Ok(ArconMessage::watermark(w.timestamp, sender.into())),
            checkpoint(c) => Ok(ArconMessage::epoch(c.epoch, sender.into())),
        }
    }
}

/// A stream element that contains an `ArconType` and an optional timestamp
#[derive(Clone, Debug, Copy)]
pub struct ArconElement<A>
where
    A: 'static + ArconType,
{
    pub data: A,
    pub timestamp: Option<u64>,
}

impl<A> ArconElement<A>
where
    A: 'static + ArconType,
{
    pub fn new(data: A) -> Self {
        ArconElement {
            data,
            timestamp: None,
        }
    }

    pub fn with_timestamp(data: A, ts: u64) -> Self {
        ArconElement {
            data,
            timestamp: Some(ts),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Pair<K, V> {
    pub ele1: K,
    pub ele2: V,
}

unsafe impl<K, V> Send for Pair<K, V> {}
unsafe impl<K, V> Sync for Pair<K, V> {}

/// `ArconVec` is essentially the same as a `WeldVec`,
/// however, it is reintroduced here to add extra features
/// and to easier integrate with serialisation/deserialisation
#[repr(C)]
#[derive(Copy)]
pub struct ArconVec<A: ArconType> {
    pub ptr: *const A,
    pub len: i64,
}

impl<A: ArconType> Clone for ArconVec<A> {
    fn clone(&self) -> ArconVec<A> {
        let boxed_vec = Box::new(self.to_vec());
        let arcon_vec: ArconVec<A> = ArconVec::new(*boxed_vec);
        arcon_vec
    }
}

impl<A: ArconType> ArconVec<A> {
    /// Create a new `ArconVec`
    ///
    /// ```rust
    /// use arcon::data::ArconVec;
    ///
    /// let mut vec = vec![1,2,3,4,5];
    /// let arcon_vec = ArconVec::new(vec);
    /// assert_eq!(arcon_vec.len, 5);
    /// ```
    pub fn new(mut vec: Vec<A>) -> Self {
        vec.shrink_to_fit();

        let arcon_vec = ArconVec {
            ptr: vec.as_ptr() as *const A,
            len: vec.len() as i64,
        };

        // NOTE: Explicitly do not destruct the Vec to which ArconVec
        //       points to. Deallocation will be handled later on
        std::mem::forget(vec);

        arcon_vec
    }

    /// Turns `ArconVec` into a normal Rust Vec
    ///
    /// ```rust
    /// use arcon::data::ArconVec;
    ///
    /// let mut vec = vec![1,2,3,4,5];
    /// let arcon_vec = ArconVec::new(vec.clone());
    /// let rust_vec = arcon_vec.to_vec();
    /// assert_eq!(vec, rust_vec);
    /// ```
    pub fn to_vec(&self) -> Vec<A> {
        let boxed: Box<[A]> = unsafe {
            Vec::from_raw_parts(self.ptr as *mut A, self.len as usize, self.len as usize)
                .into_boxed_slice()
        };
        let vec = boxed.clone().into_vec();
        std::mem::forget(boxed);
        vec
    }
}

/// Access `ArconVec` as slice
///
/// ```rust
/// use arcon::data::ArconVec;
///
/// let mut vec = vec![1,2,3,4,5];
/// let arcon_vec = ArconVec::new(vec);
/// assert_eq!(arcon_vec[0], 1);
/// ```
impl<A: ArconType> Deref for ArconVec<A> {
    type Target = [A];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

impl<A: ArconType> Display for ArconVec<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vec: Vec<A> = self.to_vec();
        write!(f, "{:?}", vec)
    }
}

impl<A: ArconType> Debug for ArconVec<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vec: Vec<A> = self.to_vec();
        write!(f, "{:?}", vec)
    }
}

unsafe impl<A: ArconType> Sync for ArconVec<A> {}
unsafe impl<A: ArconType> Send for ArconVec<A> {}

/// `Serde` visitor for deserialisation of `ArconVec`
struct ArconVecVisitor<A: ArconType> {
    marker: PhantomData<fn() -> ArconVec<A>>,
}

impl<A: ArconType> ArconVecVisitor<A> {
    fn new() -> Self {
        ArconVecVisitor {
            marker: PhantomData,
        }
    }
}

/// Defines how the `ArconVecVisitor` is supposed to
/// traverse and deserialize the `ArconVec`
impl<'de, A> Visitor<'de> for ArconVecVisitor<A>
where
    A: ArconType,
{
    type Value = ArconVec<A>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an ArconVec")
    }

    fn visit_seq<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: SeqAccess<'de>,
    {
        let mut vec: Vec<A> = Vec::with_capacity(access.size_hint().unwrap_or(0));

        while let Some(value) = access.next_element::<A>()? {
            vec.push(value);
        }

        Ok(ArconVec::new(vec))
    }
}

/// Implements the `Deserialize` trait for `ArconVec`
impl<'de, A> Deserialize<'de> for ArconVec<A>
where
    A: ArconType,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArconVecVisitor::new())
    }
}

impl<A: ArconType> Serialize for ArconVec<A>
where
    A: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut vec: Vec<A> = unsafe {
            Vec::from_raw_parts(self.ptr as *mut A, self.len as usize, self.len as usize)
        };

        let mut seq = serializer.serialize_seq(Some(self.len as usize))?;
        for e in vec.iter_mut() {
            seq.serialize_element(&e)?;
        }

        // We do not want to call the destructor to the underlying pointer
        std::mem::forget(vec);

        seq.end()
    }
}

/// Float wrappers for Arcon
///
/// The `Hash` impl rounds the floats down to an integer
/// and then hashes it. Might want to change this later on..
#[arcon]
pub struct ArconF32(f32);

impl Hash for ArconF32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: u64 = self.0.trunc() as u64;
        s.hash(state);
    }
}

impl std::str::FromStr for ArconF32 {
    type Err = ::std::num::ParseFloatError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let f: f32 = s.parse::<f32>()?;
        Ok(ArconF32(f))
    }
}

impl Deref for ArconF32 {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[arcon]
pub struct ArconF64(f64);

impl Hash for ArconF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: u64 = self.0.trunc() as u64;
        s.hash(state);
    }
}

impl std::str::FromStr for ArconF64 {
    type Err = ::std::num::ParseFloatError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let f: f64 = s.parse::<f64>()?;
        Ok(ArconF64(f))
    }
}

impl Deref for ArconF64 {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Implements `ArconType` for all data types that are supported
impl ArconType for u8 {}
impl ArconType for u16 {}
impl ArconType for u32 {}
impl ArconType for u64 {}
impl ArconType for i8 {}
impl ArconType for i16 {}
impl ArconType for i32 {}
impl ArconType for i64 {}
impl ArconType for f32 {}
impl ArconType for f64 {}
impl<A: ArconType> ArconType for ArconVec<A> {}
impl<K: ArconType, V: ArconType> ArconType for Pair<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::weld::module::*;
    use weld::WeldContext;

    #[test]
    fn basic_arcon_collection_test() {
        let code = String::from("|x: vec[i32]| map(x, |a: i32| a + i32(5))");
        let module = Module::new(code).unwrap();
        let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
        let input: Vec<i32> = vec![1, 2, 3, 4, 5];
        let vec = ArconVec::new(input);
        let result: ModuleRun<ArconVec<i32>> = module.run(&vec, ctx).unwrap();
        let data = result.0;
        assert_eq!(data.len, 5);
        let expected: Vec<i32> = vec![6, 7, 8, 9, 10];
        let result_collection = data.to_vec();
        assert_eq!(expected, result_collection);
    }

    #[test]
    fn nested_arcon_collection_test() {
        let code = String::from("|x: vec[vec[i32]]| map(x, |a: vec[i32]| len(a))");
        let module = Module::new(code).unwrap();
        let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
        let v1 = vec![1, 2, 3, 4, 5];
        let v1_arcon = ArconVec::new(v1);
        let v2 = vec![6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let v2_arcon = ArconVec::new(v2);
        let input = vec![v1_arcon, v2_arcon];
        let result: ModuleRun<ArconVec<i64>> = module.run(&input, ctx).unwrap();
        let data = result.0;
        assert_eq!(data.len, 2);
        let result_collection = data.to_vec();
        let expected = vec![5, 10];
        assert_eq!(expected, result_collection);
    }

    #[test]
    fn arcon_vec_serder_test() {
        let v1: Vec<i32> = vec![1, 2, 3, 4, 5];
        let v1_arcon = ArconVec::new(v1);
        let bytes = bincode::serialize(&v1_arcon).unwrap();

        {
            let deser_vec: ArconVec<i32> = bincode::deserialize(&bytes).unwrap();
            assert_eq!(deser_vec.len, 5);
            for i in 0..5 {
                assert_eq!((i + 1) as i32, deser_vec[i]);
            }
            let code = String::from("|x: vec[i32]| map(x, |a: i32| a + i32(5))");
            let module = Module::new(code).unwrap();
            let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
            let result: ModuleRun<ArconVec<i32>> = module.run(&deser_vec, ctx).unwrap();
            let data = result.0;
            for i in 0..5 {
                assert_eq!((i + 6) as i32, data[i]);
            }

            let _ = bincode::serialize(&data).unwrap();
        }
    }
}
