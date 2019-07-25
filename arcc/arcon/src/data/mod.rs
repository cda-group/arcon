use crate::error::ArconResult;
use crate::messages::protobuf::messages::StreamTaskMessage;
use crate::messages::protobuf::messages::StreamTaskMessage_oneof_payload::*;
use crate::messages::protobuf::*;
use serde::de::{DeserializeOwned, Deserializer, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::*;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;

/// Type that can be passed through the Arcon runtime
pub trait ArconType: Sync + Send + Clone + Copy + Debug + Serialize + DeserializeOwned {}

/// Wrapper for unifying passing of Elements and other stream messages (watermarks)
#[derive(Clone, Debug, Copy)]
pub enum ArconEvent<A: 'static + ArconType> {
    Element(ArconElement<A>),
    Watermark(Watermark),
}

/// Watermark
#[derive(Clone, Debug, Copy)]
pub struct Watermark {
    pub timestamp: u64,
}

impl<A: 'static + ArconType> ArconEvent<A> {
    pub fn from_remote(event: StreamTaskMessage) -> ArconResult<Self> {
        let payload = event.payload.unwrap();
        match payload {
            element(e) => {
                let event: A = bincode::deserialize(e.get_data()).map_err(|e| {
                    arcon_err_kind!("Failed to deserialise event with err {}", e.to_string())
                })?;
                let arcon_element = ArconElement::with_timestamp(event, e.get_timestamp());
                Ok(ArconEvent::Element(arcon_element))
            }
            keyed_element(e) => {
                // The key is implicit from the key_by
                let event: A = bincode::deserialize(e.get_data()).map_err(|e| {
                    arcon_err_kind!("Failed to deserialise event with err {}", e.to_string())
                })?;
                let arcon_element = ArconElement::with_timestamp(event, e.get_timestamp());
                Ok(ArconEvent::Element(arcon_element))
            }
            watermark(w) => Ok(ArconEvent::Watermark(Watermark::new(w.timestamp))),
            checkpoint(_) => {
                unimplemented!();
            }
        }
    }
    pub fn to_remote(&self) -> ArconResult<StreamTaskMessage> {
        match self {
            ArconEvent::Element(e) => {
                let serialised_event: Vec<u8> = bincode::serialize(&e.data).map_err(|e| {
                    arcon_err_kind!("Failed to serialise event with err {}", e.to_string())
                })?;

                let timestamp = e.timestamp.unwrap_or(0);
                Ok(create_element(serialised_event, timestamp))

                /* How do we know if it's keyed or not?
                if let Some(key) = key {
                    create_keyed_element(serialised_event, timestamp, key);

                } else {
                    create_element(serialised_event, timestamp);
                } */
            }
            ArconEvent::Watermark(w) => {
                let mut msg = StreamTaskMessage::new();
                let mut msg_watermark = messages::Watermark::new();
                msg_watermark.set_timestamp(w.timestamp);
                msg.set_watermark(msg_watermark);
                Ok(msg)
            }
        }
    }
}

impl Watermark {
    pub fn new(timestamp: u64) -> Self {
        Watermark { timestamp }
    }
}

/// An stream element that contains a `ArconType` and an optional timestamp
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
#[derive(Clone, Debug, Copy)]
pub struct ArconVec<A: ArconType> {
    pub ptr: *const A,
    pub len: i64,
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
