use std::marker::PhantomData;
use serde::{Serialize, Deserialize};
use error::ErrorKind;
use crate::{
    state_backend::{
        in_memory::{StateCommon, InMemory},
        state_types::{State, AppendingState, VecState, MergingState},
        StateBackend
    },
    prelude::ArconResult,
};

pub struct InMemoryVecState<IK, N, T> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T> State<InMemory, IK, N> for InMemoryVecState<IK, N, T>
    where IK: Serialize, N: Serialize {
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&key)
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T> AppendingState<InMemory, IK, N, T, Vec<T>> for InMemoryVecState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {
    fn get(&self, backend: &InMemory) -> ArconResult<Vec<T>> {
        let key = self.common.get_db_key(&())?;
        let serialized = backend.get(&key)?;

        // reader is updated in the loop to point at the yet unconsumed part of the serialized data
        let mut reader = &serialized[..];
        let mut res = vec![];
        while !reader.is_empty() {
            let val = bincode::deserialize_from(&mut reader)
                .map_err(|e| arcon_err_kind!("Could not deserialize vec state value: {}", e))?;
            res.push(val);
        }

        Ok(res)
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get_mut_or_init_empty(&key)?;

        bincode::serialize_into(storage, &value)
            .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))
    }
}

impl<IK, N, T> MergingState<InMemory, IK, N, T, Vec<T>> for InMemoryVecState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {}

impl<IK, N, T> VecState<InMemory, IK, N, T> for InMemoryVecState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {
    fn set(&self, backend: &mut InMemory, value: Vec<T>) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let mut storage = vec![];
        for elem in value {
            bincode::serialize_into(&mut storage, &elem)
                .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))?;
        }
        backend.put(&key, &storage)
    }

    fn add_all(&self, backend: &mut InMemory, values: impl IntoIterator<Item=T>) -> ArconResult<()> where Self: Sized {
        let key = self.common.get_db_key(&())?;
        let mut storage = backend.get_mut_or_init_empty(&key)?;

        for value in values {
            bincode::serialize_into(&mut storage, &value)
                .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))?;
        }

        Ok(())
    }

    fn add_all_dyn(&self, backend: &mut InMemory, values: &mut dyn Iterator<Item=T>) -> ArconResult<()> {
        self.add_all(backend, values)
    }

    fn len(&self, backend: &InMemory) -> ArconResult<usize> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get(&key);

        match storage {
            Err(e) => {
                match e.kind() {
                    ErrorKind::ArconError(message) if &*message == "Value not found" => Ok(0),
                    _ => Err(e)
                }
            }
            Ok(buf) => {
                let mut reader = buf;
                if buf.len() == 0 { return Ok(0); }

                // it's a bit unfortunate, but we need a value of a given type T, to compute its
                // bincode serialized size
                let first_value: T = bincode::deserialize_from(&mut reader)
                    .map_err(|e| arcon_err_kind!("Could not deserialize vec state value: {}", e))?;
                let first_value_serialized_size = bincode::serialized_size(&first_value)
                    .map_err(|e| arcon_err_kind!("Could not get the size of serialized vec state value: {}", e))? as usize;

                debug_assert_ne!(first_value_serialized_size, 0);

                let len = buf.len() / first_value_serialized_size;
                let rem = buf.len() % first_value_serialized_size;

                // sanity check
                if rem != 0 { return arcon_err!("vec state storage length is not a multiple of element size"); }

                Ok(len)
            }
        }
    }
}
