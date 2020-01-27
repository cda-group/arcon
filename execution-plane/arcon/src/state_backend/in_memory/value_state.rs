use std::marker::PhantomData;
use serde::{Serialize, Deserialize};
use crate::{
    state_backend::{
        in_memory::{StateCommon, InMemory},
        state_types::{State, ValueState},
        StateBackend
    },
    prelude::ArconResult,
};

pub struct InMemoryValueState<IK, N, T> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T> State<InMemory, IK, N> for InMemoryValueState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize {
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T> ValueState<InMemory, IK, N, T> for InMemoryValueState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {
    fn get(&self, backend: &InMemory) -> ArconResult<T> {
        let key = self.common.get_db_key(&())?;
        let serialized = backend.get(&key)?;
        let value = bincode::deserialize(&serialized)
            .map_err(|e| arcon_err_kind!("Cannot deserialize value state: {}", e))?;
        Ok(value)
    }

    fn set(&self, backend: &mut InMemory, new_value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let serialized = bincode::serialize(&new_value)
            .map_err(|e| arcon_err_kind!("Cannot serialize value state: {}", e))?;
        backend.put(&key, &serialized)?;
        Ok(())
    }
}
