// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::state_backend::{
    in_memory::{
        aggregating_state::InMemoryAggregatingState, map_state::InMemoryMapState,
        reducing_state::InMemoryReducingState, value_state::InMemoryValueState,
        vec_state::InMemoryVecState,
    },
    serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
    state_types::*,
    AggregatingStateBuilder, MapStateBuilder, ReducingStateBuilder, StateBackend,
    ValueStateBuilder, VecStateBuilder,
};
use arcon_error::*;
use std::{collections::HashMap, fmt::Debug};
use uuid::Uuid;

pub struct InMemory {
    db: HashMap<Vec<u8>, Vec<u8>>,
}

impl InMemory {
    fn new_state_common<IK, N, KS, TS>(
        &self,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> StateCommon<IK, N, KS, TS> {
        StateCommon {
            id: Uuid::new_v4(),
            item_key,
            namespace,
            key_serializer,
            value_serializer,
        }
    }

    pub fn remove_matching(&mut self, prefix: &[u8]) -> ArconResult<()> {
        self.db.retain(|k, _| &k[..prefix.len()] != prefix);
        Ok(())
    }

    pub fn iter_matching(
        &self,
        prefix: impl AsRef<[u8]> + Debug,
    ) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.db.iter().filter_map(move |(k, v)| {
            if &k[..prefix.as_ref().len()] != prefix.as_ref() {
                return None;
            }
            Some((k.as_slice(), v.as_slice()))
        })
    }

    pub fn contains(&self, key: &[u8]) -> ArconResult<bool> {
        Ok(self.db.contains_key(key))
    }

    pub fn get(&self, key: &[u8]) -> ArconResult<&[u8]> {
        if let Some(data) = self.db.get(key) {
            Ok(&*data)
        } else {
            return arcon_err!("Value not found");
        }
    }

    pub fn get_mut(&mut self, key: &[u8]) -> ArconResult<&mut Vec<u8>> {
        if let Some(data) = self.db.get_mut(key) {
            Ok(data)
        } else {
            return arcon_err!("Value not found");
        }
    }

    pub fn get_mut_or_init_empty(&mut self, key: &[u8]) -> ArconResult<&mut Vec<u8>> {
        Ok(self.db.entry(key.to_vec()).or_insert_with(|| vec![]))
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> ArconResult<()> {
        self.db.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> ArconResult<()> {
        let _ = self.db.remove(key);
        Ok(())
    }
}

// since we don't do checkpointing for InMemory state backend, the name of the state is simply discarded
// TODO: maybe keep it for debugging purposes?
impl<IK, N, T, KS, TS> ValueStateBuilder<IK, N, T, KS, TS> for InMemory
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = InMemoryValueState<IK, N, T, KS, TS>;

    fn new_value_state(
        &mut self,
        _name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = self.new_state_common(
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        );
        InMemoryValueState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, K, V, KS, TS> MapStateBuilder<IK, N, K, V, KS, TS> for InMemory
where
    IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    N: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: SerializableWith<TS> + DeserializableWith<TS>,
    KS: Clone + 'static,
    TS: Clone + 'static,
{
    type Type = InMemoryMapState<IK, N, K, V, KS, TS>;

    fn new_map_state(
        &mut self,
        _name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = self.new_state_common(
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        );
        InMemoryMapState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, KS, TS> VecStateBuilder<IK, N, T, KS, TS> for InMemory
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = InMemoryVecState<IK, N, T, KS, TS>;

    fn new_vec_state(
        &mut self,
        _name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = self.new_state_common(
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        );
        InMemoryVecState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, F, KS, TS> ReducingStateBuilder<IK, N, T, F, KS, TS> for InMemory
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
    type Type = InMemoryReducingState<IK, N, T, F, KS, TS>;

    fn new_reducing_state(
        &mut self,
        _name: &str,
        init_item_key: IK,
        init_namespace: N,
        reduce_fn: F,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = self.new_state_common(
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        );
        InMemoryReducingState {
            common,
            reduce_fn,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, AGG, KS, TS> AggregatingStateBuilder<IK, N, T, AGG, KS, TS> for InMemory
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = InMemoryAggregatingState<IK, N, T, AGG, KS, TS>;

    fn new_aggregating_state(
        &mut self,
        _name: &str,
        init_item_key: IK,
        init_namespace: N,
        aggregator: AGG,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = self.new_state_common(
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        );
        InMemoryAggregatingState {
            common,
            aggregator,
            _phantom: Default::default(),
        }
    }
}

impl StateBackend for InMemory {
    fn new(_path: &str) -> ArconResult<InMemory> {
        Ok(InMemory { db: HashMap::new() })
    }

    fn checkpoint(&self, _id: &str) -> ArconResult<()> {
        // TODO: proper logging
        eprintln!("InMemory backend snapshotting is not implemented");
        Ok(())
    }

    fn restore(restore_path: &str, _checkpoint_path: &str) -> ArconResult<Self>
    where
        Self: Sized,
    {
        // TODO: proper logging
        eprintln!("InMemory backend restoring is not implemented");
        Self::new(restore_path)
    }
}

pub(crate) struct StateCommon<IK, N, KS, TS> {
    id: Uuid,
    item_key: IK,
    namespace: N,
    key_serializer: KS,
    value_serializer: TS,
}

impl<IK, N, KS, TS> StateCommon<IK, N, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn get_db_key_with_user_key<UK>(&self, user_key: &UK) -> ArconResult<Vec<u8>>
    where
        UK: SerializableWith<KS>,
    {
        // UUID is not always serializable, let's just dump the bytes
        let mut res = self.id.as_bytes().to_vec();
        IK::serialize_into(&self.key_serializer, &mut res, &self.item_key)?;
        N::serialize_into(&self.key_serializer, &mut res, &self.namespace)?;
        UK::serialize_into(&self.key_serializer, &mut res, user_key)?;

        Ok(res)
    }

    fn get_db_key_prefix(&self) -> ArconResult<Vec<u8>> {
        // UUID is not always serializable, let's just dump the bytes
        let mut res = self.id.as_bytes().to_vec();
        IK::serialize_into(&self.key_serializer, &mut res, &self.item_key)?;
        N::serialize_into(&self.key_serializer, &mut res, &self.namespace)?;

        Ok(res)
    }
}

mod aggregating_state;
mod map_state;
mod reducing_state;
mod value_state;
mod vec_state;

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::serialization::Bincode;

    #[test]
    fn in_mem_test() {
        let mut db = InMemory::new("test").unwrap();
        let key = "key";
        let value = "hej";
        db.put(key.to_string().into_bytes(), value.to_string().into_bytes())
            .unwrap();
        let fetched = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(fetched));
        db.remove(key.as_bytes()).unwrap();
        let res = db.get(key.as_bytes());
        assert!(res.is_err());
    }

    #[test]
    fn test_namespace_serialization() {
        // we rely on the order of the serialized fields, because we search by prefix when clearing
        // map state

        let state = StateCommon {
            id: Uuid::new_v4(),
            item_key: 42,
            namespace: 255,
            key_serializer: Bincode,
            value_serializer: Bincode,
        };

        let v = state.get_db_key_prefix().unwrap();
        let v2 = state.get_db_key_with_user_key(&"hello").unwrap();

        assert_eq!(&v2[..v.len()], &v[..]);
    }
}
