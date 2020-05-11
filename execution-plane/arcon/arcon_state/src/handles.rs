// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::*;
use bytes::BufMut;

pub struct Handle<S, IK = (), N = ()>
where
    S: StateType,
    IK: Metakey,
    N: Metakey,
{
    pub id: &'static str,
    pub item_key: IK,
    pub namespace: N,
    pub extra_data: S::ExtraData,
    pub state_type: S,
    pub registered: bool,
}

impl<S, IK, N> Debug for Handle<S, IK, N>
where
    S: StateType,
    IK: Metakey,
    N: Metakey,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InactiveHandle<{}, {:?}>",
            any::type_name::<S>(),
            self.id
        )
    }
}

pub struct ActiveHandle<'a, B, S, IK = (), N = ()>
where
    B: Backend + ?Sized,
    S: StateType,
    IK: Metakey,
    N: Metakey,
{
    pub backend: &'a mut B,
    pub inner: &'a mut Handle<S, IK, N>,
}

impl<B, S, IK, N> Debug for ActiveHandle<'_, B, S, IK, N>
where
    B: Backend + ?Sized,
    S: StateType,
    IK: Metakey,
    N: Metakey,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Handle<{}, {:?}>", any::type_name::<S>(), self.inner.id)
    }
}

// region handle builders
impl<S: StateType<ExtraData = ()>> Handle<S, (), ()> {
    #[inline(always)]
    fn no_extra(id: &'static str) -> Handle<S> {
        Handle {
            id,
            item_key: (),
            namespace: (),
            extra_data: (),
            state_type: Default::default(),
            registered: false,
        }
    }
}

impl<T: Value> Handle<ValueState<T>> {
    pub fn value(id: &'static str) -> Self {
        Handle::no_extra(id)
    }
}
impl<K: Key, V: Value> Handle<MapState<K, V>> {
    pub fn map(id: &'static str) -> Self {
        Handle::no_extra(id)
    }
}
impl<T: Value> Handle<VecState<T>> {
    pub fn vec(id: &'static str) -> Self {
        Handle::no_extra(id)
    }
}
impl<T: Value, F: Reducer<T>> Handle<ReducerState<T, F>> {
    pub fn reducer(id: &'static str, reducer: F) -> Self {
        Handle {
            id,
            item_key: (),
            namespace: (),
            extra_data: reducer,
            state_type: ReducerState::default(),
            registered: false,
        }
    }
}
impl<A: Aggregator> Handle<AggregatorState<A>> {
    pub fn aggregator(id: &'static str, aggregator: A) -> Self {
        Handle {
            id,
            item_key: (),
            namespace: (),
            extra_data: aggregator,
            state_type: AggregatorState::default(),
            registered: false,
        }
    }
}

impl<S: StateType, IK: Metakey, N: Metakey> Handle<S, IK, N> {
    pub fn with_item_key<NIK: Metakey>(self, item_key: NIK) -> Handle<S, NIK, N> {
        Handle {
            id: self.id,
            item_key,
            namespace: self.namespace,
            extra_data: self.extra_data,
            state_type: self.state_type,
            registered: self.registered,
        }
    }
    pub fn with_namespace<NN: Metakey>(self, namespace: NN) -> Handle<S, IK, NN> {
        Handle {
            id: self.id,
            item_key: self.item_key,
            namespace,
            extra_data: self.extra_data,
            state_type: self.state_type,
            registered: self.registered,
        }
    }

    pub fn set_item_key(&mut self, item_key: IK) {
        self.item_key = item_key;
    }
    pub fn set_namespace(&mut self, namespace: N) {
        self.namespace = namespace;
    }

    #[inline(always)]
    pub fn serialize_metakeys_into(&self, dest: &mut impl BufMut) -> Result<()> {
        use crate::serialization::fixed_bytes::serialize_into;
        serialize_into(dest, &self.item_key)?;
        serialize_into(dest, &self.namespace)?;
        Ok(())
    }

    #[inline(always)]
    pub fn serialize_metakeys(&self) -> Result<Vec<u8>> {
        let mut dest = Vec::with_capacity(self.metakey_size());
        self.serialize_metakeys_into(&mut dest)?;
        Ok(dest)
    }

    #[inline(always)]
    pub fn serialize_metakeys_and_key_into(
        &self,
        key: &impl Key,
        dest: &mut impl BufMut,
    ) -> Result<()> {
        use crate::serialization::protobuf;
        self.serialize_metakeys_into(dest)?;
        protobuf::serialize_into(dest, key)?;
        Ok(())
    }

    #[inline(always)]
    pub fn serialize_metakeys_and_key(&self, key: &impl Key) -> Result<Vec<u8>> {
        use crate::serialization::*;
        let mut dest =
            Vec::with_capacity(self.metakey_size() + protobuf::size_hint(key).unwrap_or(0));
        self.serialize_metakeys_and_key_into(key, &mut dest)?;
        Ok(dest)
    }

    #[inline(always)]
    pub fn serialize_id_and_metakeys_into(&self, dest: &mut impl BufMut) -> Result<()> {
        use crate::serialization::*;
        fixed_bytes::serialize_bytes_into(dest, self.id.as_bytes())?;
        self.serialize_metakeys_into(dest)?;
        Ok(())
    }

    #[inline(always)]
    pub fn serialize_id_and_metakeys(&self) -> Result<Vec<u8>> {
        use crate::serialization::*;
        let mut dest = Vec::with_capacity(
            <usize as fixed_bytes::FixedBytes>::SIZE + self.id.len() + self.metakey_size(),
        );
        self.serialize_id_and_metakeys_into(&mut dest)?;
        Ok(dest)
    }

    #[inline(always)]
    pub fn serialize_id_metakeys_and_key_into(
        &self,
        dest: &mut impl BufMut,
        key: &impl Key,
    ) -> Result<()> {
        use crate::serialization::*;
        self.serialize_id_and_metakeys_into(dest)?;
        protobuf::serialize_into(dest, key)?;
        Ok(())
    }

    #[inline(always)]
    pub fn serialize_id_metakeys_and_key(&self, key: &impl Key) -> Result<Vec<u8>> {
        use crate::serialization::*;
        let mut dest = Vec::with_capacity(
            <usize as fixed_bytes::FixedBytes>::SIZE
                + self.id.len()
                + self.metakey_size()
                + protobuf::size_hint(key).unwrap_or(0),
        );
        self.serialize_id_metakeys_and_key_into(&mut dest, key)?;
        Ok(dest)
    }

    #[inline(always)]
    pub fn metakey_size(&self) -> usize {
        return IK::SIZE + N::SIZE;
    }
}
//endregion

impl<S: StateType, IK: Metakey, N: Metakey> Handle<S, IK, N> {
    #[doc(hidden)]
    #[inline]
    pub fn activate<'s, B: Backend>(
        &'s mut self,
        backend: &'s mut B,
    ) -> ActiveHandle<'s, B, S, IK, N> {
        if !self.registered {
            panic!("State handles should be registered before activation!")
        }
        ActiveHandle {
            backend,
            inner: self,
        }
    }
}

// region handle activators
impl<T: Value, IK: Metakey, N: Metakey> Handle<ValueState<T>, IK, N> {
    #[doc(hidden)]
    pub fn register<'s, B: Backend>(
        &'s mut self,
        session: &'s mut Session<B>,
        _registration_token: &RegistrationToken,
    ) {
        session.backend.register_value_handle(self)
    }
}
impl<K: Key, V: Value, IK: Metakey, N: Metakey> Handle<MapState<K, V>, IK, N> {
    #[doc(hidden)]
    pub fn register<'s, B: Backend>(
        &'s mut self,
        session: &'s mut Session<B>,
        _registration_token: &RegistrationToken,
    ) {
        session.backend.register_map_handle(self)
    }
}
impl<T: Value, IK: Metakey, N: Metakey> Handle<VecState<T>, IK, N> {
    #[doc(hidden)]
    pub fn register<'s, B: Backend>(
        &'s mut self,
        session: &'s mut Session<B>,
        _registration_token: &RegistrationToken,
    ) {
        session.backend.register_vec_handle(self)
    }
}
impl<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey> Handle<ReducerState<T, F>, IK, N> {
    #[doc(hidden)]
    pub fn register<'s, B: Backend>(
        &'s mut self,
        session: &'s mut Session<B>,
        _registration_token: &RegistrationToken,
    ) {
        session.backend.register_reducer_handle(self)
    }
}
impl<A: Aggregator, IK: Metakey, N: Metakey> Handle<AggregatorState<A>, IK, N> {
    #[doc(hidden)]
    pub fn register<'s, B: Backend>(
        &'s mut self,
        session: &'s mut Session<B>,
        _registration_token: &RegistrationToken,
    ) {
        session.backend.register_aggregator_handle(self)
    }
}
// endregion

impl<B: Backend, S: StateType, IK: Metakey, N: Metakey> ActiveHandle<'_, B, S, IK, N> {
    pub fn set_item_key(&mut self, item_key: IK) {
        self.inner.set_item_key(item_key)
    }
    pub fn set_namespace(&mut self, namespace: N) {
        self.inner.set_namespace(namespace)
    }
}

impl<B: Backend, T: Value, IK: Metakey, N: Metakey> ActiveHandle<'_, B, ValueState<T>, IK, N> {
    pub fn clear(&mut self) -> Result<()> {
        self.backend.value_clear(self.inner)
    }

    pub fn get(&self) -> Result<Option<T>> {
        self.backend.value_get(self.inner)
    }

    pub fn set(&mut self, value: T) -> Result<Option<T>> {
        self.backend.value_set(self.inner, value)
    }

    pub fn fast_set(&mut self, value: T) -> Result<()> {
        self.backend.value_fast_set(self.inner, value)
    }
}

pub type BoxedIteratorOfResult<'a, T> = Box<dyn Iterator<Item = Result<T>> + 'a>;
impl<B: Backend, K: Key, V: Value, IK: Metakey, N: Metakey>
    ActiveHandle<'_, B, MapState<K, V>, IK, N>
{
    pub fn clear(&mut self) -> Result<()> {
        self.backend.map_clear(self.inner)
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.backend.map_get(self.inner, key)
    }
    pub fn fast_insert(&mut self, key: K, value: V) -> Result<()> {
        self.backend.map_fast_insert(self.inner, key, value)
    }
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        self.backend.map_insert(self.inner, key, value)
    }
    pub fn insert_all(&mut self, key_value_pairs: impl IntoIterator<Item = (K, V)>) -> Result<()> {
        self.backend.map_insert_all(self.inner, key_value_pairs)
    }

    pub fn remove(&mut self, key: &K) -> Result<Option<V>> {
        self.backend.map_remove(self.inner, key)
    }
    pub fn fast_remove(&mut self, key: &K) -> Result<()> {
        self.backend.map_fast_remove(self.inner, key)
    }
    pub fn contains(&self, key: &K) -> Result<bool> {
        self.backend.map_contains(self.inner, key)
    }

    // unboxed iterators would require associated types generic over backend's lifetime
    // TODO: impl this when GATs land on nightly
    pub fn iter(&self) -> Result<BoxedIteratorOfResult<(K, V)>> {
        self.backend.map_iter(self.inner)
    }
    pub fn keys(&self) -> Result<BoxedIteratorOfResult<K>> {
        self.backend.map_keys(self.inner)
    }
    pub fn values(&self) -> Result<BoxedIteratorOfResult<V>> {
        self.backend.map_values(self.inner)
    }

    pub fn len(&self) -> Result<usize> {
        self.backend.map_len(self.inner)
    }
    pub fn is_empty(&self) -> Result<bool> {
        self.backend.map_is_empty(self.inner)
    }
}

impl<B: Backend, T: Value, IK: Metakey, N: Metakey> ActiveHandle<'_, B, VecState<T>, IK, N> {
    pub fn clear(&mut self) -> Result<()> {
        self.backend.vec_clear(self.inner)
    }
    pub fn append(&mut self, value: T) -> Result<()> {
        self.backend.vec_append(self.inner, value)
    }
    pub fn get(&self) -> Result<Vec<T>> {
        self.backend.vec_get(self.inner)
    }
    pub fn iter(&self) -> Result<BoxedIteratorOfResult<T>> {
        self.backend.vec_iter(self.inner)
    }
    pub fn set(&mut self, value: Vec<T>) -> Result<()> {
        self.backend.vec_set(self.inner, value)
    }
    pub fn add_all(&mut self, values: impl IntoIterator<Item = T>) -> Result<()> {
        self.backend.vec_add_all(self.inner, values)
    }
    pub fn is_empty(&self) -> Result<bool> {
        self.backend.vec_is_empty(self.inner)
    }
    pub fn len(&self) -> Result<usize> {
        self.backend.vec_len(self.inner)
    }
}

impl<B: Backend, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>
    ActiveHandle<'_, B, ReducerState<T, F>, IK, N>
{
    pub fn clear(&mut self) -> Result<()> {
        self.backend.reducer_clear(self.inner)
    }

    pub fn get(&self) -> Result<Option<T>> {
        self.backend.reducer_get(self.inner)
    }

    pub fn reduce(&mut self, value: T) -> Result<()> {
        self.backend.reducer_reduce(self.inner, value)
    }
}

impl<B: Backend, A: Aggregator, IK: Metakey, N: Metakey>
    ActiveHandle<'_, B, AggregatorState<A>, IK, N>
{
    pub fn clear(&mut self) -> Result<()> {
        self.backend.aggregator_clear(self.inner)
    }

    pub fn get(&self) -> Result<A::Result> {
        self.backend.aggregator_get(self.inner)
    }

    pub fn aggregate(&mut self, value: A::Input) -> Result<()> {
        self.backend.aggregator_aggregate(self.inner, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_serialization() {
        let handle: Handle<MapState<String, u32>, _, _> =
            Handle::map("test").with_item_key(0u8).with_namespace(0u8);
        let key = handle
            .serialize_metakeys_and_key(&"foobar".to_string())
            .unwrap();
        assert_eq!(
            key.len(),
            1 + 1 + serialization::protobuf::size_hint(&"foobar".to_string()).unwrap()
        );
    }

    #[test]
    fn test_unit_state_key_empty() {
        let handle: Handle<ValueState<u32>> = Handle::value("test");
        let v = handle.serialize_metakeys().unwrap();
        assert!(v.is_empty());
    }
}
