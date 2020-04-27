// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        metered::Metered,
        state_types::{BoxedIteratorOfArconResult, MapState, State},
    },
};

pub struct MeteredMapState<MS> {
    pub(crate) inner: MS,
}

impl_metered_state!(MeteredMapState<MS>: MapState);

impl<SB, MS, IK, N, K, V> MapState<Metered<SB>, IK, N, K, V> for MeteredMapState<MS>
where
    MS: MapState<SB, IK, N, K, V>,
    K: 'static,
    V: 'static,
{
    measure_delegated! { MapState:
        fn get(&self, backend: &Metered<SB>, key: &K) -> ArconResult<Option<V>>;
        fn fast_insert(&self, backend: &mut Metered<SB>, key: K, value: V) -> ArconResult<()>;
        fn insert(&self, backend: &mut Metered<SB>, key: K, value: V) -> ArconResult<Option<V>>;
        fn insert_all_dyn(
            &self,
            backend: &mut Metered<SB>,
            key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
        ) -> ArconResult<()>;
        fn insert_all(
            &self,
            backend: &mut Metered<SB>,
            key_value_pairs: impl IntoIterator<Item = (K, V)>,
        ) -> ArconResult<()>
        where
            Self: Sized;
        fn remove(&self, backend: &mut Metered<SB>, key: &K) -> ArconResult<()>;
        fn contains(&self, backend: &Metered<SB>, key: &K) -> ArconResult<bool>;
        fn len(&self, backend: &Metered<SB>) -> ArconResult<usize>;
        fn is_empty(&self, backend: &Metered<SB>) -> ArconResult<bool>;
    }

    // the macro above doesn't support lifetime annotations
    fn iter<'a>(
        &self,
        backend: &'a Metered<SB>,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, (K, V)>> {
        let mut iter = backend.measure("MapState::iter", |backend| self.inner.iter(backend))?;
        let iter =
            std::iter::from_fn(move || backend.measure("MapState::iter::next", |_| iter.next()));
        Ok(Box::new(iter))
    }

    fn keys<'a>(&self, backend: &'a Metered<SB>) -> ArconResult<BoxedIteratorOfArconResult<'a, K>> {
        let mut iter = backend.measure("MapState::keys", |backend| self.inner.keys(backend))?;
        let iter =
            std::iter::from_fn(move || backend.measure("MapState::keys::next", |_| iter.next()));
        Ok(Box::new(iter))
    }

    fn values<'a>(
        &self,
        backend: &'a Metered<SB>,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, V>> {
        let mut iter: BoxedIteratorOfArconResult<'a, V> =
            backend.measure("MapState::values", |backend| self.inner.values(backend))?;
        let iter =
            std::iter::from_fn(move || backend.measure("MapState::values::next", |_| iter.next()));
        Ok(Box::new(iter))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        builders::MapStateBuilder,
        in_memory::InMemory,
        serialization::{NativeEndianBytesDump, Prost},
        StateBackend,
    };

    #[test]
    fn map_state_test() {
        let mut db = Metered::<InMemory>::new("test".as_ref()).unwrap();
        let map_state = db.new_map_state("test_state", (), (), NativeEndianBytesDump, Prost);

        // TODO: &String is weird, maybe look at how it's done with the keys in std hash-map
        assert!(!map_state.contains(&db, &"first key".to_string()).unwrap());

        map_state
            .fast_insert(&mut db, "first key".to_string(), 42)
            .unwrap();
        map_state
            .fast_insert(&mut db, "second key".to_string(), 69)
            .unwrap();

        assert!(map_state.contains(&db, &"first key".to_string()).unwrap());
        assert!(map_state.contains(&db, &"second key".to_string()).unwrap());

        assert_eq!(
            map_state
                .get(&db, &"first key".to_string())
                .unwrap()
                .unwrap(),
            42
        );
        assert_eq!(
            map_state
                .get(&db, &"second key".to_string())
                .unwrap()
                .unwrap(),
            69
        );

        assert_eq!(map_state.len(&db).unwrap(), 2);

        let keys: Vec<_> = map_state.keys(&db).unwrap().map(Result::unwrap).collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"first key".to_string()));
        assert!(keys.contains(&"second key".to_string()));

        map_state.clear(&mut db).unwrap();
        assert_eq!(map_state.len(&db).unwrap(), 0);
        assert!(map_state.is_empty(&db).unwrap());

        println!(
            "test metrics for =={}==\n{:#?}",
            db.backend_name,
            &**db.metrics.borrow()
        )
    }
}
