// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use std::marker::PhantomData;
use serde::{Serialize, Deserialize};
use crate::{
    state_backend::{
        rocksdb::{StateCommon, RocksDb},
        state_types::{State, MapState},
    },
    prelude::ArconResult,
};
use super::rocksdb::WriteBatch;

pub struct RocksDbMapState<IK, N, K, V> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) _phantom: PhantomData<(K, V)>,
}

impl<IK, N, K, V> State<RocksDb, IK, N> for RocksDbMapState<IK, N, K, V>
    where IK: Serialize, N: Serialize {
    fn clear(&self, backend: &mut RocksDb) -> ArconResult<()> {
        // () is not serialized, and the user key is the tail of the db key, so in effect we get
        // the prefix with which to search the underlying db.
        let cf = backend.get_cf(&self.common.cf_name)?;
        let start = self.common.get_db_key(&())?;
        if start.is_empty() { //
            backend.db.drop_cf()
        }

        let mut end = start.clone();
        end.last_mut().map(|last| { *last += 1; last });


        let mut wb = WriteBatch::default();
        wb.delete_range_cf(cf, first, last)
            .map_err(|e| arcon_err_kind!("Could not create delete_range operation: {}", e))?;

        backend.db.write(wb)
            .map_err(|e| arcon_err_kind!("Could not perform delete_range operation: {}", e))?;

        Ok(())
    }


    delegate_key_and_namespace!(common);
}

impl<IK, N, K, V> MapState<RocksDb, IK, N, K, V> for RocksDbMapState<IK, N, K, V>
    where
        IK: Serialize + for<'a> Deserialize<'a>,
        N: Serialize + for<'a> Deserialize<'a>,
        K: Serialize + for<'a> Deserialize<'a>,
        V: Serialize + for<'a> Deserialize<'a> {
    fn get(&self, backend: &RocksDb, key: &K) -> ArconResult<V> {
        let key = self.common.get_db_key(key)?;
        let serialized = backend.get(&self.common.cf_name, &key)?;
        let value = bincode::deserialize(&serialized)
            .map_err(|e| arcon_err_kind!("Cannot deserialize map state value: {}", e))?;

        Ok(value)
    }

    fn put(&self, backend: &mut RocksDb, key: K, value: V) -> ArconResult<()> {
        let key = self.common.get_db_key(&key)?;
        let serialized = bincode::serialize(&value)
            .map_err(|e| arcon_err_kind!("Could not serialize map state value: {}", e))?;
        backend.put(&self.common.cf_name, key, serialized)?;

        Ok(())
    }

    fn put_all_dyn(&self, backend: &mut RocksDb, key_value_pairs: &mut dyn Iterator<Item=(K, V)>) -> ArconResult<()> {
        self.put_all(backend, key_value_pairs)
    }

    fn put_all(&self, backend: &mut RocksDb, key_value_pairs: impl IntoIterator<Item=(K, V)>) -> ArconResult<()> where Self: Sized {
        todo!("Implement this as batch write")
    }

    fn remove(&self, backend: &mut RocksDb, key: &K) -> ArconResult<()> {
        let key = self.common.get_db_key(key)?;
        backend.remove(&self.common.cf_name, &key)?;

        Ok(())
    }

    fn contains(&self, backend: &RocksDb, key: &K) -> ArconResult<bool> {
        let key = self.common.get_db_key(key)?;
        backend.contains(&key)
    }

    // TODO: unboxed versions of below
    fn iter<'a>(&self, backend: &'a RocksDb) -> ArconResult<Box<dyn Iterator<Item=(K, V)> + 'a>> {
        todo!()
    }

    fn keys<'a>(&self, backend: &'a RocksDb) -> ArconResult<Box<dyn Iterator<Item=K> + 'a>> {
        todo!()
    }

    fn values<'a>(&self, backend: &'a RocksDb) -> ArconResult<Box<dyn Iterator<Item=V> + 'a>> {
        todo!()
    }

    fn is_empty(&self, backend: &RocksDb) -> ArconResult<bool> {
        let prefix = self.common.get_db_key(&())?;
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{StateBackend, MapStateBuilder};
    use tempfile::TempDir;

    #[test]
    fn map_state_test() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let mut db = RocksDb::new(&dir_path).unwrap();
        let map_state = db.new_map_state((), ());

        // TODO: &String is weird, maybe look at how it's done with the keys in std hash-map
        assert!(!map_state.contains(&db, &"first key".to_string()).unwrap());

        map_state.put(&mut db, "first key".to_string(), 42).unwrap();
        map_state.put(&mut db, "second key".to_string(), 69).unwrap();

        assert!(map_state.contains(&db, &"first key".to_string()).unwrap());
        assert!(map_state.contains(&db, &"second key".to_string()).unwrap());

        assert_eq!(map_state.get(&db, &"first key".to_string()).unwrap(), 42);
        assert_eq!(map_state.get(&db, &"second key".to_string()).unwrap(), 69);

        let keys: Vec<_> = map_state.keys(&db).unwrap().collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"first key".to_string()));
        assert!(keys.contains(&"second key".to_string()));
    }
}
