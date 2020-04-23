// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        metered::Metered,
        state_types::{AppendingState, MergingState, State, VecState},
    },
};

pub struct MeteredVecState<VS> {
    pub(crate) inner: VS,
}

impl_metered_state!(MeteredVecState<VS>: VecState);

impl<SB, VS, IK, N, T> AppendingState<Metered<SB>, IK, N, T, Vec<T>> for MeteredVecState<VS>
where
    VS: AppendingState<SB, IK, N, T, Vec<T>>,
{
    measure_delegated! { VecState:
        fn get(&self, backend: &Metered<SB>) -> ArconResult<Vec<T>>;
        fn append(&self, backend: &mut Metered<SB>, value: T) -> ArconResult<()>;
    }
}

impl<SB, VS, IK, N, T> MergingState<Metered<SB>, IK, N, T, Vec<T>> for MeteredVecState<VS> where
    VS: MergingState<SB, IK, N, T, Vec<T>>
{
}

impl<SB, VS, IK, N, T> VecState<Metered<SB>, IK, N, T> for MeteredVecState<VS>
where
    VS: VecState<SB, IK, N, T>,
{
    measure_delegated! { VecState:
        fn set(&self, backend: &mut Metered<SB>, value: Vec<T>) -> ArconResult<()>;
        fn add_all(
            &self,
            backend: &mut Metered<SB>,
            values: impl IntoIterator<Item = T>,
        ) -> ArconResult<()>
        where
            Self: Sized;
        fn add_all_dyn(
            &self,
            backend: &mut Metered<SB>,
            values: &mut dyn Iterator<Item = T>,
        ) -> ArconResult<()>;
        fn is_empty(&self, backend: &Metered<SB>) -> ArconResult<bool>;
        fn len(&self, backend: &Metered<SB>) -> ArconResult<usize>;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        in_memory::InMemory,
        serialization::{NativeEndianBytesDump, Prost},
        StateBackend, VecStateBuilder,
    };

    #[test]
    fn vec_state_test() {
        let mut db = Metered::<InMemory>::new("test".as_ref()).unwrap();
        let vec_state = db.new_vec_state("test_state", (), (), NativeEndianBytesDump, Prost);
        assert!(vec_state.is_empty(&db).unwrap());
        assert_eq!(vec_state.len(&db).unwrap(), 0);

        vec_state.append(&mut db, 1).unwrap();
        vec_state.append(&mut db, 2).unwrap();
        vec_state.append(&mut db, 3).unwrap();
        vec_state.add_all(&mut db, vec![4, 5, 6]).unwrap();

        assert_eq!(vec_state.get(&db).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(vec_state.len(&db).unwrap(), 6);

        println!(
            "test metrics for =={}==\n{:#?}",
            db.backend_name,
            &**db.metrics.borrow()
        )
    }
}
