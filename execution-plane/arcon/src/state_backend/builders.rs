// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::state_backend::{
    serialization::{Bincode, DeserializableWith, SerializableFixedSizeWith, SerializableWith},
    state_types::{AggregatingState, Aggregator, MapState, ReducingState, ValueState, VecState},
    StateBackend,
};

// here be lil' dragons
macro_rules! impl_dynamic_builder {
    (
        $builder_name: ident <$($builder_params: ident),*> builds $state_name: ident <_, $($params: path),*>
        where {$($bounds: tt)*};
        $builder_fn: ident $(extra: ($($arg_name: ident : $arg_ty: ty),*))?
    ) => {
        impl<$($builder_params),*> $builder_name<$($builder_params),*> for dyn StateBackend
        where $($bounds)*
        {
            type Type = Box<dyn $state_name<dyn StateBackend, $($params),*> + Send + Sync + 'static>;

            fn $builder_fn(
                &mut self,
                name: &str,
                init_item_key: IK,
                init_namespace: N,
                $($($arg_name: $arg_ty,)*)?
                key_serializer: KS,
                value_serializer: TS,
            ) -> Self::Type {

                // yes, the macro expands to a macro definition, why are you asking?
                macro_rules! handle_backend {
                    ($backend: ty) => {{
                         if let Ok(b) = self.downcast_mut::<$backend>() {
                            return $state_name::erase_backend_type(b.$builder_fn(
                                name,
                                init_item_key,
                                init_namespace,
                                $($($arg_name,)*)?
                                key_serializer,
                                value_serializer,
                            ));
                         }
                    }};
                }

                handle_backend!(crate::state_backend::in_memory::InMemory);
                #[cfg(feature = "arcon_rocksdb")]
                handle_backend!(crate::state_backend::rocks::RocksDb);

                // NOTE: every implemented state backend should be added here

                unimplemented!(concat!(
                    "Unimplemented! Does `{}` implement `", stringify!($builder_name),
                    "`? Is `impl_dynamic_builder` checking the type?"
                ), self.type_name())
            }
        }
    };
}

// ideally this would be a part of the StateBackend trait, but we lack generic associated types, and
// there are probably some other issues with that as well
pub trait ValueStateBuilder<IK, N, T, KS, TS> {
    type Type: ValueState<Self, IK, N, T>;
    fn new_value_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    ValueStateBuilder<IK, N, T, KS, TS> builds ValueState<_, IK, N, T> where {
        IK: SerializableFixedSizeWith<KS> + Send + Sync + 'static,
        N: SerializableFixedSizeWith<KS> + Send + Sync + 'static,
        T: SerializableWith<TS> + DeserializableWith<TS> + Send + Sync + Clone + 'static,
        KS: Send + Sync + 'static,
        TS: Send + Sync + 'static,
    }; new_value_state
}

pub trait MapStateBuilder<IK, N, K, V, KS, TS> {
    type Type: MapState<Self, IK, N, K, V>;
    fn new_map_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    MapStateBuilder<IK, N, K, V, KS, TS> builds MapState<_, IK, N, K, V> where {
        IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS> + Send + Sync + 'static,
        N: SerializableFixedSizeWith<KS> + DeserializableWith<KS> + Send + Sync + 'static,
        K: SerializableWith<KS> + DeserializableWith<KS> + Send + Sync + 'static,
        V: SerializableWith<TS> + DeserializableWith<TS> + Send + Sync + Clone +'static,
        KS: Clone + Send + Sync + 'static,
        TS: Clone + Send + Sync + 'static,
    }; new_map_state
}

pub trait VecStateBuilder<IK, N, T, KS, TS> {
    type Type: VecState<Self, IK, N, T>;
    fn new_vec_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    VecStateBuilder<IK, N, T, KS, TS> builds VecState<_, IK, N, T> where {
        IK: SerializableFixedSizeWith<KS> + Send + Sync + 'static,
        N: SerializableFixedSizeWith<KS> + Send + Sync + 'static,
        T: SerializableWith<TS> + DeserializableWith<TS> + Send + Sync + Clone + 'static,
        KS: Send + Sync + 'static,
        TS: Send + Sync + 'static,
    }; new_vec_state
}

pub trait ReducingStateBuilder<IK, N, T, F, KS, TS> {
    type Type: ReducingState<Self, IK, N, T>;
    fn new_reducing_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        reduce_fn: F,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    ReducingStateBuilder<IK, N, T, F, KS, TS> builds ReducingState<_, IK, N, T> where {
        IK: SerializableFixedSizeWith<KS> + Send + Sync + 'static,
        N: SerializableFixedSizeWith<KS> + Send + Sync + 'static,
        T: SerializableWith<TS> + DeserializableWith<TS> + Send + Sync + Clone + 'static,
        KS: Send + Sync + Clone + 'static,
        TS: Send + Sync + Clone + 'static,
        F: Fn(&T, &T) -> T + Send + Sync + Clone + 'static
    }; new_reducing_state extra: (reduce_fn: F)
}

pub trait AggregatingStateBuilder<IK, N, T, AGG: Aggregator<T>, KS, TS> {
    type Type: AggregatingState<Self, IK, N, T, AGG::Result>;
    fn new_aggregating_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        aggregator: AGG,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    AggregatingStateBuilder<IK, N, T, AGG, KS, TS> builds AggregatingState<_, IK, N, T, AGG::Result> where {
        AGG: Aggregator<T> + Send + Sync + Clone + 'static,
        IK: SerializableFixedSizeWith<KS> + Send + Sync + 'static,
        N: SerializableFixedSizeWith<KS> + Send + Sync + 'static,
        AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS> + Send + Sync + Clone,
        T: SerializableWith<TS> + DeserializableWith<TS> + Send + Sync + 'static,
        KS: Send + Sync + Clone + 'static,
        TS: Send + Sync + Clone + 'static,
    }; new_aggregating_state extra: (aggregator: AGG)
}

/// Convenient high-level builder for States. Assumes no special item key nor namespace and Bincode
/// for all the serializers by default.
pub struct StateBuilder<'n, 'b, SB: ?Sized, IK, N, KS, TS> {
    name: &'n str,
    state_backend: &'b mut SB,
    init_item_key: IK,
    init_namespace: N,
    key_serializer: KS,
    value_serializer: TS,
}

// 200 lines of boilerplate ;_;
impl<'n, 'b, SB: ?Sized, IK, N, KS, TS> StateBuilder<'n, 'b, SB, IK, N, KS, TS> {
    pub fn with_init_item_key<NIK>(
        self,
        init_item_key: NIK,
    ) -> StateBuilder<'n, 'b, SB, NIK, N, KS, TS> {
        let StateBuilder {
            name,
            state_backend,
            init_namespace,
            key_serializer,
            value_serializer,
            ..
        } = self;
        StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        }
    }

    pub fn with_init_namespace<NN>(
        self,
        init_namespace: NN,
    ) -> StateBuilder<'n, 'b, SB, IK, NN, KS, TS> {
        let StateBuilder {
            name,
            state_backend,
            init_item_key,
            key_serializer,
            value_serializer,
            ..
        } = self;
        StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        }
    }

    pub fn with_key_serializer<NKS>(
        self,
        key_serializer: NKS,
    ) -> StateBuilder<'n, 'b, SB, IK, N, NKS, TS> {
        let StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            value_serializer,
            ..
        } = self;
        StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        }
    }

    pub fn with_value_serializer<NTS>(
        self,
        value_serializer: NTS,
    ) -> StateBuilder<'n, 'b, SB, IK, N, KS, NTS> {
        let StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            ..
        } = self;
        StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        }
    }

    pub fn value<T>(self) -> SB::Type
    where
        SB: ValueStateBuilder<IK, N, T, KS, TS>,
    {
        let StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        } = self;
        state_backend.new_value_state(
            name,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        )
    }

    pub fn map<K, V>(self) -> SB::Type
    where
        SB: MapStateBuilder<IK, N, K, V, KS, TS>,
    {
        let StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        } = self;
        state_backend.new_map_state(
            name,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        )
    }

    pub fn vec<T>(self) -> SB::Type
    where
        SB: VecStateBuilder<IK, N, T, KS, TS>,
    {
        let StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        } = self;
        state_backend.new_vec_state(
            name,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        )
    }

    pub fn reducing<T, F>(self, reduce_fn: F) -> SB::Type
    where
        SB: ReducingStateBuilder<IK, N, T, F, KS, TS>,
    {
        let StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        } = self;
        state_backend.new_reducing_state(
            name,
            init_item_key,
            init_namespace,
            reduce_fn,
            key_serializer,
            value_serializer,
        )
    }

    pub fn aggregating<T, AGG>(self, aggregator: AGG) -> SB::Type
    where
        SB: AggregatingStateBuilder<IK, N, T, AGG, KS, TS>,
        AGG: Aggregator<T>,
    {
        let StateBuilder {
            name,
            state_backend,
            init_item_key,
            init_namespace,
            key_serializer,
            value_serializer,
        } = self;
        state_backend.new_aggregating_state(
            name,
            init_item_key,
            init_namespace,
            aggregator,
            key_serializer,
            value_serializer,
        )
    }
}

pub trait StateBackendExt {
    fn build<'b, 'n>(
        &'b mut self,
        name: &'n str,
    ) -> StateBuilder<'n, 'b, Self, (), (), Bincode, Bincode> {
        StateBuilder {
            name,
            state_backend: self,
            init_item_key: (),
            init_namespace: (),
            key_serializer: Bincode,
            value_serializer: Bincode,
        }
    }
}

impl<T: StateBackend> StateBackendExt for T {}
impl StateBackendExt for dyn StateBackend {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::in_memory::InMemory;

    #[test]
    fn test_by_value_state_backend() {
        let mut sb = InMemory::new("test").unwrap();
        let value = sb.build("test_state").value();
        value.set(&mut sb, 42).unwrap();
    }

    #[test]
    fn test_dynamic_state_backend() {
        let mut sb: Box<dyn StateBackend> = Box::new(InMemory::new("test").unwrap());
        let value = sb.build("test_state").value();
        value.set(&mut *sb, 42).unwrap();
    }
}
