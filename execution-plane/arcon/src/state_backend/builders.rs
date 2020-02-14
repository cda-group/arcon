// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::state_backend::{
    serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
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
            type Type = Box<dyn $state_name<dyn StateBackend, $($params),*>>;

            fn $builder_fn(
                &mut self,
                name: &str,
                init_item_key: IK,
                init_namespace: N,
                $($($arg_name: $arg_ty,)*)?
                key_serializer: KS,
                value_serializer: TS,
            ) -> Self::Type {
                {
                    use crate::state_backend::in_memory::InMemory;
                    if let Ok(in_memory) = self.downcast_mut::<InMemory>() {
                        return $state_name::erase_backend_type(in_memory.$builder_fn(
                            name,
                            init_item_key,
                            init_namespace,
                            $($($arg_name,)*)?
                            key_serializer,
                            value_serializer,
                        ));
                    }
                }

                #[cfg(feature = "arcon_rocksdb")]
                {
                    use crate::state_backend::rocks::RocksDb;
                    if let Ok(rocks) = self.downcast_mut::<RocksDb>() {
                        return $state_name::erase_backend_type(rocks.$builder_fn(
                            name,
                            init_item_key,
                            init_namespace,
                            $($($arg_name,)*)?
                            key_serializer,
                            value_serializer,
                        ));
                    }
                }

                // NOTE: every implemented state backend should be added here
                // TODO: maybe figure out some sort of dynamic discovery using the inventory crate?

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
        IK: SerializableFixedSizeWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + 'static,
        (): SerializableFixedSizeWith<KS>,
        T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: 'static,
        TS: 'static,
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
        IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + DeserializableWith<KS> + 'static,
        (): SerializableFixedSizeWith<KS>,
        K: SerializableWith<KS> + DeserializableWith<KS> + 'static,
        V: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: Clone + 'static,
        TS: Clone + 'static,
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
        IK: SerializableFixedSizeWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + 'static,
        (): SerializableWith<KS>,
        T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: 'static,
        TS: 'static,
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
        IK: SerializableFixedSizeWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + 'static,
        (): SerializableWith<KS>,
        T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
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
        IK: SerializableFixedSizeWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + 'static,
        (): SerializableWith<KS>,
        AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
        T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: Send + Sync + Clone + 'static,
        TS: Send + Sync + Clone + 'static,
    }; new_aggregating_state extra: (aggregator: AGG)
}
