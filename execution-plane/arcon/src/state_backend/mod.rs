// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{conf::ArconConf, data::NodeID};
use arcon_error::*;
use std::{
    any::{type_name, Any, TypeId},
    collections::HashMap,
    fs,
    path::Path,
};

/// Trait required for all state backend implementations in Arcon
pub trait StateBackend: Any + Send {
    fn new(path: &Path) -> ArconResult<Self>
    where
        Self: Sized;

    fn checkpoint(&self, checkpoint_path: &Path) -> ArconResult<()>;
    fn restore(restore_path: &Path, checkpoint_path: &Path) -> ArconResult<Self>
    where
        Self: Sized;

    fn type_name(&self) -> &'static str {
        type_name::<Self>()
    }

    fn was_restored(&self) -> bool;

    fn restore_or_new(cfg: &ArconConf, num_nodes: u64, node_id: NodeID) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let mut state_path = cfg.state_dir.clone();
        state_path.push(node_id.id.to_string());
        if state_path.exists() {
            fs::remove_dir_all(&state_path).ctx("Could not clear live state directory")?;
        }

        let dirs: Vec<String> = fs::read_dir(&cfg.checkpoint_dir)
            .ctx("Cannot read checkpoints directory")?
            .map(|d| d.map(|d| d.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>, _>>()
            .ctx("Cannot read checkpoints directory file names")?;

        let latest_complete_checkpoint = dirs
            .into_iter()
            .filter(|x| x.starts_with("checkpoint_"))
            .map(|x| x.split('_').last().unwrap().parse::<u64>().unwrap())
            .fold(HashMap::new(), |mut acc, el| {
                *acc.entry(el).or_insert(0) += 1;
                acc
            })
            .into_iter()
            .filter(|e| e.1 == num_nodes)
            .map(|e| e.0)
            .max();

        match latest_complete_checkpoint {
            Some(epoch) => {
                let mut latest_checkpoint_path = cfg.checkpoint_dir.clone();
                latest_checkpoint_path.push(format!(
                    "checkpoint_{id}_{epoch}",
                    id = node_id.id,
                    epoch = epoch
                ));

                Self::restore(&state_path, &latest_checkpoint_path)
            }
            None => Self::new(&state_path),
        }
    }

    // NB: Self HAS TO outlive the session - Session should actually probably be a Session<'a>, but
    // right now this is ignored to make prototyping faster
    fn new_session(&self) -> Session {
        Session(Box::new(NoOpSession))
    }
}

// This is copied from std::any, because rust trait inheritance kinda sucks. Even std::any has
// duplicated impls for different combinations of Any with marker traits such as Send and Sync :/
impl dyn StateBackend {
    pub fn is<SB: StateBackend>(&self) -> bool {
        let t = TypeId::of::<SB>();
        let concrete = self.type_id();
        t == concrete
    }

    pub fn downcast_ref<SB: StateBackend>(&self) -> ArconResult<&SB> {
        if self.is::<SB>() {
            unsafe { Ok(&*(self as *const dyn StateBackend as *const SB)) }
        } else {
            arcon_err!("dynamic backend reference is of wrong actual type")
        }
    }

    pub fn downcast_mut<SB: StateBackend>(&mut self) -> ArconResult<&mut SB> {
        if self.is::<SB>() {
            unsafe { Ok(&mut *(self as *mut dyn StateBackend as *mut SB)) }
        } else {
            arcon_err!("dynamic backend reference is of wrong actual type")
        }
    }
}

pub struct Session(Box<dyn SessionImpl>);
trait SessionImpl {}
struct NoOpSession;
impl SessionImpl for NoOpSession {}

pub mod builders;
pub mod serialization;
#[macro_use]
pub mod state_types;

pub mod in_memory;
pub use in_memory::InMemory;
pub fn in_memory(
    cfg: &ArconConf,
    num_nodes: u64,
    node_id: NodeID,
) -> ArconResult<Box<dyn StateBackend>> {
    let sb = in_memory::InMemory::restore_or_new(cfg, num_nodes, node_id)?;
    Ok(Box::new(sb))
}

#[cfg(feature = "arcon_rocksdb")]
pub mod rocks;
#[cfg(feature = "arcon_rocksdb")]
pub use rocks::RocksDb;
#[cfg(feature = "arcon_rocksdb")]
pub fn rocks(
    cfg: &ArconConf,
    num_nodes: u64,
    node_id: NodeID,
) -> ArconResult<Box<dyn StateBackend>> {
    let sb = rocks::RocksDb::restore_or_new(cfg, num_nodes, node_id)?;
    Ok(Box::new(sb))
}

#[cfg(feature = "arcon_sled")]
pub mod sled;
#[cfg(feature = "arcon_sled")]
pub use self::sled::Sled;
#[cfg(feature = "arcon_sled")]
pub fn sled(
    cfg: &ArconConf,
    num_nodes: u64,
    node_id: NodeID,
) -> ArconResult<Box<dyn StateBackend>> {
    let sb = sled::Sled::restore_or_new(cfg, num_nodes, node_id)?;
    Ok(Box::new(sb))
}

#[cfg(all(feature = "arcon_faster", target_os = "linux"))]
pub mod faster;
#[cfg(all(feature = "arcon_faster", target_os = "linux"))]
pub use faster::Faster;
#[cfg(all(feature = "arcon_faster", target_os = "linux"))]
pub fn faster(
    cfg: &ArconConf,
    num_nodes: u64,
    node_id: NodeID,
) -> ArconResult<Box<dyn StateBackend>> {
    let sb = faster::Faster::restore_or_new(cfg, num_nodes, node_id)?;
    Ok(Box::new(sb))
}

#[cfg(feature = "metered_state_backend")]
pub mod metered;
#[cfg(feature = "metered_state_backend")]
pub use metered::Metered;
#[cfg(feature = "metered_state_backend")]
pub fn metered_in_memory(
    cfg: &ArconConf,
    num_nodes: u64,
    node_id: NodeID,
) -> ArconResult<Box<dyn StateBackend>> {
    let sb = metered::Metered::<InMemory>::restore_or_new(cfg, num_nodes, node_id)?;
    Ok(Box::new(sb))
}
#[cfg(all(feature = "metered_state_backend", feature = "arcon_rocksdb"))]
pub fn metered_rocks(
    cfg: &ArconConf,
    num_nodes: u64,
    node_id: NodeID,
) -> ArconResult<Box<dyn StateBackend>> {
    let sb = metered::Metered::<rocks::RocksDb>::restore_or_new(cfg, num_nodes, node_id)?;
    Ok(Box::new(sb))
}
#[cfg(all(feature = "metered_state_backend", feature = "arcon_faster"))]
pub fn metered_faster(
    cfg: &ArconConf,
    num_nodes: u64,
    node_id: NodeID,
) -> ArconResult<Box<dyn StateBackend>> {
    let sb = metered::Metered::<faster::Faster>::restore_or_new(cfg, num_nodes, node_id)?;
    Ok(Box::new(sb))
}
#[cfg(all(feature = "metered_state_backend", feature = "arcon_sled"))]
pub fn metered_sled(
    cfg: &ArconConf,
    num_nodes: u64,
    node_id: NodeID,
) -> ArconResult<Box<dyn StateBackend>> {
    let sb = metered::Metered::<sled::Sled>::restore_or_new(cfg, num_nodes, node_id)?;
    Ok(Box::new(sb))
}

#[cfg(test)]
mod test {
    use super::{
        builders::*,
        serialization::{NativeEndianBytesDump, Prost},
        state_types::*,
        *,
    };

    #[test]
    fn test_dynamic_backends() {
        // The downside of this approach is that everything is boxed and you have dynamic dispatch.
        // Every state backend is compatible with this and you don't have to specify which
        // operations you perform on the backend, but the set of supported operations is limited
        // (see the bounds in the `impl_dynamic_builder!` macro invocations.
        fn do_backend_ops(sb: &mut dyn StateBackend) {
            let value_state: Box<dyn ValueState<dyn StateBackend, _, _, _>> =
                sb.new_value_state("value", (), (), NativeEndianBytesDump, Prost);

            let map_state: Box<dyn MapState<dyn StateBackend, _, _, _, _>> =
                sb.new_map_state("map", (), (), NativeEndianBytesDump, Prost);

            value_state.set(sb, 42).unwrap();
            map_state
                .fast_insert(sb, 123, "foobar".to_string())
                .unwrap();

            assert_eq!(value_state.get(sb).unwrap().unwrap(), 42);
            assert_eq!(
                map_state.get(sb, &123).unwrap().unwrap(),
                "foobar".to_string()
            );
        }

        #[cfg(feature = "arcon_rocksdb")]
        {
            let mut test_rocks = rocks::test::TestDb::new();
            let test_rocks: &mut rocks::RocksDb = &mut *test_rocks;
            let dynamic_rocks: &mut dyn StateBackend = test_rocks;
            do_backend_ops(dynamic_rocks);
        }

        #[cfg(feature = "arcon_sled")]
        {
            let mut test_sled = super::sled::test::TestDb::new();
            let test_sled: &mut super::sled::Sled = &mut *test_sled;
            let dynamic_sled: &mut dyn StateBackend = test_sled;
            do_backend_ops(dynamic_sled);
        }

        #[cfg(all(feature = "arcon_faster", target_os = "linux"))]
        {
            let mut test_faster = faster::test::TestDb::new();
            let test_faster: &mut faster::Faster = &mut *test_faster;
            let dynamic_faster: &mut dyn StateBackend = test_faster;
            do_backend_ops(dynamic_faster);
        }

        #[cfg(feature = "metered_state_backend")]
        {
            let mut test_metered =
                metered::Metered::<in_memory::InMemory>::new("test_metered".as_ref()).unwrap();
            let dynamic_metered: &mut dyn StateBackend = &mut test_metered;
            do_backend_ops(dynamic_metered);
        }

        let mut test_in_memory = in_memory::InMemory::new("test_im".as_ref()).unwrap();
        let dynamic_in_memory: &mut dyn StateBackend = &mut test_in_memory;
        do_backend_ops(dynamic_in_memory);
    }

    #[test]
    fn test_generic_backends() {
        #[derive(Copy, Clone, Debug)]
        struct TestMeanAggregator;
        impl Aggregator<u8> for TestMeanAggregator {
            type Accumulator = (u64, u64);
            type Result = u8;

            fn create_accumulator(&self) -> Self::Accumulator {
                (0, 0)
            }

            fn add(&self, acc: &mut Self::Accumulator, value: u8) {
                acc.0 += 1;
                acc.1 += value as u64;
            }

            fn merge_accumulators(
                &self,
                fst: Self::Accumulator,
                snd: Self::Accumulator,
            ) -> Self::Accumulator {
                (fst.0 + snd.0, fst.1 + snd.1)
            }

            fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result {
                // shitty impl, will panic on division by 0 - won't happen in this test tho
                (acc.1 / acc.0) as u8
            }
        }

        // The downside of this approach is that you have to specify all the operations on your
        // state backend as its trait bounds - depending on the bounds you state, not every
        // StateBackend may be compatible.
        fn do_backend_ops<SB: StateBackend + ?Sized, AS>(sb: &mut SB)
        where
            SB: AggregatingStateBuilder<
                u32,
                (),
                u8,
                TestMeanAggregator,
                NativeEndianBytesDump,
                NativeEndianBytesDump,
                Type = AS,
            >,
            // the line below won't be required when chalk will be the default trait solver in rustc
            AS: AggregatingState<SB, u32, (), u8, u8>,
        {
            let mean_state = sb.new_aggregating_state(
                "mean",
                1,
                (),
                TestMeanAggregator,
                NativeEndianBytesDump,
                NativeEndianBytesDump,
            );
            mean_state.clear(sb).unwrap();

            mean_state.append(sb, 1).unwrap();
            mean_state.append(sb, 2).unwrap();
            mean_state.append(sb, 3).unwrap();

            assert_eq!(mean_state.get(sb).unwrap(), 2);
        }

        #[cfg(feature = "arcon_rocksdb")]
        {
            let mut test_rocks = rocks::test::TestDb::new();
            let test_rocks: &mut rocks::RocksDb = &mut *test_rocks;
            do_backend_ops(test_rocks);
        }

        #[cfg(feature = "arcon_sled")]
        {
            let mut test_sled = super::sled::test::TestDb::new();
            let test_sled: &mut super::sled::Sled = &mut *test_sled;
            do_backend_ops(test_sled);
        }

        #[cfg(all(feature = "arcon_faster", target_os = "linux"))]
        {
            let mut test_faster = faster::test::TestDb::new();
            let test_faster: &mut faster::Faster = &mut *test_faster;
            do_backend_ops(test_faster);
        }

        #[cfg(feature = "metered_state_backend")]
        {
            let mut test_metered =
                metered::Metered::<in_memory::InMemory>::new("test_metered".as_ref()).unwrap();
            do_backend_ops(&mut test_metered);
        }

        let mut test_in_memory = in_memory::InMemory::new("test_im".as_ref()).unwrap();
        do_backend_ops(&mut test_in_memory);

        // but you *still* can plug a dynamic state backend there anyway
        let test_dynamic: &mut dyn StateBackend = &mut test_in_memory;
        do_backend_ops(test_dynamic);
    }
}
