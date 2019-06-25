use kompact::{ActorRef, ActorPath};
use std::hash::{Hash, Hasher};

pub mod hash_partitioner;

#[derive(Clone)]
pub enum Task {
    Local(ActorRef),
    Remote(ActorPath),
}

pub trait Partitioner<A: Hash> {
    fn get_task(&mut self, input: A) -> Option<Task>;
    fn remove_task(&mut self, task: Task);
    fn add_task(&mut self, task: Task);
}
