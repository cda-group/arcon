use kompact::{ActorPath, ActorRef};
use std::hash::{Hash, Hasher};

pub mod hash;

#[derive(Clone)]
pub enum Task {
    Local(ActorRef),
    Remote(ActorPath),
}

pub trait Partitioner<A: Hash> {
    fn get_task(&mut self, input: A) -> Option<Task>;
    fn get_task_by_key(&mut self, key: u64) -> Option<Task>;
    fn remove_task(&mut self, task: Task);
    fn add_task(&mut self, task: Task);
}
