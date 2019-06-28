use kompact::{ActorPath, ActorRef};

pub mod partitioner;
pub mod task;
pub mod window;

#[derive(Clone, PartialEq)]
pub enum Channel {
    Local(ActorRef),
    Remote(ActorPath),
}
