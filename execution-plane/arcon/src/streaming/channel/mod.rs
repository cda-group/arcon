use kompact::{ActorPath, ActorRef};

pub mod strategy;

#[derive(Clone)]
pub enum Channel {
    Local(ActorRef),
    Remote(ActorPath),
}
