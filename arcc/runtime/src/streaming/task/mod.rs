pub mod stateless;
pub mod manager;
pub mod partitioner;

use kompact::ActorPath;

pub struct Destination {
    pub path: ActorPath,
    pub task_id: String,
}

impl Destination {
    pub fn new(path: ActorPath, task_id: String) -> Destination {
        Destination { path, task_id }
    }
}
