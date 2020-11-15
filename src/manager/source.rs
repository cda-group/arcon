// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::stream::source::ArconSource;
use kompact::{component::AbstractComponent, prelude::*};
use std::sync::Arc;

/// Component that manages a set of Arcon sources
#[derive(ComponentDefinition)]
pub(crate) struct SourceManager {
    /// Component Context
    ctx: ComponentContext<Self>,
    /// Vector of source components
    ///
    /// May contain more than 1 component if the source supports parallelism
    pub(crate) sources: Vec<Arc<dyn AbstractComponent<Message = ArconSource>>>,
}

impl SourceManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            sources: Vec::new(),
        }
    }
}

ignore_lifecycle!(SourceManager);

impl Actor for SourceManager {
    type Message = ArconSource;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            ArconSource::Epoch(_) => {
                for source in &self.sources {
                    source.actor_ref().tell(msg.clone());
                }
            }
            ArconSource::Start => {
                for source in &self.sources {
                    source.actor_ref().tell(msg.clone());
                }
            }
        }
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unimplemented!();
    }
}
