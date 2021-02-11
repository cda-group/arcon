// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use kompact::prelude::*;

pub const ENDPOINT_MANAGER_NAME: &str = "endpoint_manager";

#[derive(ComponentDefinition)]
pub struct EndpointManager {
    ctx: ComponentContext<Self>,
}

impl EndpointManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
        }
    }
}

impl Actor for EndpointManager {
    type Message = Never;

    fn receive_local(&mut self, _: Self::Message) -> Handled {
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unimplemented!();
    }
}

impl ComponentLifecycle for EndpointManager {
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }
    fn on_stop(&mut self) -> Handled {
        Handled::Ok
    }
}
