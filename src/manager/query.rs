// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::arrow::ImmutableTable;
use arrow::util::pretty::*;
use datafusion::prelude::*;
use kompact::prelude::*;
use prost::*;
use std::{collections::HashSet, fmt};

pub const QUERY_MANAGER_NAME: &str = "query_manager";

pub mod messages {
    use super::*;

    #[derive(Message)]
    pub struct QueryRequest {
        #[prost(string)]
        pub sql_str: String,
    }

    #[derive(Message)]
    pub struct QueryResponse {
        #[prost(string)]
        pub response: String,
    }

    impl Serialisable for QueryResponse {
        fn ser_id(&self) -> u64 {
            100
        }
        fn size_hint(&self) -> Option<usize> {
            Some(self.encoded_len())
        }
        fn serialise(&self, mut buf: &mut dyn BufMut) -> Result<(), SerError> {
            self.encode(&mut buf)
                .map_err(|e| SerError::InvalidData(e.to_string()))?;

            Ok(())
        }
        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }

    impl Deserialiser<QueryResponse> for QueryResponse {
        const SER_ID: SerId = 100;

        fn deserialise(buf: &mut dyn Buf) -> Result<QueryResponse, SerError> {
            Self::decode(buf.chunk()).map_err(|e| SerError::InvalidData(e.to_string()))
        }
    }

    impl Serialisable for QueryRequest {
        fn ser_id(&self) -> u64 {
            101
        }
        fn size_hint(&self) -> Option<usize> {
            Some(self.encoded_len())
        }
        fn serialise(&self, mut buf: &mut dyn BufMut) -> Result<(), SerError> {
            self.encode(&mut buf)
                .map_err(|e| SerError::InvalidData(e.to_string()))?;

            Ok(())
        }
        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }

    impl Deserialiser<QueryRequest> for QueryRequest {
        const SER_ID: SerId = 101;

        fn deserialise(buf: &mut dyn Buf) -> Result<QueryRequest, SerError> {
            Self::decode(buf.chunk()).map_err(|e| SerError::InvalidData(e.to_string()))
        }
    }
}

#[derive(Clone)]
pub struct TableRegistration {
    pub epoch: u64,
    pub table: ImmutableTable,
}

impl fmt::Debug for TableRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableRegistration").finish()
    }
}

pub struct QueryManagerPort;

impl Port for QueryManagerPort {
    type Indication = Never;
    type Request = TableRegistration;
}

#[derive(ComponentDefinition)]
pub struct QueryManager {
    ctx: ComponentContext<Self>,
    pub(crate) manager_port: ProvidedPort<QueryManagerPort>,
    query_ctx: ExecutionContext,
    known_tables: HashSet<String>,
}

impl QueryManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            manager_port: ProvidedPort::uninitialised(),
            query_ctx: ExecutionContext::new(),
            known_tables: HashSet::default(),
        }
    }

    fn handle_registation(&mut self, table_reg: TableRegistration) {
        let table_name = table_reg.table.name();
        trace!(self.ctx.log(), "Registering table {}", table_name);
        let mem_table = table_reg.table.mem_table().unwrap();
        self.query_ctx
            .register_table(&table_name, Box::new(mem_table));

        self.known_tables.insert(table_name);
    }
    fn query(&mut self, request: messages::QueryRequest, destination: ActorPath) -> Handled {
        info!(self.ctx.log(), "Executing Query {:?}", request.sql_str);
        self.spawn_local(move |mut async_self| async move {
            let response_str = match async_self.query_ctx.sql(&request.sql_str) {
                Ok(sql_query) => match sql_query.collect().await {
                    Ok(sql_result) => pretty_format_batches(&sql_result).unwrap(),
                    Err(err) => err.to_string(),
                },
                Err(err) => err.to_string(),
            };

            let response = messages::QueryResponse {
                response: response_str,
            };

            if let Err(err) = destination.tell_serialised(response, &*async_self) {
                error!(
                    async_self.ctx.log(),
                    "Failed to send query result with err {:?}", err
                );
            }

            Handled::Ok
        });

        Handled::Ok
    }
}

impl Actor for QueryManager {
    type Message = messages::QueryRequest;

    fn receive_local(&mut self, _: Self::Message) -> Handled {
        Handled::Ok
    }
    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        match msg
            .data
            .try_deserialise::<messages::QueryRequest, messages::QueryRequest>()
        {
            Ok(request) => self.query(request, msg.sender),
            Err(err) => {
                error!(
                    self.ctx.log(),
                    "Failed to Deserialise QueryRequest with error {:?}", err
                );
                Handled::Ok
            }
        }
    }
}

impl ComponentLifecycle for QueryManager {
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }
    fn on_stop(&mut self) -> Handled {
        Handled::Ok
    }
}

impl Provide<QueryManagerPort> for QueryManager {
    fn handle(&mut self, table_reg: TableRegistration) -> Handled {
        self.handle_registation(table_reg);
        Handled::Ok
    }
}
