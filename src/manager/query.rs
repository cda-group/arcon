// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::table::ImmutableTable;
use arrow::{datatypes::Schema, util::pretty::*};
use datafusion::prelude::*;
use kompact::prelude::*;
use prost::*;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
    time::Instant,
};

pub const QUERY_MANAGER_NAME: &str = "query_manager";

pub mod messages {
    use super::*;

    #[derive(Clone, Message)]
    pub struct Table {
        #[prost(string)]
        pub name: String,
        #[prost(string)]
        pub fields: String,
    }
    #[derive(Clone, Message)]
    pub struct Tables {
        #[prost(message, repeated)]
        pub tables: Vec<Table>,
    }

    #[derive(Clone, Message)]
    pub struct Context {
        #[prost(uint64)]
        pub epoch: u64,
        #[prost(bool)]
        pub committed: bool,
        #[prost(uint64)]
        pub registration_timestamp: u64,
    }

    #[derive(Clone, Message)]
    pub struct Contexts {
        #[prost(message, repeated)]
        pub contexts: Vec<Context>,
    }

    #[derive(Clone, Message)]
    pub struct ExecResult {
        #[prost(string)]
        pub response: String,
        #[prost(float)]
        pub runtime: f32,
        #[prost(uint64)]
        pub rows: u64,
    }

    #[derive(Clone, Message)]
    pub struct ExecRequest {
        #[prost(string)]
        pub sql_str: String,
        #[prost(uint64)]
        pub epoch: u64,
    }

    #[derive(Clone, Message)]
    pub struct QueryRequest {
        #[prost(oneof = "RequestMessage", tags = "1, 2, 3")]
        pub msg: Option<RequestMessage>,
    }

    #[derive(Clone, Oneof)]
    pub enum RequestMessage {
        #[prost(message, tag = "1")]
        Tables(u64),
        #[prost(message, tag = "2")]
        Query(ExecRequest),
        #[prost(message, tag = "3")]
        Contexts(u64),
    }

    #[derive(Clone, Oneof)]
    pub enum ResponseMessage {
        #[prost(message, tag = "1")]
        QueryResult(ExecResult),
        #[prost(message, tag = "2")]
        Tables(Tables),
        #[prost(message, tag = "3")]
        Contexts(Contexts),
    }

    #[derive(Clone, Message)]
    pub struct QueryResponse {
        #[prost(oneof = "ResponseMessage", tags = "1, 2, 3")]
        pub msg: Option<ResponseMessage>,
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
pub struct QueryContext {
    pub ctx: ExecutionContext,
    pub committed: bool,
    pub registration_timestamp: u64,
}

#[derive(Debug, Clone)]
pub enum QueryManagerMsg {
    TableRegistration(TableRegistration),
    EpochCommit(u64),
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
    type Request = QueryManagerMsg;
}

#[derive(ComponentDefinition)]
pub struct QueryManager {
    ctx: ComponentContext<Self>,
    pub(crate) manager_port: ProvidedPort<QueryManagerPort>,
    known_tables: HashSet<(String, String)>,
    known_contexts: HashMap<u64, QueryContext>,
    committed_epochs: HashSet<u64>,
}

impl QueryManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            manager_port: ProvidedPort::uninitialised(),
            known_tables: HashSet::default(),
            known_contexts: HashMap::default(),
            committed_epochs: HashSet::default(),
        }
    }

    #[inline]
    fn new_query_ctx() -> QueryContext {
        QueryContext {
            committed: false,
            ctx: ExecutionContext::new(),
            registration_timestamp: crate::util::get_system_time(),
        }
    }

    #[inline]
    fn handle_epoch_commit(&mut self, epoch: u64) {
        if let Some(context) = self.known_contexts.get_mut(&epoch) {
            // if the context has been registred already, then update status
            context.committed = true;
        } else {
            // if context has not yet been created due to lag in message sending,
            // add epoch to committed epochs set and handle at registration
            self.committed_epochs.insert(epoch);
        }
    }

    #[inline]
    fn handle_registation(&mut self, table_reg: TableRegistration) {
        let table_name = table_reg.table.name();
        let schema = table_reg.table.schema();
        let epoch = table_reg.epoch;

        let query_ctx = self
            .known_contexts
            .entry(epoch)
            .or_insert_with(Self::new_query_ctx);

        if self.committed_epochs.take(&epoch).is_some() {
            // If this epoch is found in our committed epochs set,
            // then remove and update query_ctx status...
            query_ctx.committed = true;
        }

        let mem_table = table_reg.table.mem_table().unwrap();

        query_ctx
            .ctx
            .register_table(&table_name, Arc::new(mem_table));

        trace!(self.ctx.log(), "Registering table {}", table_name);

        let fields_string = Self::construct_fields_str(schema);
        self.known_tables.insert((table_name, fields_string));
    }

    fn construct_fields_str(schema: Arc<Schema>) -> String {
        let mut data = String::from("");
        for field in schema.fields() {
            data += &format!("{}:{} ", field.name(), field.data_type().to_string());
        }
        data.trim_end().to_string()
    }

    fn tables(&mut self, destination: ActorPath) -> Handled {
        let mut tables = Vec::new();
        for (table, fields) in &self.known_tables {
            let table = messages::Table {
                name: table.to_string(),
                fields: fields.to_string(),
            };
            tables.push(table);
        }

        let response = messages::QueryResponse {
            msg: Some(messages::ResponseMessage::Tables(messages::Tables {
                tables,
            })),
        };

        if let Err(err) = destination.tell_serialised(response, self) {
            error!(self.ctx.log(), "Failed to send tables with err {:?}", err);
        }

        Handled::Ok
    }

    fn contexts(&mut self, destination: ActorPath) -> Handled {
        let mut contexts = Vec::new();
        for (epoch, query_ctx) in &self.known_contexts {
            let context = messages::Context {
                epoch: *epoch,
                committed: query_ctx.committed,
                registration_timestamp: query_ctx.registration_timestamp,
            };
            contexts.push(context);
        }
        let response = messages::QueryResponse {
            msg: Some(messages::ResponseMessage::Contexts(messages::Contexts {
                contexts,
            })),
        };

        if let Err(err) = destination.tell_serialised(response, self) {
            error!(self.ctx.log(), "Failed to send contexts with err {:?}", err);
        }

        Handled::Ok
    }

    #[tokio::main]
    async fn sql_query(&mut self, exec_request: messages::ExecRequest) -> messages::ExecResult {
        let epoch = exec_request.epoch;
        if !self.known_contexts.contains_key(&epoch) {
            return messages::ExecResult {
                response: format!("Could not find query context for epoch {}", epoch),
                runtime: 0.0,
                rows: 0,
            };
        }
        info!(self.ctx.log(), "Executing Query Using epoch {}", epoch);
        let query_ctx = self.known_contexts.get_mut(&epoch).unwrap();

        let now = Instant::now();
        match query_ctx.ctx.sql(&exec_request.sql_str) {
            Ok(sql_query) => match sql_query.collect().await {
                Ok(sql_result) => messages::ExecResult {
                    response: pretty_format_batches(&sql_result).unwrap(),
                    runtime: now.elapsed().as_secs_f32(),
                    rows: sql_result.iter().map(|b| b.num_rows()).sum::<usize>() as u64,
                },
                Err(err) => messages::ExecResult {
                    response: err.to_string(),
                    runtime: now.elapsed().as_secs_f32(),
                    rows: 0,
                },
            },
            Err(err) => messages::ExecResult {
                response: err.to_string(),
                runtime: now.elapsed().as_secs_f32(),
                rows: 0,
            },
        }
    }

    fn query(&mut self, exec_request: messages::ExecRequest, destination: ActorPath) -> Handled {
        info!(self.ctx.log(), "Executing Query {:?}", exec_request.sql_str);
        self.spawn_local(move |mut async_self| async move {
            let exec_result = async_self.sql_query(exec_request);

            let response = messages::QueryResponse {
                msg: Some(messages::ResponseMessage::QueryResult(exec_result)),
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
            Ok(request) => {
                let request_msg = request.msg.unwrap();
                match request_msg {
                    messages::RequestMessage::Tables(_) => self.tables(msg.sender),
                    messages::RequestMessage::Query(exec_request) => {
                        self.query(exec_request, msg.sender)
                    }
                    messages::RequestMessage::Contexts(_) => self.contexts(msg.sender),
                }
            }
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
    fn handle(&mut self, msg: QueryManagerMsg) -> Handled {
        match msg {
            QueryManagerMsg::TableRegistration(reg) => self.handle_registation(reg),
            QueryManagerMsg::EpochCommit(epoch) => self.handle_epoch_commit(epoch),
        }
        Handled::Ok
    }
}
