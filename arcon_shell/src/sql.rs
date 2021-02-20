use arcon::{
    client::*,
    prelude::{Serialisable, *},
};
use prettytable::Table;
use rustyline::{error::ReadlineError, Editor};
use std::time::Duration;

pub const QUERY_SENDER_PATH: &str = "query_sender";

#[derive(Debug)]
pub struct QueryRequestWrapper(QueryRequest);
impl Serialisable for QueryRequestWrapper {
    fn ser_id(&self) -> u64 {
        101
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.0.encoded_len())
    }
    fn serialise(&self, mut buf: &mut dyn BufMut) -> Result<(), SerError> {
        self.0
            .encode(&mut buf)
            .map_err(|e| SerError::InvalidData(e.to_string()))?;

        Ok(())
    }
    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

#[derive(Debug)]
pub struct QueryResponseWrapper(QueryResponse);
impl Deserialiser<QueryResponseWrapper> for QueryResponseWrapper {
    const SER_ID: SerId = 100;

    fn deserialise(buf: &mut dyn Buf) -> Result<QueryResponseWrapper, SerError> {
        let res =
            QueryResponse::decode(buf.chunk()).map_err(|e| SerError::InvalidData(e.to_string()))?;
        Ok(QueryResponseWrapper(res))
    }
}

#[derive(ComponentDefinition)]
pub struct QuerySender {
    ctx: ComponentContext<Self>,
    outstanding_request: Option<Ask<QueryRequest, QueryResponse>>,
    query_manager: ActorPath,
    query_sender_path: ActorPath,
}

impl QuerySender {
    pub fn new(query_manager: ActorPath, query_sender_path: ActorPath) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            outstanding_request: None,
            query_manager,
            query_sender_path,
        }
    }
}

impl Actor for QuerySender {
    type Message = Ask<QueryRequest, QueryResponse>;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        assert!(
            self.outstanding_request.is_none(),
            "One request at the time only"
        );
        let outgoing_msg = QueryRequestWrapper(QueryRequest {
            sql_str: msg.request().sql_str.clone(),
        });

        self.query_manager
            .tell_with_sender(outgoing_msg, self, self.query_sender_path.clone());
        self.outstanding_request = Some(msg);

        Handled::Ok
    }
    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        match msg
            .data
            .try_deserialise::<QueryResponseWrapper, QueryResponseWrapper>()
        {
            Ok(response) => {
                let ask = self.outstanding_request.take().unwrap();
                ask.reply(response.0).expect("failed to respond to Ask");
            }
            Err(err) => {
                error!(self.ctx.log(), "Failed to deserialise with error {:?}", err);
            }
        }

        Handled::Ok
    }
}

impl ComponentLifecycle for QuerySender {
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }
    fn on_stop(&mut self) -> Handled {
        Handled::Ok
    }
}

pub fn repl(
    repl_dir: &str,
    query_sender: ActorRefStrong<Ask<QueryRequest, QueryResponse>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut repl = Editor::<()>::new();
    let history_path = format!("{}/sql_history.txt", repl_dir);
    let _ = repl.load_history(&history_path);

    loop {
        match repl.readline("sql>> ") {
            Ok(input) if input == "tables" => {
                unimplemented!();
            }
            Ok(input) if input == "help" => {
                unimplemented!();
            }
            Ok(query) if query.trim_end().ends_with(';') => {
                repl.add_history_entry(query.clone());
                match query_sender
                    .ask(QueryRequest { sql_str: query })
                    .wait_timeout(Duration::from_millis(10000)) // 10 seconds
                {
                    Ok(query_response) => {
                        let table = Table::from_csv_string(&query_response.response)?;
                        table.printstd();
                    }
                    Err(_) => {
                        println!("Timed out while sending query!");
                    }
                }
            }
            Ok(_) => {
                println!("Did you just try to execute a SQL query without ending it with a ;");
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(_) => {
                break;
            }
        }
    }

    repl.save_history(&history_path)?;

    Ok(())
}
