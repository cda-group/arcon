use arcon::{
    client::*,
    prelude::{Serialisable, *},
};
use prettytable::{format, Table};
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
        let outgoing_msg = QueryRequestWrapper(msg.request().clone());

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

    let context = String::from("sql[latest]>> ");

    loop {
        match repl.readline(&context) {
            Ok(input) if input == "tables" => {
                match query_sender
                    .ask(QueryRequest { msg: Some(RequestMessage::Tables(0)) })
                    .wait_timeout(Duration::from_millis(10000)) // 10 seconds
                {
                    Ok(table_response) => {
                        let response = table_response.msg.unwrap();
                        match response {
                            ResponseMessage::Tables(t) => {
                                print_tables(t.tables);
                            }
                            _ => panic!("Got unexpected query response"),
                        }
                    }
                    Err(_) => {
                        println!("Timed out while sending query!");
                    }
                }
            }
            Ok(input) if input == "contexts" => {
                unimplemented!();
            }
            Ok(input) if input == "help" => {
                print_help();
            }
            Ok(query) if query.trim_end().ends_with(';') => {
                repl.add_history_entry(query.clone());
                match query_sender
                    .ask(QueryRequest { msg: Some(RequestMessage::Query(query)) })
                    .wait_timeout(Duration::from_millis(10000)) // 10 seconds
                {
                    Ok(query_response) => {
                        let response = query_response.msg.unwrap();
                        match response {
                            ResponseMessage::QueryResult(data) => {
                                let mut table = Table::from_csv_string(&data)?;
                                table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                                table.printstd();
                            }
                            _ => panic!("Got unexpected query response"),
                        }
                    }
                    Err(_) => {
                        println!("Timed out while sending query!");
                    }
                }
            }
            Ok(query) => {
                println!("Did you just try to execute a SQL query without ending it with a ;");
                repl.add_history_entry(query);
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

fn print_help() {
    let table = table!(
        ["Command", "Description"],
        ["tables", "List all available tables"],
        ["contexts", "List all availble contexts"],
        ["set context", "Set the context [Default: latest]"]
    );
    table.printstd();
}
fn print_tables(tables: Vec<arcon::client::Table>) {
    if tables.is_empty() {
        println!("0 tables found!");
    } else {
        let mut pretty_table = Table::new();
        pretty_table.add_row(row!["Name", "Fields"]);
        for table in tables {
            pretty_table.add_row(row![&table.name, &table.fields]);
        }
        pretty_table.printstd();
    }
}
