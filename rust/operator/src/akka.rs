use crate::akka_api::messages::*;
use crate::akka_api::util::*;
use crate::util::addr_split;
use bytes::{Buf, BufMut};
use kompact::*;
use protobuf::Message;
use std::net::SocketAddr;

pub struct AkkaConnection {
    pub addr_str: String,
    pub path: ActorPath,
    pub akka_actor_path: String,
}

fn kompact_akka_path(ip: String, port: i32, path: String) -> KompactAkkaPath {
    let mut proto = KompactAkkaPath::new();
    proto.set_ip(ip);
    proto.set_port(port);
    proto.set_path(path);
    proto
}

fn akka_registration(
    alias: &str,
    self_addr: &SocketAddr,
    conn: &AkkaConnection,
) -> KompactAkkaEnvelope {
    let sock_ip = self_addr.ip().to_string();
    let sock_port = i32::from(self_addr.port());
    let src = kompact_akka_path(sock_ip, sock_port, alias.to_string());

    let (host, port) = addr_split(&conn.addr_str).unwrap();
    let dst = kompact_akka_path(host.to_string(), port, conn.akka_actor_path.clone());

    let mut executor_registration = ExecutorRegistration::new();
    executor_registration.set_jobId(String::from("job"));
    executor_registration.set_src(src);
    executor_registration.set_dst(dst);

    let mut envelope = KompactAkkaEnvelope::new();
    let mut akka_msg = KompactAkkaMsg::new();
    akka_msg.set_executorRegistration(executor_registration);
    envelope.set_msg(akka_msg);
    envelope
}

#[derive(ComponentDefinition)]
struct AkkaActor {
    ctx: ComponentContext<AkkaActor>,
    self_addr: SocketAddr,
    conn: AkkaConnection,
    alias: String,
}

impl AkkaActor {
    pub fn new(self_addr: SocketAddr, conn: AkkaConnection, alias: String) -> AkkaActor {
        AkkaActor {
            ctx: ComponentContext::new(),
            self_addr,
            conn,
            alias,
        }
    }
}

impl Serialisable for KompactAkkaEnvelope {
    fn serid(&self) -> u64 {
        serialisation_ids::PBUF
    }
    fn size_hint(&self) -> Option<usize> {
        if let Ok(bytes) = self.write_to_bytes() {
            Some(bytes.len())
        } else {
            None
        }
    }
    fn serialise(&self, buf: &mut BufMut) -> Result<(), SerError> {
        let bytes = self
            .write_to_bytes()
            .map_err(|err| SerError::InvalidData(err.to_string()))?;
        buf.put_slice(&bytes);
        Ok(())
    }
    fn local(self: Box<Self>) -> Result<Box<Any + Send>, Box<Serialisable>> {
        Ok(self)
    }
}

pub struct ProtoSer;

impl Deserialiser<KompactAkkaEnvelope> for ProtoSer {
    fn deserialise(buf: &mut Buf) -> Result<KompactAkkaEnvelope, SerError> {
        let parsed = protobuf::parse_from_bytes(buf.bytes())
            .map_err(|err| SerError::InvalidData(err.to_string()))?;
        Ok(parsed)
    }
}

impl Provide<ControlPort> for AkkaActor {
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            // Register at Akka
            let envelope = akka_registration(&self.alias, &self.self_addr, &self.conn);
            self.conn.path.tell(envelope, self);
        }
    }
}

impl Actor for AkkaActor {
    fn receive_local(&mut self, _sender: ActorRef, _msg: Box<Any>) {}
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r: Result<KompactAkkaEnvelope, SerError> = ProtoSer::deserialise(buf);
            if let Ok(data) = r {
                let payload = data.msg.unwrap().payload.unwrap();
                match payload {
                    KompactAkkaMsg_oneof_payload::hello(v) => {
                        println!("{}", v.hey);
                        let mut reply = KompactAkkaMsg::new();
                        let mut hey = Hello::new();
                        hey.set_hey(String::from("hello from rust"));
                        reply.set_hello(hey);
                        let mut envelope = KompactAkkaEnvelope::new();
                        envelope.set_msg(reply);
                        sender.tell(envelope, self)
                    }
                    KompactAkkaMsg_oneof_payload::ask(ask) => {
                        // Refactor
                        let mut hey = Hello::new();
                        hey.set_hey(String::from("my_ask_reply"));
                        let mut response = KompactAkkaMsg::new();
                        response.set_hello(hey);

                        let mut ask_reply = AskReply::new();
                        ask_reply.set_askActor(ask.askActor);
                        ask_reply.set_msg(response);

                        let mut reply_msg = KompactAkkaMsg::new();
                        reply_msg.set_askReply(ask_reply);

                        let mut envelope = KompactAkkaEnvelope::new();
                        envelope.set_msg(reply_msg);
                        sender.tell(envelope, self)
                    }
                    KompactAkkaMsg_oneof_payload::executorRegistration(_) => {}
                    KompactAkkaMsg_oneof_payload::askReply(_) => {}
                }
            }
        } else {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operational_plane::util::*;
    use kompact::default_components::DeadletterBox;

    #[test]
    fn setup() {
        let self_socket_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), 0);
        let mut cfg = KompicsConfig::new();

        cfg.system_components(DeadletterBox::new, move || {
            let net_config = NetworkConfig::new(self_socket_addr);
            NetworkDispatcher::with_config(net_config)
        });

        let system = KompicsSystem::new(cfg);

        // TODO: handle localhost also
        let state_manager_proxy = String::from("127.0.0.1:2000");
        let state_master_actor_path = String::from("some_path");
        let sm_socket_addr = parse_socket_addr(&state_manager_proxy).unwrap();
        let sm_path = ActorPath::Named(NamedPath::with_socket(
            Transport::TCP,
            sm_socket_addr,
            vec!["state_manager_proxy".into()],
        ));

        let sm_connection: AkkaConnection = AkkaConnection {
            akka_actor_path: state_master_actor_path.to_string(),
            addr_str: state_manager_proxy.to_string(),
            path: sm_path,
        };

        let akka_actor = system.create_and_register(move || {
            AkkaActor::new(self_socket_addr, sm_connection, String::from("test_actor"))
        });

        system.start(&akka_actor);
        // Yeah, probably look to assert something here...
    }
}
