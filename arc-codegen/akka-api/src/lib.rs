extern crate kompact;
extern crate protobuf;

pub mod messages;
pub mod util;

use crate::messages::messages::*;
use crate::util::*;
use kompact::prelude::BufMut;
use kompact::*;
use protobuf::Message;
use std::net::SocketAddr;

pub struct AkkaConnection {
    pub addr_str: String,
    pub path: ActorPath,
    pub akka_actor_path: String,
}

pub fn kompact_akka_path(ip: String, port: i32, path: String) -> KompactAkkaPath {
    let mut proto = KompactAkkaPath::new();
    proto.set_ip(ip);
    proto.set_port(port);
    proto.set_path(path);
    proto
}

pub fn registration(
    alias: &str,
    self_addr: &SocketAddr,
    conn: &AkkaConnection,
) -> KompactAkkaEnvelope {
    let sock_ip = self_addr.ip().to_string();
    let sock_port = i32::from(self_addr.port());
    let src = kompact_akka_path(sock_ip, sock_port, alias.to_string());

    let (host, port) = addr_split(&conn.addr_str).unwrap();
    let dst = kompact_akka_path(host.to_string(), port, conn.akka_actor_path.clone());

    let mut kompact_registration = KompactRegistration::new();
    kompact_registration.set_id(String::from("job"));
    kompact_registration.set_src(src);
    kompact_registration.set_dst(dst);

    let mut envelope = KompactAkkaEnvelope::new();
    let mut akka_msg = KompactAkkaMsg::new();
    akka_msg.set_kompactRegistration(kompact_registration);
    envelope.set_msg(akka_msg);
    envelope
}

pub struct ProtoSer;

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

impl Deserialiser<KompactAkkaEnvelope> for ProtoSer {
    fn deserialise(buf: &mut Buf) -> Result<KompactAkkaEnvelope, SerError> {
        let parsed = protobuf::parse_from_bytes(buf.bytes())
            .map_err(|err| SerError::InvalidData(err.to_string()))?;
        Ok(parsed)
    }
}
