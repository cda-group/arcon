use crate::error::Error;
use crate::error::ErrorKind::*;
use std::net::SocketAddr;

pub fn parse_socket_addr(addr: &str) -> Result<SocketAddr, std::net::AddrParseError> {
    addr.parse()
}

pub fn addr_split(addr: &str) -> crate::error::Result<(&str, i32)> {
    let mut splitter = addr.splitn(2, ':');
    if let Some(host) = splitter.next() {
        if let Some(port_str) = splitter.next() {
            if let Ok(port) = port_str.parse::<i32>() {
                Ok((host, port))
            } else {
                Err(Error::new(SocketAddrError))
            }
        } else {
            Err(Error::new(SocketAddrError))
        }
    } else {
        Err(Error::new(SocketAddrError))
    }
}
