use std::net::SocketAddr;

pub fn parse_socket_addr(addr: &str) -> Result<SocketAddr, std::net::AddrParseError> {
    addr.parse()
}

// TODO: refactor
pub fn addr_split(addr: &str) -> Option<(&str, i32)> {
    let mut splitter = addr.splitn(2, ':');
    if let Some(host) = splitter.next() {
        if let Some(port_str) = splitter.next() {
            if let Ok(port) = port_str.parse::<i32>() {
                Some((host, port))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}
