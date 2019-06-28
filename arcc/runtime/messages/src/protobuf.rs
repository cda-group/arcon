extern crate protobuf;

pub use protobuf::messages::*;
pub use protobuf::ProtoSer;

pub fn create_window_trigger() -> WindowMessage {
    let mut msg = WindowMessage::new();
    let trigger = WindowTrigger::new();
    msg.set_trigger(trigger);
    msg
}

pub fn create_window_complete() -> WindowMessage {
    let mut msg = WindowMessage::new();
    let complete = WindowComplete::new();
    msg.set_complete(complete);
    msg
}

pub fn create_windowed_element(element: Element) -> WindowMessage {
    let mut msg = WindowMessage::new();
    msg.set_element(element);
    msg
}

pub fn create_element(data: Vec<u8>, ts: u64) -> StreamTaskMessage {
    let mut msg = StreamTaskMessage::new();
    let mut element= Element::new();
    element.set_data(data);
    element.set_timestamp(ts);

    msg.set_element(element);
    msg
}

pub fn create_keyed_element(data: Vec<u8>, ts: u64, key: u64) -> StreamTaskMessage {
    let mut msg = StreamTaskMessage::new();
    let mut keyed = KeyedElement::new();
    keyed.set_data(data);
    keyed.set_key(key);
    keyed.set_timestamp(ts);

    msg.set_keyed_element(keyed);
    msg
}
