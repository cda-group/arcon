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
