use crate::data::{ArconEvent, ArconType};
use crate::error::{ArconResult, Error};
use crate::reportable_error;
use crate::stream::channel::strategy::{send, ChannelStrategy};
use kompact::prelude::{ComponentDefinition, SerError};

// Common helper function for adding events to a ChannelStrategy and possibly
// dispatching Arcon messages.
#[inline]
pub fn add_outgoing_event<OUT: ArconType>(
    event: ArconEvent<OUT>,
    strategy: &mut ChannelStrategy<OUT>,
    cd: &impl ComponentDefinition,
) -> ArconResult<()> {
    for (channel, msg) in strategy.push(event) {
        match send(&channel, msg, cd) {
            Err(SerError::BufferError(msg)) | Err(SerError::NoBuffersAvailable(msg)) => {
                // TODO: actually handle it
                return Err(Error::Unsupported { msg });
            }
            Err(SerError::InvalidData(msg))
            | Err(SerError::InvalidType(msg))
            | Err(SerError::Unknown(msg)) => {
                return reportable_error!("{}", msg);
            }
            Err(SerError::NoClone) => {
                return reportable_error!("Got Kompact's SerError::NoClone");
            }
            Ok(_) => (),
        }
    }
    Ok(())
}
