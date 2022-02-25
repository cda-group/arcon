use super::build::BuildExt;
use crate::{
    application::assembled::AssembledApplication, data::ArconType, dataflow::stream::Stream,
};

/// Extension trait for debugging
pub trait DebugExt: BuildExt {
    /// Build an application and make sure outputs is printed to the console
    ///
    /// # Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .map(|x| x + 10);
    ///
    /// let app = stream.to_console();
    /// ```
    fn to_console(self) -> AssembledApplication;
}

impl<T: ArconType> DebugExt for Stream<T> {
    fn to_console(mut self) -> AssembledApplication {
        self.ctx.console_output = true;
        self.build()
    }
}
