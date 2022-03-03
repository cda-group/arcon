#![allow(clippy::needless_doctest_main)]

//! The arcon_macros crate contains macros used by arcon.

#![recursion_limit = "128"]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;

mod app;
mod arcon;
mod arrow;
mod decoder;
mod proto;
mod state;

/// Derive macro for declaring an ArconType
///
/// ArconType has 3 required attributes:
///
/// *   ``unsafe_ser_id``: an identifier used for identifying if an ArconType was sent over the wire using unsafe flight mode
/// *   ``reliable_ser_id``: an identifier used for identifying if an ArconType was sent over the wire using reliable flight mode
/// *   ``version``: an identifier used for schema evolution and deployment updates
///
/// ```rust
/// use arcon::prelude::*;
///
/// #[arcon::proto]
/// #[derive(Arcon, Clone)]
/// #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1)]
/// pub struct ArconStruct {
///     pub id: u32,
///     pub timestamp: u64,
/// }
/// ```
#[proc_macro_derive(Arcon, attributes(arcon))]
pub fn arcon(input: TokenStream) -> TokenStream {
    arcon::derive_arcon(input)
}

/// A macro that helps set up and run an [Application](../arcon/application/struct.Application.html).
///
/// This macro is meant to simplify the creation of
/// arcon applications that do not require complex configuration. For more flexibility,
/// have a look at [ApplicationBulder](../arcon/application/builder/struct.ApplicationBuilder.html).
///
/// ## Usage
///
/// ### With no arguments
///
/// ```no_run
/// #[arcon::app]
/// fn main() {
///  (0..100u64)
///     .to_stream(|conf| conf.set_arcon_time(ArconTime::Process))
///     .map(|x| x * 10)
///     .print()
/// }
/// ```
///
/// Expands to the following
///
/// ```no_run
/// fn main() {
///    use arcon::prelude::*;
///    let mut builder = (0..100u64)
///     .to_stream(|conf| conf.set_arcon_time(ArconTime::Process))
///     .map(|x| x * 10)
///     .print()
///     .builder();
///
///    builder
///     .build()
///     .run_and_block();
/// }
/// ```
#[proc_macro_attribute]
pub fn app(delimiter: TokenStream, input: TokenStream) -> TokenStream {
    app::main(delimiter, input)
}

/// Derive macro for declaring an ArconState
///
/// ```rust
/// use arcon::prelude::*;
///
/// #[derive(ArconState)]
/// pub struct StreamingState<B: Backend> {
///   values: LazyValue<u64, B>,
/// }
/// ```
#[proc_macro_derive(ArconState, attributes(ephemeral, table))]
pub fn state(input: TokenStream) -> TokenStream {
    state::derive_state(input)
}

/// Derive macro for declaring an Arrow convertable type within the Arcon runtime
///
/// ```rust
/// use arcon::prelude::*;
///
/// #[derive(Arrow)]
/// pub struct ArrowStruct {
///     pub id: u32,
///     pub name: String,
/// }
/// ```
#[proc_macro_derive(Arrow)]
pub fn arrow(input: TokenStream) -> TokenStream {
    arrow::derive_arrow(input)
}

/// Implements [std::str::FromStr] for a struct using a delimiter
///
/// If no delimiter is specified, then `,` is chosen as default.
/// Note: All inner fields of the struct need to implement [std::str::FromStr] for the macro to work.
#[proc_macro_attribute]
pub fn decoder(delimiter: TokenStream, input: TokenStream) -> TokenStream {
    decoder::derive_decoder(delimiter, input)
}

/// Helper macro to make a struct or enum prost-compatible without the need for annotations.
///
/// ```rust
/// use arcon::prelude::*;
///
/// #[arcon::proto]
/// struct Event {
///     s: String,
///     p: Point,
/// }
/// #[arcon::proto]
/// struct Point {
///     x: i32,
///     y: i32,
/// }
/// ```
#[proc_macro_attribute]
pub fn proto(_: TokenStream, input: TokenStream) -> TokenStream {
    proto::derive_proto(input)
}
