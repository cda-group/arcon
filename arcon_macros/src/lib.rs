// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! The arcon_macros crate contains macros used by [arcon].

#![recursion_limit = "128"]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;

mod arcon;
#[cfg(feature = "arcon_arrow")]
mod arrow;
mod decoder;
mod state;

/// Derive macro for declaring an ArconType
///
/// ```rust,ignore
/// #[derive(Arcon)]
/// #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1)]
/// pub struct ArconStruct {
///     #[prost(uint32, tag = "1")]
///     pub id: u32,
/// }
/// ```
#[proc_macro_derive(Arcon, attributes(arcon))]
pub fn arcon(input: TokenStream) -> TokenStream {
    arcon::derive_arcon(input)
}

/// Derive macro to declare ArconState
///
/// ```rust,ignore
/// #[derive(ArconState)]
/// pub struct StreamingState<B: Backend> {
///   values: LazyValue<u64, B>,
/// }
/// ```
#[proc_macro_derive(ArconState, attributes(ephemeral, table))]
pub fn state(input: TokenStream) -> TokenStream {
    state::derive_state(input)
}

/// Derive macro for declaring a type a type that supports the Arrow data format the Arrow data format
#[cfg(feature = "arcon_arrow")]
#[proc_macro_derive(Arrow)]
pub fn arrow(input: TokenStream) -> TokenStream {
    arrow::derive_arrow(input)
}

/// Implements [std::str::FromStr] for a struct using a delimiter
///
/// If no delimiter is specified, then `,` is chosen as default.
/// Note: All inner fields of the struct need to implement [std::str::FromStr] for the macro to work.
#[proc_macro_attribute]
pub fn arcon_decoder(delimiter: TokenStream, input: TokenStream) -> TokenStream {
    decoder::derive_decoder(delimiter, input)
}
