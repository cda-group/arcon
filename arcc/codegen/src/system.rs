use proc_macro2::TokenStream;

pub fn system(
    addr: &str,
    kompact_extra_options: Option<TokenStream>,
    kompact_connections: Option<TokenStream>,
    termination: Option<TokenStream>,
) -> TokenStream {
    quote! {
        let mut cfg = KompactConfig::new();
        let sock_addr = #addr.parse().expect("Failed to parse SocketAddr");
        cfg.system_components(DeadletterBox::new, NetworkConfig::new(sock_addr).build());

        #kompact_extra_options

        let system = KompactSystem::new(cfg).expect("Failed to create KompactSystem");

        // Connect Components, Create ActorPaths, Create Tasks
        // Connect it all

        #kompact_connections

        // enable optional termination for testing
        #termination 
    }
}

#[cfg(test)]
mod tests {

}