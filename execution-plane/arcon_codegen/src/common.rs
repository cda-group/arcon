use proc_macro2::{Ident, Span, TokenStream};

pub fn verify_and_start(name: &Ident, sys_name: &str) -> TokenStream {
    let system = Ident::new(sys_name, Span::call_site());
    quote! {
        reg.wait_timeout(std::time::Duration::from_millis(1000))
            .expect("Component never registered!")
            .expect("Component failed to register!");

        #system.start(&#name);
    }
}
