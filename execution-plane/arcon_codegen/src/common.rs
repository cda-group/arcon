use proc_macro2::{Ident, Span, TokenStream};

pub fn verify_and_start(name: &Ident, sys_name: &str) -> TokenStream {
    let system = Ident::new(sys_name, Span::call_site());
    quote! {
        reg.wait_expect(std::time::Duration::from_millis(1000),
                        "failed to register componenet");
        //reg.wait_timeout(std::time::Duration::from_millis(1000))
         //   .expect("Component failed to register!");

        #system.start(&#name);
    }
}
