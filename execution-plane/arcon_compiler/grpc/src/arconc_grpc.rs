// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_ARCONC_COMPILE: ::grpcio::Method<super::arconc::ArconcRequest, super::arconc::ArconcReply> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/arconc.Arconc/Compile",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct ArconcClient {
    client: ::grpcio::Client,
}

impl ArconcClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        ArconcClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn compile_opt(&self, req: &super::arconc::ArconcRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::arconc::ArconcReply> {
        self.client.unary_call(&METHOD_ARCONC_COMPILE, req, opt)
    }

    pub fn compile(&self, req: &super::arconc::ArconcRequest) -> ::grpcio::Result<super::arconc::ArconcReply> {
        self.compile_opt(req, ::grpcio::CallOption::default())
    }

    pub fn compile_async_opt(&self, req: &super::arconc::ArconcRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::arconc::ArconcReply>> {
        self.client.unary_call_async(&METHOD_ARCONC_COMPILE, req, opt)
    }

    pub fn compile_async(&self, req: &super::arconc::ArconcRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::arconc::ArconcReply>> {
        self.compile_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Arconc {
    fn compile(&mut self, ctx: ::grpcio::RpcContext, req: super::arconc::ArconcRequest, sink: ::grpcio::UnarySink<super::arconc::ArconcReply>);
}

pub fn create_arconc<S: Arconc + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_ARCONC_COMPILE, move |ctx, req, resp| {
        instance.compile(ctx, req, resp)
    });
    builder.build()
}
