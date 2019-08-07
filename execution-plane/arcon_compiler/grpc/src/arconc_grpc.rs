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

const METHOD_COMPILER_COMPILE: ::grpcio::Method<super::arconc::CompileRequest, super::arconc::CompileReply> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/arconc.Compiler/Compile",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct CompilerClient {
    client: ::grpcio::Client,
}

impl CompilerClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        CompilerClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn compile_opt(&self, req: &super::arconc::CompileRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::arconc::CompileReply> {
        self.client.unary_call(&METHOD_COMPILER_COMPILE, req, opt)
    }

    pub fn compile(&self, req: &super::arconc::CompileRequest) -> ::grpcio::Result<super::arconc::CompileReply> {
        self.compile_opt(req, ::grpcio::CallOption::default())
    }

    pub fn compile_async_opt(&self, req: &super::arconc::CompileRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::arconc::CompileReply>> {
        self.client.unary_call_async(&METHOD_COMPILER_COMPILE, req, opt)
    }

    pub fn compile_async(&self, req: &super::arconc::CompileRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::arconc::CompileReply>> {
        self.compile_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Compiler {
    fn compile(&mut self, ctx: ::grpcio::RpcContext, req: super::arconc::CompileRequest, sink: ::grpcio::UnarySink<super::arconc::CompileReply>);
}

pub fn create_compiler<S: Compiler + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_COMPILER_COMPILE, move |ctx, req, resp| {
        instance.compile(ctx, req, resp)
    });
    builder.build()
}
