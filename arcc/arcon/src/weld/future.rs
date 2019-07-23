use crate::data::ArconType;
use crate::error::*;
use crate::weld::module::Module;
use futures::{Async, Future, Poll};
use std::marker::PhantomData;
use std::sync::Arc;
use weld::*;

pub struct WeldFuture<A: ?Sized + ArconType + Send, B> {
    pub module: Arc<Module>,
    input: Arc<A>,
    output: PhantomData<B>,
}

impl<A: ArconType + Send, B: Clone> WeldFuture<A, B> {
    pub fn new(module: Arc<Module>, input: Arc<A>) -> WeldFuture<A, B> {
        WeldFuture {
            module: Arc::clone(&module),
            input: Arc::clone(&input),
            output: PhantomData,
        }
    }

    pub fn default(&mut self, ctx: &mut WeldContext) -> ArconResult<B> {
        let r = self.module.run(&*self.input, ctx)?;
        Ok(r.0)
    }
}

unsafe impl<A: ArconType, B> Send for WeldFuture<A, B> {}

impl<A: ArconType, B: Clone> Future for WeldFuture<A, B> {
    type Item = ArconResult<B>;
    type Error = crate::error::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let ref mut ctx = WeldContext::new(&self.module.conf()).map_err(|e| {
            weld_error!(
                "Failed to create WeldContext with err {}",
                e.message().to_string_lossy().into_owned()
            )
        })?;
        let result: ArconResult<B> = self.default(ctx);
        Ok(Async::Ready(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;

    #[test]
    fn weld_addition_future_test() {
        let code = String::from("|x:i32| 40 + x");
        let module = Arc::new(Module::new(code).unwrap());
        let arg = Arc::new(200);
        let mod_1 = Arc::clone(&module);
        let mod_2 = Arc::clone(&module);

        let task: WeldFuture<i32, i32> = WeldFuture::new(mod_1, arg);

        let future = task
            .map(move |res| res.unwrap() + 100)
            .and_then(|v| {
                let task2: WeldFuture<i32, i32> = WeldFuture::new(mod_2, Arc::new(v));
                task2
            })
            .and_then(|end| {
                assert_eq!(end.unwrap(), 380);
                Ok(())
            })
            .map_err(|_| assert!(false));

        tokio::run(future);
    }

    // TODO: Test WeldFuture with ArconVec...
}
