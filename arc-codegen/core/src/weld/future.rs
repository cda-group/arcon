use crate::weld::module::Module;
use std::sync::Arc;

use crate::error::*;

use std::marker::PhantomData;
use weld::*;

use futures::{Async, Future, Poll};

pub struct WeldFuture<A: ?Sized, B: ?Sized> {
    pub module: Arc<Module>,
    input: Box<A>,
    output: PhantomData<B>,
}

impl<A, B: Clone> WeldFuture<A, B> {
    pub fn new(module: Arc<Module>, input: A) -> WeldFuture<A, B> {
        WeldFuture {
            module: Arc::clone(&module),
            input: Box::new(input),
            output: PhantomData,
        }
    }

    pub fn default(&mut self, ctx: &mut WeldContext) -> Result<B> {
        let r = self.module.run(&*self.input, ctx)?;
        Ok(r.0)
    }
}

unsafe impl<A, B> Send for WeldFuture<A, B> {}

impl<A, B: Clone> Future for WeldFuture<A, B> {
    type Item = Result<B>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let ref mut ctx = WeldContext::new(&self.module.conf()).unwrap();
        let result: Result<B> = self.default(ctx);
        Ok(Async::Ready(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;

    #[test]
    fn weld_task_fut_chain() {
        let id = String::from("addition");
        let code = String::from("|x:i32| 40 + x");
        let priority = 0;
        let module = Arc::new(Module::new(id, code, priority, None).unwrap());
        let arg = 200;
        let mod_1 = Arc::clone(&module);
        let mod_2 = Arc::clone(&module);

        let task: WeldFuture<i32, i32> = WeldFuture::new(mod_1, arg);

        let future = task
            .map(move |res| res.unwrap() + 100)
            .and_then(|v| {
                let task2: WeldFuture<i32, i32> = WeldFuture::new(mod_2, v);
                task2
            })
            .and_then(|end| {
                assert_eq!(end.unwrap(), 380);
                Ok(())
            });
        tokio::run(future);
    }
}
