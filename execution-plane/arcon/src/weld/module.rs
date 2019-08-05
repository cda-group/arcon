use crate::error::*;
use std::time::Instant;
use weld_core::*;

pub use weld_core::WeldContext;

/// `ModuleRun` is a holder for the result of an
/// execution and its elapsed time in nanoseconds
pub type ModuleRun<A> = (A, u64);

/// `Module` is a wrapper for `WeldModule`
pub struct Module {
    code: String,
    module: WeldModule,
    conf: WeldConf,
    id: Option<String>,
}

impl Module {
    /// Creates a new `Module`
    ///
    /// ```rust
    /// use arcon::weld::Module;
    ///
    /// let code = String::from("|x: i32| x + 5");
    /// let module = Module::new(code).unwrap();
    /// ```
    pub fn new(code: String) -> ArconResult<Module> {
        let conf = WeldConf::new();
        let module = Module::compile_module(code.clone(), &conf)?;

        Ok(Module {
            code,
            module,
            conf,
            id: None,
        })
    }

    /// Creates a new `Module` with threads
    ///
    /// NOTE: Weld does currently not have a multi-threaded backend.
    ///
    /// ```rust
    /// use arcon::weld::Module;
    ///
    /// let code = String::from("|x: i32| x + 5");
    /// let module = Module::with_threads(code, 4).unwrap();
    /// ```
    pub fn with_threads(code: String, threads: i32) -> ArconResult<Module> {
        let mut conf = WeldConf::new();
        let thread_str = format!("{}", threads);
        conf.set("weld.threads", thread_str);
        let module = Module::compile_module(code.clone(), &conf)?;

        Ok(Module {
            code,
            module,
            conf,
            id: None,
        })
    }

    fn compile_module(code: String, conf: &WeldConf) -> ArconResult<WeldModule> {
        WeldModule::compile(code.clone(), &conf).map_err(|e| {
            weld_error!(
                "Failed to compile WeldModule with err {}",
                e.message().to_string_lossy().into_owned()
            )
        })
    }
    pub fn set_id(&mut self, id: String) {
        if self.id.is_none() {
            self.id = Some(id);
        }
    }

    pub fn get_code(&mut self) -> String {
        self.code.clone()
    }

    pub fn conf(&self) -> &WeldConf {
        &self.conf
    }

    pub fn return_type(&self) -> weld::ast::Type {
        self.module.return_type()
    }

    pub fn param_types(&self) -> Vec<weld::ast::Type> {
        self.module.param_types()
    }

    /// Executes a `Module` with given input
    ///
    /// ```rust
    /// use arcon::weld::{Module, ModuleRun, WeldContext};
    ///
    /// let code = String::from("|x: i32| x + 5");
    /// let module = Module::new(code).unwrap();
    /// let ref input = 10;
    /// let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
    /// let result: ModuleRun<i32> = module.run(input, ctx).unwrap();
    /// let data = result.0;
    /// let time_ns = result.1;
    /// assert_eq!(data, 15);
    /// assert!(time_ns > 0);
    /// ```
    pub fn run<I, O: Clone>(&self, ptr: &I, ctx: &mut WeldContext) -> ArconResult<ModuleRun<O>> {
        let ref arg = WeldValue::new_from_data(ptr as *const _ as Data);
        let (result, time_ns) = unsafe {
            let (res, ns) = self.mat_runner(ctx, arg)?;
            let data = res.data() as *const O;
            ((*data).clone(), ns)
        };
        Ok((result, time_ns))
    }

    unsafe fn mat_runner(
        &self,
        ctx: &mut WeldContext,
        arg: &WeldValue,
    ) -> ArconResult<(WeldValue, u64)> {
        let start = Instant::now();
        let res = self.module.run(ctx, arg).map_err(|e| {
            weld_error!(
                "Failed to run WeldModule with err {}",
                e.message().to_string_lossy().into_owned()
            )
        })?;
        let elapsed = start.elapsed();
        let ns: u64 = elapsed.as_secs() * 1_000_000_000 + u64::from(elapsed.subsec_nanos());
        Ok((res, ns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ArconVec;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn addition_module() {
        let code = String::from("|x:i32| 40 + x");
        let module = Module::new(code).unwrap();
        let ref mut ctx = WeldContext::new(&module.conf).unwrap();
        assert_eq!(ctx.memory_usage(), 0);
        let arg: i32 = 10;
        let res: ModuleRun<i32> = module.run(&arg, ctx).unwrap();
        let data = res.0;
        let time_ns = res.1;
        assert_eq!(data, 50);
        assert!(ctx.memory_usage() > 0);
        assert!(time_ns > 0);
    }

    #[test]
    fn vec_len_module() {
        let code = String::from("|x:vec[i32]| len(x)");
        let input_vec: Vec<i32> = vec![2, 3, 4, 2, 1];
        let input_data = ArconVec::new(input_vec);
        let module = Module::new(code).unwrap();
        let ref mut ctx = WeldContext::new(&module.conf).unwrap();
        let res: ModuleRun<i32> = module.run(&input_data, ctx).unwrap();
        let data = res.0;
        assert_eq!(data, 5);
    }

    #[test]
    fn vec_filter_module() {
        let code = String::from("|v: vec[i32]| filter(v, |a:i32| a > 2)");
        let input_vec: Vec<i32> = vec![2, 3, 4, 2, 1];
        let input_data = ArconVec::new(input_vec);
        let module = Module::new(code).unwrap();
        let ref mut ctx = WeldContext::new(&module.conf).unwrap();
        let result: ModuleRun<ArconVec<i32>> = module.run(&input_data, ctx).unwrap();
        assert_eq!(result.0.len, 2);
        // Run the module again with the returned Vec
        let result: ModuleRun<ArconVec<i32>> = module.run(&result, ctx).unwrap();
        // Should equal in the same
        assert_eq!(result.0.len, 2);
    }

    #[test]
    fn multi_thread_module() {
        let code = String::from("|x:i32| 40 + x");
        let module = Arc::new(Module::new(code).unwrap());
        let num_threads = 4;
        let mut threads = vec![];
        for _ in 0..num_threads {
            let module = Arc::clone(&module);
            threads.push(thread::spawn(move || {
                let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
                let input: i32 = 10;
                let result: ModuleRun<i32> = module.run(&input, ctx).unwrap();
                assert_eq!(result.0, 50);
            }))
        }
        for t in threads {
            t.join().unwrap();
        }
    }
}
