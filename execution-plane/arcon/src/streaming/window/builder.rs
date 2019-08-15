use crate::data::*;
use crate::error::*;
use crate::weld::module::Module;
use crate::weld::module::ModuleRun;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::Arc;
use weld_core::*;

/// `WindowModules` is a helper struct that holds Weld UDFs
///  for a specific WindowBuilder
#[derive(Clone)]
pub struct WindowModules {
    pub init_builder: Arc<Module>,
    pub udf: Arc<Module>,
    pub materializer: Arc<Module>,
}

/// `WindowFn` consists of the methods required by the `WindowBuilder` where:
///
/// A: Element type sent to the Window
/// B: Weld Builder or ArconType to be updated (e.g., Appender<u32>)
/// C: Expected output type of the Window
pub trait WindowFn<A, B, C>
where
    A: ArconType,
    B: Clone,
    C: ArconType,
{
    fn new(modules: WindowModules) -> ArconResult<Self>
    where
        Self: Sized;
    fn on_element(&mut self, element: A) -> ArconResult<()>;
    fn result(&mut self) -> ArconResult<C>;
}

/// Input struct for the `WindowBuilder` UDF
///
/// Example UDF: "|x: u32, y: appender[u32]| merge(y, x)"
/// where `element` is type u32 and `builder` appender[u32]
#[derive(Clone, Debug)]
#[repr(C)]
pub struct WindowBuilderInput<A, B>
where
    A: ArconType,
    B: Clone,
{
    element: A,
    builder: B,
}

/// A Window Builder for Streaming
///
/// The `WindowBuilder` uses a single `WeldContext` in
/// order to continously update its builder in-place
///
/// Also, the `WindowBuilder` contains two Weld UDFs, one for
/// updating its builder per element and the other,
/// to materialize the complete window
pub struct WindowBuilder<A, B, C>
where
    A: ArconType,
    B: Clone,
    C: ArconType,
{
    builder: UnsafeCell<B>,
    builder_ctx: WeldContext,
    udf: Arc<Module>,
    materializer: Arc<Module>,
    _input: PhantomData<A>,
    _output: PhantomData<C>,
}

impl<A, B, C> WindowFn<A, B, C> for WindowBuilder<A, B, C>
where
    A: ArconType,
    B: Clone,
    C: ArconType,
{
    /// Creates a new `WindowBuilder`
    ///
    /// ```rust
    /// use arcon::prelude::*;
    /// use weld::data::Appender;
    /// use std::sync::Arc;
    ///
    /// let init_builder_code = String::from("|| appender[u32]");
    /// let init_builder = Arc::new(Module::new(init_builder_code).unwrap());
    /// let udf_code = String::from("|x: u32, y: appender[u32]| merge(y, x)");
    /// let udf = Arc::new(Module::new(udf_code).unwrap());
    /// let result_udf = String::from("|y: appender[u32]| map(result(y), |a:u32| a + u32(5))");
    /// let materializer = Arc::new(Module::new(result_udf).unwrap());
    ///
    /// let window_modules = WindowModules {
    ///     init_builder,
    ///     udf,
    ///     materializer,
    /// };
    /// let mut window_builder: WindowBuilder<u32, Appender<u32>, ArconVec<u32>> = WindowBuilder::new(window_modules).unwrap();
    /// ```
    fn new(modules: WindowModules) -> ArconResult<WindowBuilder<A, B, C>> {
        let mut ctx = WeldContext::new(modules.udf.conf()).map_err(|e| {
            weld_error!(
                "Failed to create WeldContext with err {}",
                e.message().to_string_lossy().into_owned()
            )
        })?;
        let run: ModuleRun<B> = modules.init_builder.run(&1, &mut ctx)?;

        Ok(WindowBuilder {
            builder: UnsafeCell::new(run.0),
            builder_ctx: ctx,
            udf: modules.udf,
            materializer: modules.materializer,
            _input: PhantomData,
            _output: PhantomData,
        })
    }

    /// Appends the input to the `WindowBuilder`
    ///
    /// ```rust
    /// # use arcon::prelude::*;
    /// # use weld::data::Appender;
    /// # use std::sync::Arc;
    ///
    /// # let init_builder_code = String::from("|| appender[u32]");
    /// # let init_builder = Arc::new(Module::new(init_builder_code).unwrap());
    /// # let udf_code = String::from("|x: u32, y: appender[u32]| merge(y, x)");
    /// # let udf = Arc::new(Module::new(udf_code).unwrap());
    /// # let result_udf = String::from("|y: appender[u32]| map(result(y), |a:u32| a + u32(5))");
    /// # let materializer = Arc::new(Module::new(result_udf).unwrap());
    ///
    /// # let window_modules = WindowModules {
    /// #   init_builder,
    /// #   udf,
    /// #   materializer,
    /// # };
    /// let mut window_builder: WindowBuilder<u32, Appender<u32>, ArconVec<u32>> = WindowBuilder::new(window_modules).unwrap();
    /// let _ = window_builder.on_element(10);
    /// ```
    fn on_element(&mut self, element: A) -> ArconResult<()> {
        let ref input = WindowBuilderInput {
            element: element,
            builder: unsafe { (*self.builder.get()).clone() },
        };

        let run: ModuleRun<B> = self.udf.run(input, &mut self.builder_ctx)?;
        unsafe {
            *self.builder.get() = run.0;
        }
        Ok(())
    }

    /// Materializes the `WindowBuilder`
    ///
    /// ```rust
    /// # use arcon::prelude::*;
    /// # use weld::data::Appender;
    /// # use std::sync::Arc;
    ///
    /// # let init_builder_code = String::from("|| appender[u32]");
    /// # let init_builder = Arc::new(Module::new(init_builder_code).unwrap());
    /// # let udf_code = String::from("|x: u32, y: appender[u32]| merge(y, x)");
    /// # let udf = Arc::new(Module::new(udf_code).unwrap());
    /// # let result_udf = String::from("|y: appender[u32]| map(result(y), |a:u32| a + u32(5))");
    /// # let materializer = Arc::new(Module::new(result_udf).unwrap());
    ///
    /// # let window_modules = WindowModules {
    /// #   init_builder,
    /// #   udf,
    /// #   materializer,
    /// # };
    /// let mut window_builder: WindowBuilder<u32, Appender<u32>, ArconVec<u32>> = WindowBuilder::new(window_modules).unwrap();
    /// let _ = window_builder.on_element(10);
    /// let _ = window_builder.on_element(20);
    /// let result = window_builder.result().unwrap();
    /// assert_eq!(result[0], 15);
    /// assert_eq!(result[1], 25);
    /// ```
    fn result(&mut self) -> ArconResult<C> {
        let run: ModuleRun<C> = self
            .materializer
            .run(&self.builder, &mut self.builder_ctx)?;
        Ok(run.0)
    }
}

unsafe impl<A, B, C> Send for WindowBuilder<A, B, C>
where
    A: ArconType,
    B: Clone,
    C: ArconType,
{
}
unsafe impl<A, B, C> Sync for WindowBuilder<A, B, C>
where
    A: ArconType,
    B: Clone,
    C: ArconType,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Pair;
    use kompact::*;
    use std::sync::Arc;
    use weld::data::Appender;
    use weld::data::DictMerger;

    #[arcon]
    #[repr(C)]
    #[derive(Hash)]
    pub struct Item {
        pub id: u64,
        pub price: u32,
    }

    #[test]
    fn simple_appender_window_builder_test() {
        // initialize the WindowBuilder's builder
        let init_builder_code = String::from("|| appender[u32]");
        let init_builder = Arc::new(Module::new(init_builder_code).unwrap());

        // define the main udf to be executed on each window element
        let udf_code = String::from("|x: u32, y: appender[u32]| merge(y, x)");
        let udf = Arc::new(Module::new(udf_code).unwrap());

        // define the materializer
        let result_udf = String::from("|y: appender[u32]| map(result(y), |a:u32| a + u32(5))");
        let materializer = Arc::new(Module::new(result_udf).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf,
            materializer,
        };

        let mut window_builder: WindowBuilder<u32, Appender<u32>, ArconVec<u32>> =
            WindowBuilder::new(window_modules).unwrap();

        for i in 0..10000 {
            let _ = window_builder.on_element(i);
        }

        let result = window_builder.result().unwrap();
        assert_eq!(result.len as usize, 10000);
        for i in 0..(result.len as isize) {
            let item = unsafe { *result.ptr.offset(i) };
            assert_eq!(item, i as u32 + 5);
        }
    }

    #[test]
    fn avg_agg_window_builder_test() {
        #[arcon]
        #[derive(Hash)]
        pub struct AvgAgg {
            total: u64,
            counter: u64,
        }

        // initialize the WindowBuilder's builder
        let init_builder_code = String::from("|| {u64(0),u64(0)}");
        let init_builder = Arc::new(Module::new(init_builder_code).unwrap());

        // define the main udf to be executed on each window element
        let udf_code = String::from(
            "type item = {u64,u32}; type avg_agg = {u64, u64};
                                     |a: item, b: avg_agg| {b.$0 + u64(a.$1), b.$1 + u64(1)}",
        );
        let udf = Arc::new(Module::new(udf_code).unwrap());

        // define the materializer
        let result_udf =
            String::from("type avg_agg = {u64,u64}; |x: avg_agg| u64(x.$0) / u64(x.$1)");
        let materializer = Arc::new(Module::new(result_udf).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf,
            materializer,
        };

        let mut window_builder: WindowBuilder<Item, AvgAgg, u64> =
            WindowBuilder::new(window_modules).unwrap();

        let i1 = Item { id: 1, price: 100 };
        let i2 = Item { id: 2, price: 150 };
        let i3 = Item { id: 3, price: 200 };
        let i4 = Item { id: 4, price: 300 };
        let i5 = Item { id: 5, price: 100 };
        let i6 = Item { id: 6, price: 120 };
        let i7 = Item { id: 7, price: 200 };

        let _ = window_builder.on_element(i1);
        let _ = window_builder.on_element(i2);
        let _ = window_builder.on_element(i3);
        let _ = window_builder.on_element(i4);
        let _ = window_builder.on_element(i5);
        let _ = window_builder.on_element(i6);
        let _ = window_builder.on_element(i7);

        let result = window_builder.result().unwrap();
        assert_eq!(result, 167);
    }

    #[test]
    fn dictmerger_window_builder_test() {
        #[repr(C)]
        #[arcon]
        pub struct Input {
            pub id: u64,
            pub nums: ArconVec<u64>,
        }

        // initialize the WindowBuilder's builder
        let init_builder_code = String::from("|| dictmerger[u64,u64,+]");
        let init_builder = Arc::new(Module::new(init_builder_code).unwrap());

        // define the main udf to be executed on each window element
        let udf_code = String::from(
            "|in: {u64, vec[u64]}, dm: dictmerger[u64,u64,+]|
            for(in.$1, dm, |b,i,e| merge(b, {in.$0, u64(e)}))",
        );
        let udf = Arc::new(Module::new(udf_code).unwrap());

        // define the materializer
        let result_udf = String::from("|dm: dictmerger[u64,u64,+]| tovec(result(dm))");
        let materializer = Arc::new(Module::new(result_udf).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf,
            materializer,
        };

        let mut window_builder: WindowBuilder<
            Input,
            DictMerger<u64, u64>,
            ArconVec<Pair<u64, u64>>,
        > = WindowBuilder::new(window_modules).unwrap();

        let i1_nums: Vec<u64> = vec![1, 3, 5, 6];
        let i1 = Input {
            id: 1 as u64,
            nums: ArconVec::new(i1_nums),
        };

        let i2_nums: Vec<u64> = vec![6, 2, 5, 5];
        let i2 = Input {
            id: 2 as u64,
            nums: ArconVec::new(i2_nums),
        };

        let i3_nums: Vec<u64> = vec![10, 20, 30, 100];
        let i3 = Input {
            id: 3 as u64,
            nums: ArconVec::new(i3_nums),
        };

        let _ = window_builder.on_element(i1);
        let _ = window_builder.on_element(i2);
        let _ = window_builder.on_element(i3);

        let result = window_builder.result().unwrap();

        let expected: Vec<(u64, u64)> = vec![(1, 15), (2, 18), (3, 160)];

        let collected: Vec<(u64, u64)> = (0..result.len)
            .into_iter()
            .map(|x| {
                let id = unsafe { (*result.ptr.offset(x as isize)).ele1 };
                let sum = unsafe { ((*result.ptr.offset(x as isize)).ele2) };
                (id, sum)
            })
            .collect();

        assert_eq!(result.len, 3);
        assert_eq!(collected, expected);
    }

    #[test]
    fn normalise_window_builder_test() {
        let builder_code = String::from("||appender[i64]");
        let udf_code = String::from("|e:i64,w:appender[i64]| merge(w,e):appender[i64]");
        let materialiser_code = String::from("|e: appender[i64]| let elem = result(e); let sum = result(for(elem, merger[i64, +], |b: merger[i64, +], i: i64, e: i64| merge(b, e))); 
                                         let count = len(elem); let avg = sum / count; result(for(elem, appender[i64], |b: appender[i64], i: i64, e: i64| merge(b, e / avg)))") ;
        let init_builder = Arc::new(Module::new(builder_code).unwrap());
        let udf = Arc::new(Module::new(udf_code).unwrap());
        let materializer = Arc::new(Module::new(materialiser_code).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf,
            materializer,
        };

        let mut window_builder: WindowBuilder<i64, Appender<i64>, ArconVec<i64>> =
            WindowBuilder::new(window_modules).unwrap();

        for i in 1..10 {
            let _ = window_builder.on_element(i as i64);
        }

        let result = window_builder.result().unwrap();
        assert_eq!(result.len, 9);
        let expected: Vec<i64> = vec![0, 0, 0, 0, 1, 1, 1, 1, 1];
        assert_eq!(*expected, *result);
    }

    #[test]
    fn max_by_price_window_builder_test() {
        // TODO: In this case it is not really a builder, but a Item struct.
        //       Gotta find out how to use merger[{u64,u32}, max] with structs

        // NOTE: Those explicit types are not there for show.
        //       If not given, it will lead to weird runtime
        //       behaviour which took hours to debug...
        let init_builder_code = String::from("|| {u64(0),u32(0)}");
        let init_builder = Arc::new(Module::new(init_builder_code).unwrap());

        // define the main udf to be executed on each window element
        let udf_code = String::from("|a: {u64,u32}, b: {u64,u32}| if(a.$1 > b.$1, a, b)");
        let udf = Arc::new(Module::new(udf_code).unwrap());

        // define the materializer
        let result_udf = String::from("|y: {u64,u32}| y");
        let materializer = Arc::new(Module::new(result_udf).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf,
            materializer,
        };

        let mut window_builder: WindowBuilder<Item, Item, Item> =
            WindowBuilder::new(window_modules).unwrap();

        let i1 = Item { id: 1, price: 10 };
        let i2 = Item { id: 2, price: 15 };
        let i3 = Item { id: 3, price: 5 };
        let i4 = Item { id: 4, price: 20 };

        let _ = window_builder.on_element(i1);
        let _ = window_builder.on_element(i2);
        let _ = window_builder.on_element(i3);
        let _ = window_builder.on_element(i4);

        let result = window_builder.result().unwrap();
        assert_eq!(result.price, 20);
    }

    #[test]
    fn vecmerger_window_builder_test() {
        // initialize the WindowBuilder's builder
        let init_builder_code = String::from("|| appender[vec[u32]]");
        let init_builder = Arc::new(Module::new(init_builder_code).unwrap());

        // define the main udf to be executed on each window element
        let udf_code = String::from(
            "|x: vec[u32], y: appender[vec[u32]]|
                          merge(y, result(for(x, vecmerger[u32,+](x), |b,i,e| merge(b, {i, e + u32(5)}))))",
        );
        let udf = Arc::new(Module::new(udf_code).unwrap());

        // define the materializer
        let result_udf = String::from("|y: appender[vec[u32]]| result(y)");
        let materializer = Arc::new(Module::new(result_udf).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf,
            materializer,
        };
        let mut window_builder: WindowBuilder<
            ArconVec<u32>,
            Appender<ArconVec<u32>>,
            ArconVec<ArconVec<u32>>,
        > = WindowBuilder::new(window_modules).unwrap();

        let mut window_data: Vec<Vec<u32>> = Vec::new();

        let r1 = vec![1, 2, 3, 4, 5];
        window_data.push(r1.clone());
        let r1_data = ArconVec::new(r1);

        let r2 = vec![6, 7, 9, 10, 11];
        window_data.push(r2.clone());
        let r2_data = ArconVec::new(r2);

        let r3 = vec![12, 13, 14, 15, 16];
        window_data.push(r3.clone());
        let r3_data = ArconVec::new(r3);

        let r4 = vec![17, 18, 19, 20, 21];
        window_data.push(r4.clone());
        let r4_data = ArconVec::new(r4);

        let _ = window_builder.on_element(r1_data);
        let _ = window_builder.on_element(r2_data);
        let _ = window_builder.on_element(r3_data);
        let _ = window_builder.on_element(r4_data);

        let result = window_builder.result().unwrap();

        // TODO: transform the ArconVec<..> into a Rust
        //       vector to make this more easy on the eye...
        for i in 0..(window_data.len() as isize) {
            let vec = unsafe { result.ptr.offset(i) };
            let len = unsafe { (*vec).len };
            for x in 0..(len as isize) {
                assert_eq!(
                    unsafe { *(*vec).ptr.offset(x) },
                    window_data[i as usize][x as usize] + window_data[i as usize][x as usize] + 5
                );
            }
        }
    }

    #[derive(ComponentDefinition)]
    pub struct WindowComponent {
        ctx: ComponentContext<WindowComponent>,
        window_builder: WindowBuilder<u32, Appender<u32>, ArconVec<u32>>,
        pub result: Option<ArconVec<u32>>,
    }

    impl WindowComponent {
        pub fn new(udf: Arc<Module>, materializer: Arc<Module>) -> WindowComponent {
            let init_builder_code = String::from("|| appender[u32]");
            let init_builder = Arc::new(Module::new(init_builder_code).unwrap());

            let window_modules = WindowModules {
                init_builder,
                udf,
                materializer,
            };

            let window_builder: WindowBuilder<u32, Appender<u32>, ArconVec<u32>> =
                WindowBuilder::new(window_modules).unwrap();

            WindowComponent {
                ctx: ComponentContext::new(),
                window_builder,
                result: None,
            }
        }
    }

    impl Provide<ControlPort> for WindowComponent {
        fn handle(&mut self, _event: ControlEvent) -> () {}
    }

    impl Actor for WindowComponent {
        fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
            if let Some(payload) = msg.downcast_ref::<Item>() {
                let _ = self.window_builder.on_element(payload.price);
            }

            if let Some(_) = msg.downcast_ref::<String>() {
                self.result = Some(self.window_builder.result().unwrap());
            }
        }
        fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
    }

    #[test]
    fn window_builder_component_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let udf_code = String::from("|x: u32, y: appender[u32]| merge(y, x)");
        let udf_result = String::from("|y: appender[u32]| result(y)");
        let code_udf = Arc::new(Module::new(udf_code).unwrap());
        let result_udf = Arc::new(Module::new(udf_result).unwrap());

        let (window_comp, _) = system.create_and_register(move || {
            WindowComponent::new(code_udf.clone(), result_udf.clone())
        });
        system.start(&window_comp);
        let window_comp_ref = window_comp.actor_ref();

        let items = 1000;

        for i in 0..items {
            let item = Item { id: i, price: 100 };
            window_comp_ref.tell(Box::new(item), &window_comp_ref);
        }
        window_comp_ref.tell(Box::new(String::from("done")), &window_comp_ref);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut window_c = window_comp.definition().lock().unwrap();
        let result = window_c.result.take().unwrap();
        assert_eq!(result.len, items as i64);
        for i in 0..(result.len as isize) {
            let price = unsafe { *result.ptr.offset(i) };
            assert_eq!(price, 100);
        }
    }
}
