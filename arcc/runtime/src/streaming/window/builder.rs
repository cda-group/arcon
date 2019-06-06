use crate::error::ErrorKind::*;
use crate::error::*;
use crate::weld::module::Module;
use crate::weld::module::ModuleRun;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::Arc;

use weld::*;

/// `WindowFn` consists of the methods required by the `WindowBuilder` where:
///
/// A: Element type sent to the Window
/// B: Weld Builder type (e.g., Appender<u32>)
/// C: Expected output type of the Window
pub trait WindowFn<A, B: Clone, C: Clone> {
    fn new(builder: B, udf: Arc<Module>, materializer: Arc<Module>) -> Self;
    fn on_element(&mut self, element: A) -> Result<()>;
    fn result(&mut self) -> Result<C>;
}

/// Input struct for the `WindowBuilder` UDF
///
/// Example UDF: "|x: u32, y: appender[u32]| merge(y, x)"
/// where `element` is type u32 and `builder` appender[u32]
#[derive(Clone, Debug)]
#[repr(C)]
pub struct WindowBuilderInput<A, B> {
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
pub struct WindowBuilder<A, B, C> {
    builder: UnsafeCell<B>,
    builder_ctx: WeldContext,
    udf: Arc<Module>,
    materializer: Arc<Module>,
    _input: PhantomData<A>,
    _output: PhantomData<C>,
}

impl<A, B: Clone, C: Clone> WindowFn<A, B, C> for WindowBuilder<A, B, C> {
    fn new(builder: B, udf: Arc<Module>, materializer: Arc<Module>) -> WindowBuilder<A, B, C> {
        let ctx = WeldContext::new(udf.conf()).unwrap();
        WindowBuilder {
            builder: UnsafeCell::new(builder),
            builder_ctx: ctx,
            udf,
            materializer,
            _input: PhantomData,
            _output: PhantomData,
        }
    }
    fn on_element(&mut self, element: A) -> Result<()> {
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

    fn result(&mut self) -> Result<C> {
        let ref mut ctx = WeldContext::new(&self.materializer.conf())
            .map_err(|e| Error::new(ContextError(e.message().to_string_lossy().into_owned())))?;
        let run: ModuleRun<C> = self.materializer.run(&self.builder, ctx)?;
        Ok(run.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weld::data::Appender;
    use weld::data::DictMerger;
    use weld::data::WeldVec;

    #[derive(Clone, Debug, PartialEq)]
    #[repr(C)]
    pub struct Pair<K, V> {
        pub ele1: K,
        pub ele2: V,
    }

    #[derive(Clone, Debug)]
    #[repr(C)]
    pub struct Item {
        pub id: u64,
        pub price: u32,
    }

    #[test]
    fn simple_appender_window_builder_test() {
        // initialize the WindowBuilder's builder
        let init_builder_code = String::from("|| appender[u32]");
        let prio = 0;
        let module =
            Module::new("init_builder".to_string(), init_builder_code, prio, None).unwrap();
        let ref input = 1;
        let ref mut ctx = WeldContext::new(module.conf()).unwrap();
        let run: ModuleRun<Appender<u32>> = module.run(input, ctx).unwrap();
        let init_builder = run.0;

        // define the main udf to be executed on each window element
        let udf_code = String::from("|x: u32, y: appender[u32]| merge(y, x)");
        let udf = Arc::new(Module::new("udf".to_string(), udf_code, prio, None).unwrap());

        // define the materializer
        let result_udf = String::from("|y: appender[u32]| result(y)");
        let result_udf =
            Arc::new(Module::new("result".to_string(), result_udf, prio, None).unwrap());

        let mut window_builder: WindowBuilder<u32, Appender<u32>, WeldVec<u32>> =
            WindowBuilder::new(init_builder, udf.clone(), result_udf.clone());

        let _ = window_builder.on_element(10);
        let _ = window_builder.on_element(20);
        let _ = window_builder.on_element(30);

        // NOTE: this does not do anything special.
        //       the result_udf simply materializes
        //       the appender builder into a WeldVec<>
        let result = window_builder.result().unwrap();
        assert_eq!(result.len as usize, 3);
    }

    #[test]
    fn avg_agg_window_builder_test() {
        // initialize the WindowBuilder's builder
        let init_builder_code = String::from("|| {u64(0),u32(0)}");
        let prio = 0;
        let module =
            Module::new("init_builder".to_string(), init_builder_code, prio, None).unwrap();
        let ref input = 1;
        let ref mut ctx = WeldContext::new(module.conf()).unwrap();
        let run: ModuleRun<Item> = module.run(input, ctx).unwrap();
        let init_builder = run.0;
        // define the main udf to be executed on each window element
        let udf_code = String::from(
            "type item = {u64,u32}; type avg_agg = {u64, u64};
                                     |a: item, b: avg_agg| {b.$0 + u64(a.$1), b.$1 + u64(1)}",
        );
        let udf = Arc::new(Module::new("udf".to_string(), udf_code, prio, None).unwrap());

        // define the materializer
        let result_udf =
            String::from("type avg_agg = {u64,u64}; |x: avg_agg| f64(x.$0) / f64(x.$1)");
        let result_udf =
            Arc::new(Module::new("result".to_string(), result_udf, prio, None).unwrap());

        let mut window_builder: WindowBuilder<Item, Item, f64> =
            WindowBuilder::new(init_builder, udf.clone(), result_udf.clone());

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
        let expected = "167.14";
        let fmt_result = format!("{:.2}", result);
        assert_eq!(fmt_result, expected);
    }

    #[test]
    fn dictmerger_window_builder_test() {
        #[derive(Clone, Debug)]
        #[repr(C)]
        pub struct Input {
            pub id: u64,
            pub nums: WeldVec<u64>,
        }

        // initialize the WindowBuilder's builder
        let init_builder_code = String::from("|| dictmerger[u64,u64,+]");
        let prio = 0;
        let module =
            Module::new("init_builder".to_string(), init_builder_code, prio, None).unwrap();
        let ref input = 0;
        let ref mut ctx = WeldContext::new(module.conf()).unwrap();
        let run: ModuleRun<DictMerger<u64, u64>> = module.run(input, ctx).unwrap();
        let init_builder = run.0;

        // define the main udf to be executed on each window element
        let udf_code = String::from(
            "|in: {u64, vec[u64]}, dm: dictmerger[u64,u64,+]|
            for(in.$1, dm, |b,i,e| merge(b, {in.$0, u64(e)}))",
        );
        let udf = Arc::new(Module::new("udf".to_string(), udf_code, prio, None).unwrap());

        // define the materializer
        let result_udf = String::from("|dm: dictmerger[u64,u64,+]| tovec(result(dm))");
        let result_udf =
            Arc::new(Module::new("result".to_string(), result_udf, prio, None).unwrap());

        let mut window_builder: WindowBuilder<
            Input,
            DictMerger<u64, u64>,
            WeldVec<Pair<u64, u64>>,
        > = WindowBuilder::new(init_builder, udf.clone(), result_udf.clone());

        let i1_nums: Vec<u64> = vec![1, 3, 5, 6];
        let i1 = Input {
            id: 1 as u64,
            nums: WeldVec::from(&i1_nums),
        };

        let i2_nums: Vec<u64> = vec![6, 2, 5, 5];
        let i2 = Input {
            id: 2 as u64,
            nums: WeldVec::from(&i2_nums),
        };

        let i3_nums: Vec<u64> = vec![10, 20, 30, 100];
        let i3 = Input {
            id: 3 as u64,
            nums: WeldVec::from(&i3_nums),
        };

        let _ = window_builder.on_element(i1);
        let _ = window_builder.on_element(i2);
        let _ = window_builder.on_element(i3);

        let result = window_builder.result().unwrap();

        let expected: Vec<(u64, u64)> = vec![(1, 15), (2, 18), (3, 160)];

        let mut collected: Vec<(u64, u64)> = (0..result.len)
            .into_iter()
            .map(|x| {
                let id = unsafe { (*result.data.offset(x as isize)).ele1 };
                let sum = unsafe { ((*result.data.offset(x as isize)).ele2) };
                (id, sum)
            })
            .collect();

        assert_eq!(result.len, 3);
        assert_eq!(collected, expected);
    }

    #[test]
    fn max_by_price_window_builder_test() {
        // TODO: In this case it is not really a builder, but a Item struct.
        //       Gotta find out how to use merger[{u64,u32}, max] with structs

        // NOTE: Those explicit types are not there for show.
        //       If not given, it will lead to weird runtime
        //       behaviour which took hours to debug...
        let init_builder_code = String::from("|| {u64(0),u32(0)}");
        let prio = 0;
        let module =
            Module::new("init_builder".to_string(), init_builder_code, prio, None).unwrap();
        let ref input = 1;
        let ref mut ctx = WeldContext::new(module.conf()).unwrap();
        let run: ModuleRun<Item> = module.run(input, ctx).unwrap();
        let init_builder = run.0;

        // define the main udf to be executed on each window element
        let udf_code = String::from("|a: {u64,u32}, b: {u64,u32}| if(a.$1 > b.$1, a, b)");
        let udf = Arc::new(Module::new("udf".to_string(), udf_code, prio, None).unwrap());

        // define the materializer
        let result_udf = String::from("|y: {u64,u32}| y");
        let result_udf =
            Arc::new(Module::new("result".to_string(), result_udf, prio, None).unwrap());

        let mut window_builder: WindowBuilder<Item, Item, Item> =
            WindowBuilder::new(init_builder, udf.clone(), result_udf.clone());

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
        let prio = 0;
        let module =
            Module::new("init_builder".to_string(), init_builder_code, prio, None).unwrap();
        let ref input = 1;
        let ref mut ctx = WeldContext::new(module.conf()).unwrap();
        let run: ModuleRun<Appender<WeldVec<u32>>> = module.run(input, ctx).unwrap();
        let init_builder = run.0;

        // define the main udf to be executed on each window element
        let udf_code = String::from(
            "|x: vec[u32], y: appender[vec[u32]]|
                          merge(y, result(for(x, vecmerger[u32,+](x), |b,i,e| merge(b, {i, e + u32(5)}))))",
        );
        let udf = Arc::new(Module::new("udf".to_string(), udf_code, prio, None).unwrap());

        // define the materializer
        let result_udf = String::from("|y: appender[vec[u32]]| result(y)");
        let result_udf =
            Arc::new(Module::new("result".to_string(), result_udf, prio, None).unwrap());

        let mut window_builder: WindowBuilder<
            WeldVec<u32>,
            Appender<WeldVec<u32>>,
            WeldVec<WeldVec<u32>>,
        > = WindowBuilder::new(init_builder, udf.clone(), result_udf.clone());

        let mut window_data: Vec<Vec<u32>> = Vec::new();

        let r1 = vec![1, 2, 3, 4, 5];
        let r1_data = WeldVec::from(&r1);

        let r2 = vec![6, 7, 9, 10, 11];
        let r2_data = WeldVec::from(&r2);

        let r3 = vec![12, 13, 14, 15, 16];
        let r3_data = WeldVec::from(&r3);

        let r4 = vec![17, 18, 19, 20, 21];
        let r4_data = WeldVec::from(&r4);

        window_data.push(r1);
        window_data.push(r2);
        window_data.push(r3);
        window_data.push(r4);

        let _ = window_builder.on_element(r1_data);
        let _ = window_builder.on_element(r2_data);
        let _ = window_builder.on_element(r3_data);
        let _ = window_builder.on_element(r4_data);

        let result = window_builder.result().unwrap();

        // TODO: transform the WeldVec<..> into a Rust
        //       vector to make this more easy on the eye...
        for i in 0..(window_data.len() as isize) {
            let vec = unsafe { result.data.offset(i) };
            let len = unsafe { (*vec).len };
            for x in 0..(len as isize) {
                assert_eq!(
                    unsafe { *(*vec).data.offset(x) },
                    window_data[i as usize][x as usize] + window_data[i as usize][x as usize] + 5
                );
            }
        }
    }
}
