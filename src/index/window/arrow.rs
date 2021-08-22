use arrow::{datatypes::Schema, record_batch::RecordBatch};

use crate::{
    index::{IndexOps, WindowIndex},
    prelude::*,
    stream::operator::window::WindowContext,
    table::{to_record_batches, ImmutableTable, RawRecordBatch},
    util::ArconFnBounds,
};
use arcon_state::{backend::handles::ActiveHandle, Backend, VecState};
use std::marker::PhantomData;

/// A window index for Arrow Data
///
/// Elements are appended into RecordBatches and once a window is triggered,
/// the underlying Arrow Schema and Vec<RecordBatch> is exposed.
pub struct ArrowWindow<IN, OUT, F, B>
where
    IN: ArconType + ToArrow,
    OUT: ArconType,
    F: Fn(Arc<Schema>, Vec<RecordBatch>) -> ArconResult<OUT> + ArconFnBounds,
    B: Backend,
{
    handle: ActiveHandle<B, VecState<RawRecordBatch>, u64, u64>,
    map: std::collections::HashMap<WindowContext, MutableTable>,
    udf: F,
    _marker: std::marker::PhantomData<IN>,
}

impl<IN, OUT, F, B> ArrowWindow<IN, OUT, F, B>
where
    IN: ArconType + ToArrow,
    OUT: ArconType,
    F: Fn(Arc<Schema>, Vec<RecordBatch>) -> ArconResult<OUT> + ArconFnBounds,
    B: Backend,
{
    pub fn new(backend: Arc<B>, udf: F) -> Self {
        let mut handle = Handle::vec("window_handle")
            .with_item_key(0)
            .with_namespace(0);

        backend.register_vec_handle(&mut handle);

        let handle = handle.activate(backend);

        Self {
            handle,
            map: std::collections::HashMap::new(),
            udf,
            _marker: PhantomData,
        }
    }
}

impl<IN, OUT, F, B> WindowIndex for ArrowWindow<IN, OUT, F, B>
where
    IN: ArconType + ToArrow,
    OUT: ArconType,
    F: Fn(Arc<Schema>, Vec<RecordBatch>) -> ArconResult<OUT> + ArconFnBounds,
    B: Backend,
{
    type IN = IN;
    type OUT = OUT;

    fn on_element(&mut self, element: Self::IN, ctx: WindowContext) -> ArconResult<()> {
        let table = self.map.entry(ctx).or_insert_with(IN::table);
        table.append(element)?;

        Ok(())
    }

    fn result(&mut self, ctx: WindowContext) -> ArconResult<Self::OUT> {
        let table = self.map.entry(ctx).or_insert_with(IN::table);
        self.handle.set_item_key(ctx.key);
        self.handle.set_namespace(ctx.index);

        // fetch in-memory batches
        let mut batches = table.batches()?;
        // fetch if any raw batches and append to the vector...
        let raw_batches = self.handle.get()?;
        batches.append(&mut to_record_batches(Arc::new(IN::schema()), raw_batches)?);

        (self.udf)(Arc::new(IN::schema()), batches)
    }

    fn clear(&mut self, ctx: WindowContext) -> ArconResult<()> {
        // clear from memory layer
        let _ = self.map.remove(&ctx);

        // clear everything in the backend
        self.handle.set_item_key(ctx.key);
        self.handle.set_namespace(ctx.index);

        self.handle.clear()?;

        Ok(())
    }
}
impl<IN, OUT, F, B> IndexOps for ArrowWindow<IN, OUT, F, B>
where
    IN: ArconType + ToArrow,
    OUT: ArconType,
    F: Fn(Arc<Schema>, Vec<RecordBatch>) -> ArconResult<OUT> + ArconFnBounds,
    B: Backend,
{
    fn persist(&mut self) -> ArconResult<()> {
        for (ctx, table) in self.map.iter_mut() {
            self.handle.set_item_key(ctx.key);
            self.handle.set_namespace(ctx.index);

            let batches = table.raw_batches()?;
            for batch in batches {
                self.handle.append(batch)?;
            }
        }
        Ok(())
    }
    fn set_key(&mut self, _: u64) {}
    fn table(&mut self) -> ArconResult<Option<ImmutableTable>> {
        Ok(None)
    }
}
