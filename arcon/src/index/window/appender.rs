use super::super::WindowIndex;
use crate::data::ArconType;
use crate::error::ArconResult;
use crate::stream::operator::window::WindowContext;
use crate::util::ArconFnBounds;
use crate::{index::IndexOps, table::ImmutableTable};
use arcon_state::{backend::handles::ActiveHandle, Backend, Handle, VecState};
use std::sync::Arc;

pub struct AppenderWindow<IN, OUT, F, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: Fn(&[IN]) -> OUT + ArconFnBounds,
    B: Backend,
{
    handle: ActiveHandle<B, VecState<IN>, u64, u64>,
    materializer: F,
}
impl<IN, OUT, F, B> AppenderWindow<IN, OUT, F, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: Fn(&[IN]) -> OUT + ArconFnBounds,
    B: Backend,
{
    pub fn new(backend: Arc<B>, materializer: F) -> Self {
        let mut handle = Handle::vec("window_handle")
            .with_item_key(0)
            .with_namespace(0);

        backend.register_vec_handle(&mut handle);

        let handle = handle.activate(backend);

        Self {
            handle,
            materializer,
        }
    }
}

impl<IN, OUT, F, B> WindowIndex for AppenderWindow<IN, OUT, F, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: Fn(&[IN]) -> OUT + ArconFnBounds,
    B: Backend,
{
    type IN = IN;
    type OUT = OUT;

    fn on_element(&mut self, element: Self::IN, ctx: WindowContext) -> ArconResult<()> {
        self.handle.set_item_key(ctx.key);
        self.handle.set_namespace(ctx.index);

        self.handle.append(element)?;
        Ok(())
    }

    fn result(&mut self, ctx: WindowContext) -> ArconResult<Self::OUT> {
        self.handle.set_item_key(ctx.key);
        self.handle.set_namespace(ctx.index);

        let buf = self.handle.get()?;
        Ok((self.materializer)(&buf))
    }

    fn clear(&mut self, ctx: WindowContext) -> ArconResult<()> {
        self.handle.set_item_key(ctx.key);
        self.handle.set_namespace(ctx.index);

        self.handle.clear()?;
        Ok(())
    }
}
impl<IN, OUT, F, B> IndexOps for AppenderWindow<IN, OUT, F, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: Fn(&[IN]) -> OUT + ArconFnBounds,
    B: Backend,
{
    fn persist(&mut self) -> ArconResult<()> {
        Ok(())
    }
    fn set_key(&mut self, _: u64) {}
    fn table(&mut self) -> ArconResult<Option<ImmutableTable>> {
        Ok(None)
    }
}
