use crate::{data::ArconType, util::SafelySendableFn};

pub enum WindowFunction<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    Appender(&'static dyn SafelySendableFn(&[IN]) -> OUT),
    Incremental(
        &'static dyn SafelySendableFn(IN) -> OUT,
        &'static dyn SafelySendableFn(IN, &OUT) -> OUT,
    ),
}
