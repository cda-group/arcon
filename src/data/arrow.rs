// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arrow::{
    array::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
    },
    datatypes::{DataType, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use std::sync::Arc;

/// Represents an Arcon type that can be converted to Arrow
pub trait ToArrow {
    /// Type to help the runtime know which builder to use
    type Builder: ArrayBuilder;
    /// Returns the underlying Arrow [DataType]
    fn arrow_type() -> DataType;
}

macro_rules! to_arrow {
    ($type:ty, $builder_type:ty, $arrow_type:expr) => {
        impl ToArrow for $type {
            type Builder = $builder_type;

            fn arrow_type() -> DataType {
                $arrow_type
            }
        }
    };
}

// Map types to Arrow Types
to_arrow!(u64, UInt64Builder, DataType::UInt64);
to_arrow!(u32, UInt32Builder, DataType::UInt32);
to_arrow!(i64, Int64Builder, DataType::Int64);
to_arrow!(i32, Int32Builder, DataType::Int32);
to_arrow!(f64, Float64Builder, DataType::Float64);
to_arrow!(f32, Float32Builder, DataType::Float32);
to_arrow!(bool, BooleanBuilder, DataType::Boolean);
// The following two only works because of this patch: https://github.com/Max-Meldrum/arrow/tree/as_ref_patch
//
// TODO: Contribute it upstream
to_arrow!(String, StringBuilder, DataType::Utf8);
to_arrow!(Vec<u8>, BinaryBuilder, DataType::Binary);

/// The Arrow derive macro must implement the following trait.
pub trait ArrowOps: Sized {
    /// Return the Arrow Schema
    fn schema() -> Schema;
    /// Creates an Empty ArrowTable
    fn arrow_table(capacity: usize) -> ArrowTable<Self>;
    /// Used to append `self` to an Arrow StructBuilder
    fn append(self, builder: &mut StructBuilder) -> Result<(), ArrowError>;
}

pub struct ArrowTable<A: ArrowOps> {
    schema: Arc<Schema>,
    builder: StructBuilder,
    _marker: std::marker::PhantomData<A>,
}

impl<A: ArrowOps> ArrowTable<A> {
    pub fn new(builder: StructBuilder) -> Self {
        Self {
            schema: Arc::new(A::schema()),
            builder,
            _marker: std::marker::PhantomData,
        }
    }
    pub fn load(&mut self, data: impl IntoIterator<Item = A>) -> Result<(), ArrowError> {
        for value in data {
            value.append(&mut self.builder)?;
            self.builder.append(true)?;
        }
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.builder.len()
    }
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
    pub fn record_batch(&mut self) -> Result<RecordBatch, ArrowError> {
        let columns = self.schema.fields().len();
        let data_arr = self.builder.finish();
        let mut arr = Vec::with_capacity(columns);
        for i in 0..columns {
            arr.push(data_arr.column(i).clone());
        }
        RecordBatch::try_new(self.schema(), arr)
    }
}

impl<A: ArrowOps> From<Vec<A>> for ArrowTable<A> {
    fn from(input: Vec<A>) -> Self {
        let mut table: ArrowTable<A> = A::arrow_table(input.len());
        let _ = table.load(input);
        table
    }
}
