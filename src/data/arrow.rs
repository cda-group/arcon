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
use datafusion::{datasource::MemTable, error::DataFusionError};
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
    fn arrow_table(capacity: usize) -> ArrowTable;
    /// Used to append `self` to an Arrow StructBuilder
    fn append(self, builder: &mut StructBuilder) -> Result<(), ArrowError>;
}

macro_rules! arrow_ops {
    ($type:ty) => {
        impl ArrowOps for $type {
            fn schema() -> Schema {
                unreachable!(
                    "ArrowOps not possible for single value {}",
                    stringify!($type)
                );
            }
            fn arrow_table(_: usize) -> ArrowTable {
                unreachable!(
                    "ArrowOps not possible for single value {}",
                    stringify!($type)
                );
            }
            fn append(self, _: &mut StructBuilder) -> Result<(), ArrowError> {
                unreachable!(
                    "ArrowOps not possible for single value {}",
                    stringify!($type)
                );
            }
        }
    };
}

// Implements unreachable ArrowOps impl for single values.
arrow_ops!(u64);
arrow_ops!(u32);
arrow_ops!(i64);
arrow_ops!(i32);
arrow_ops!(f64);
arrow_ops!(f32);
arrow_ops!(bool);
arrow_ops!(String);
arrow_ops!(Vec<u8>);

#[derive(Debug)]
pub struct ArrowTable {
    table_name: String,
    schema: Arc<Schema>,
    builder: StructBuilder,
}

impl ArrowTable {
    pub fn new(table_name: String, schema: Schema, builder: StructBuilder) -> Self {
        Self {
            table_name,
            schema: Arc::new(schema),
            builder,
        }
    }
    pub fn load(
        &mut self,
        data: impl IntoIterator<Item = impl ArrowOps>,
    ) -> Result<(), ArrowError> {
        for value in data {
            value.append(&mut self.builder)?;
            self.builder.append(true)?;
        }
        Ok(())
    }
    pub fn name(&self) -> &str {
        &self.table_name
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

    pub fn mem_table(&mut self) -> Result<MemTable, DataFusionError> {
        let record_batch = self.record_batch()?;
        MemTable::try_new(self.schema(), vec![vec![record_batch]])
    }
    pub fn set_name(&mut self, name: &str) {
        self.table_name = name.to_string();
    }
}

impl<A: ArrowOps> From<Vec<A>> for ArrowTable {
    fn from(input: Vec<A>) -> Self {
        let mut table = A::arrow_table(input.len());
        let _ = table.load(input);
        table
    }
}

// impl Send + Sync for ArrowTable
unsafe impl Send for ArrowTable {}
unsafe impl Sync for ArrowTable {}

#[derive(Clone)]
pub struct ImmutableTable {
    name: String,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
}

impl ImmutableTable {
    pub fn mem_table(self) -> Result<MemTable, DataFusionError> {
        MemTable::try_new(self.schema, vec![self.batches])
    }
    pub fn name(&self) -> String {
        self.name.clone()
    }
}

impl From<ArrowTable> for ImmutableTable {
    fn from(mut table: ArrowTable) -> Self {
        Self {
            name: table.name().to_string(),
            schema: table.schema(),
            batches: vec![table.record_batch().unwrap()],
        }
    }
}
