// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::table::MutableTable;
use arrow::{
    array::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
    },
    datatypes::{DataType, Schema},
    error::ArrowError,
};

/// Represents an Arcon type that can be converted to Arrow
pub trait ToArrow {
    /// Type to help the runtime know which builder to use
    type Builder: ArrayBuilder;
    /// Returns the underlying Arrow [DataType]
    fn arrow_type() -> DataType;
    /// Return the Arrow Schema
    fn schema() -> Schema;
    /// Creates a new MutableTable
    fn table() -> MutableTable;
    /// Used to append `self` to an Arrow StructBuilder
    fn append(self, builder: &mut StructBuilder) -> Result<(), ArrowError>;
}

macro_rules! to_arrow {
    ($type:ty, $builder_type:ty, $arrow_type:expr) => {
        impl ToArrow for $type {
            type Builder = $builder_type;

            fn arrow_type() -> DataType {
                $arrow_type
            }
            fn schema() -> Schema {
                unreachable!(
                    "Operation not possible for single value {}",
                    stringify!($type)
                );
            }
            fn table() -> MutableTable {
                unreachable!(
                    "Operation not possible for single value {}",
                    stringify!($type)
                );
            }
            fn append(self, _: &mut StructBuilder) -> Result<(), ArrowError> {
                unreachable!(
                    "Operation not possible for single value {}",
                    stringify!($type)
                );
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
to_arrow!(String, StringBuilder, DataType::Utf8);
to_arrow!(Vec<u8>, BinaryBuilder, DataType::Binary);
