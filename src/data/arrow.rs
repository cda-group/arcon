use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        StringBuilder, StructArray, StructBuilder, UInt32Builder, UInt64Builder,
    },
    datatypes::{DataType, Schema},
    error::ArrowError,
};

pub trait ToArrow {
    type Builder: ArrayBuilder;
    fn arrow_type(&self) -> DataType;
}

macro_rules! to_arrow {
    ($type:ty, $builder_type:ty, $arrow_type:expr) => {
        impl ToArrow for $type {
            type Builder = $builder_type;

            fn arrow_type(&self) -> DataType {
                $arrow_type
            }
        }
    };
}

// Map basic primitives to Arrow Types
to_arrow!(u64, UInt64Builder, DataType::UInt64);
to_arrow!(u32, UInt32Builder, DataType::UInt32);
to_arrow!(i64, Int64Builder, DataType::Int64);
to_arrow!(i32, Int32Builder, DataType::Int32);
to_arrow!(f64, Float64Builder, DataType::Float64);
to_arrow!(f32, Float32Builder, DataType::Float32);
to_arrow!(String, StringBuilder, DataType::Utf8);
to_arrow!(bool, BooleanBuilder, DataType::Boolean);

pub trait ArrowOps {
    /// Return the Arrow Schema
    fn schema(&self) -> Schema;
    fn append(self, builder: &mut StructBuilder);
    fn to_arrow_table(&self, capacity: usize) -> ArrowTable;
}

pub struct ArrowTable {
    builder: StructBuilder,
}

impl ArrowTable {
    pub fn new(builder: StructBuilder) -> Self {
        Self { builder }
    }
    pub fn load(&mut self, data: Vec<impl ArrowOps>) -> Result<(), ArrowError> {
        for value in data {
            value.append(&mut self.builder);
            self.builder.append(true)?;
        }
        Ok(())
    }
    pub fn finish(&mut self) -> StructArray {
        self.builder.finish()
    }
    pub fn len(&self) -> usize {
        self.builder.len()
    }
}
