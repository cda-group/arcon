+++
title = "Betting on Arrow"
description = ""
date = 2021-03-31T09:19:42+00:00
updated = 2021-03-31T09:19:42+00:00
draft = false
template = "blog/page.html"

[taxonomies]
authors = ["Max Meldrum"]

[extra]
lead = ""
+++
# Introduction

Like many other data processing systems, [Arcon](https://github.com/cda-group/arcon) is also betting on the [Arrow](https://arrow.apache.org/) format. 

Arcon has until recently been a pure row-based (Protobuf) system. While this data arrangement works for a range of streaming operations, it is not suitable for ad-hoc OLAP queries or larger bulk aggregations on streaming windows.
With a Columnar format, we gain a lot from vectorised processing (SIMD) and the fact that we can share internal Arcon state with other systems through interfaces such as ``Arrow Flight``.

In this post, we'll go through how Arcon handles the conversion from Protobuf to Arrow data.

Sections:

*   [Defining Protobuf data in Arcon](#defining-protobuf-data-in-arcon)
*   [Working with Arrow](#working-with-arrow)
*   [Arrow Derive Macro](#arrow-derive-macro)
*   [Fixed Limitations](#fixed-limitations)
*   [Current Limitations](#current-limitations)
*   [Conclusion](#conclusion)

## Defining Protobuf data in Arcon

Let's first go over the structure of an ``ArconType``. Down below you can see its Rust trait. 

```rust
pub trait ArconType: ArconTypeBounds
where
    Self: std::marker::Sized,
{
    #[cfg(feature = "unsafe_flight")]
    /// Serialisation ID for Arcon's Unsafe In-flight serde
    const UNSAFE_SER_ID: SerId;
    /// Serialisation ID for Arcon's Reliable In-flight serde
    const RELIABLE_SER_ID: SerId;
    /// Current version of this ArconType
    const VERSION_ID: VersionId;

    /// Return the key of this ArconType
    fn get_key(&self) -> u64;
}
```

It is in the ``ArconTypeBounds`` trait that we set the requirement that the type must be a Protobuf supported message. This is done through the [prost](https://github.com/danburkert/prost) crate and its ``Message`` trait. More details on the ArconType may be found [here](https://cda-group.github.io/arcon/introduction/data_format.html#arcontype).

To implement an ArconType we use the ``Arcon`` derive macro as seen below.

```rust
#[arcon::proto]
#[derive(Arcon, Clone)]
pub struct Event {
  pub f1: u64,
  pub f2: String,
  pub f3: f64,
}
```

The above Rust struct is equivalent to the following Protobuf message:

```proto
message Event {
  uint64 f1 = 1;
  string f2 = 2;
  double f3 = 3;
}
```

Right, so at this point, we have defined our Protobuf data. Next, we'll go through the steps of the Arrow conversion.

## Working with Arrow

Converting the ``Event`` struct to Arrow data is not straightforward.
The Arrow [crate](https://github.com/apache/arrow/tree/master/rust/arrow) provides a bunch of low-level builder types that we can use. Let's go through an example where we build everything manually.


```rust
let capacity = 1024;

let mut f1_builder = UInt64Builder::new(capacity);
let mut f2_builder = StringBuilder::new(capacity);
let mut f3_builder = Float64Builder::new(capacity);

// Append a single value to each builder
f1_builder.append_value(1).unwrap();
f2_builder.append_value(String::from("data")).unwrap();
f3_builder.append_value(10.5).unwrap();

// Build the Arrays
let f1_array = f1_builder.finish();
let f2_array = f2_builder.finish();
let f3_array = f3_builder.finish();

// Define Schema
let schema = Arc::new(Schema::new(vec![
      Field::new("f1", DataType::UInt64, false),
      Field::new("f2", DataType::Utf8, false),
      Field::new("f3", DataType::Float64, false),
]));

// Build up an Arrow RecordBatch using our Arrays and Schema
let batch = RecordBatch::try_new(
    schema.clone(),
    vec![Arc::new(f1_array),
         Arc::new(f2_array),
         Arc::new(f3_array),
    ],
);
```
As you can see, the process of building up columnar data of the ``Event`` struct is a bit of a hassle. In Arcon, we have implemented an ``Arrow`` derive macro that hides this complexity from the user.

## Arrow Derive Macro

Before diving into the ``Arrow`` derive macro, we first have to look at the ``ToArrow`` trait that the macro implements.

```rust
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

// Macro used to implement ToArrow for non-struct types
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
```

The ``ToArrow`` trait has an associated type called Builder which tells us what kind of Arrow ArrayBuilder we are working with.

We have so far not talked about Arrow's [StructBuilder](https://docs.rs/arrow/3.0.0/arrow/array/struct.StructBuilder.html), 
which is at the core of the macro. If we look back at the ``Event`` struct then a StructBuilder would contain the following child field builders: 
``UInt64Builder``, ``StringBuilder``, and ``Float64Builder``. We may access each child builder by its index position as we will see soon.

The derive macro will generate all the ``ToArrow`` methods for the ``Event`` struct.  It is in the table method that we construct the StructBuilder.
As long as we provide the StructBuilder with all the correct Arrow Fields, it will then take care of initialising every child field builder.

```rust
// NOTE that this is Rust macro code that has yet to be expanded
fn table() -> ::arcon::MutableTable {
  let builder = ::arcon::StructBuilder::from_fields(#fields, ::arcon::RECORD_BATCH_SIZE);
  let table_name = stringify!(#name).to_lowercase();
  ::arcon::MutableTable::new(::arcon::RecordBatchBuilder::new(table_name, Self::schema(), builder))
}
```

A ``MutableTable`` then utilises the append method each time to add a record to the table. 
Down below you can see the generated code for the append method of ``Event``.

```rust
#[arcon::proto]
#[derive(Arcon, Arrow, Clone)]
pub struct Event {
  pub f1: u64,
  pub f2: String,
  pub f3: f64,
}

// Generated code
impl ToArrow for Event {
  fn append(self, builder: &mut StructBuilder) -> Result<(), ArrowError> {
    let value = self.f1;
    match builder.field_builder::<<u64>::Builder>(0) {
        Some(b) => b.append_value(value)?,
        None => return Err(::arcon::ArrowError::SchemaError(format!("Failed to downcast Arrow Builder"))),
    }

    let value = self.f2;
    match builder.field_builder::<<String>::Builder>(1) {
        Some(b) => b.append_value(value)?,
        None => return Err(::arcon::ArrowError::SchemaError(format!("Failed to downcast Arrow Builder"))),
    }

    let value = self.f3;
    match builder.field_builder::<<f64>::Builder>(2) {
        Some(b) => b.append_value(value)?,
        None => return Err(::arcon::ArrowError::SchemaError(format!("Failed to downcast Arrow Builder"))),
    }
    Ok(())
  }
  ...
}
```

Instead of working with low-level Arrow builders, we work directly with tables that hide the complexity of creating Arrow data.
In our manual example from [Working With Arrow](#working-with-arrow), we appended a single ``Event`` struct into Arrow data. By using our ``Arrow`` macro, the code can now be written as:

```rust
let mut table = Event::table();

let event = Event { f1: 1, f2: String::from("data"), f3: 10.5 };
table.append(event);
```


## Fixed Limitations

At first, we were not able to support ``String`` and ``Vec<u8>`` as their ArrayBuilder's were constrained to accept parameters as references while our macro passes things by value. This issue is now fixed as a [patch](https://github.com/apache/arrow/pull/9570) was contributed upstream to Arrow.

## Current Limitations

The ``Arrow`` derive macro cannot handle nested structs. That is, it does not know how to handle a StructBuilder within another StructBuilder.
We aim to fix this in the coming future, you can follow the progress [here](https://github.com/cda-group/arcon/issues/153).

## Conclusion

The Arrow format has grown into an industry-standard where essentially every new data processing system builds on top of it.
Therefore, it makes sense for Arcon to join the Arrow ecosystem so that it can benefit both from Columnar processing and the
possibility of sharing internal Arcon data with other systems that use Arrow.

## References

1.  Arcon's Github [repo](https://github.com/cda-group/arcon)
1.  Arcon's Arrow derive [macro](https://github.com/cda-group/arcon/blob/master/arcon_macros/src/arrow.rs).
2.  Arcon's table implemenation can be found [here](https://github.com/cda-group/arcon/blob/master/src/table/mod.rs).