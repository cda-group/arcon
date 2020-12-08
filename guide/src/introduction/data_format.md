# Data Format

The main data format of Arcon is Protobuf. There are several reasons behind this choice and they are listed below:

1.  Universal data format that enables the data to be moved outside of the Rust ecosystem. 
2.  Supports schema evolution.
3.  Good space utilisation on disk.
4.  Decent serialisation/deserialisation cost.


Serialised size of two different Rust structs ([Reference](https://github.com/cda-group/arcon/blob/master/benches/serde.rs)):

| Framework       |  Small |  Large |
| ------------- |-------------| -----|
| Protobuf  | 14 bytes   |   106 bytes
| Serde::Bincode   | 20 bytes | 228 bytes


Arcon uses the [prost]() crate to be able to define its data types directly in Rust or through a .proto file.

```rust,edition2018,no_run,noplaypen
#[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Arcon, prost::Message, Clone, abomonation_derive::Abomonation)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct Input {
  #[prost(uint32, tag = "1")]
  pub id: u32,
}
```


## In-flight data

Arcon provides two in-flight serde modes, Unsafe and Reliable. The former uses the very unsafe and fast [Abomonation](https://github.com/TimelyDataflow/abomonation) crate to serialise data, while the latter depends on a more reliable but also slower option, Protobuf.

NOTE: Currently on local deployment is supported for Arcon.

## Serde Support



