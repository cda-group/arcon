// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::arrow::ArrowOps;
use arrow::{
    array::{ArrayBuilder, StructBuilder},
    datatypes::Schema,
    error::ArrowError,
    ipc::{
        convert::*,
        reader::read_record_batch,
        writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
    record_batch::RecordBatch,
};
use datafusion::{datasource::MemTable, error::DataFusionError};
use std::{convert::TryFrom, sync::Arc};

// Size for each RecordBatch in Arrow
pub const RECORD_BATCH_SIZE: usize = 1024;

/// A Mutable Append Only Table
#[derive(Debug)]
pub struct MutableTable {
    /// Builder used to append data to the table
    builder: RecordBatchBuilder,
    /// Stores appended record batches
    batches: Vec<RecordBatch>,
}
impl MutableTable {
    /// Creates a new MutableTable
    pub fn new(builder: RecordBatchBuilder) -> Self {
        Self {
            builder,
            batches: Vec::new(),
        }
    }

    /// Append a single element to the table
    #[inline]
    pub fn append(&mut self, elem: impl ArrowOps) -> Result<(), ArrowError> {
        if self.builder.len() == RECORD_BATCH_SIZE {
            let batch = self.builder.record_batch()?;
            self.batches.push(batch);
        }

        self.builder.append(elem)?;
        Ok(())
    }
    /// Load elements into the table from an Iterator
    #[inline]
    pub fn load(
        &mut self,
        elems: impl IntoIterator<Item = impl ArrowOps>,
    ) -> Result<(), ArrowError> {
        for elem in elems {
            self.append(elem)?;
        }
        Ok(())
    }

    // internal helper to finish last batch
    fn finish(&mut self) -> Result<(), ArrowError> {
        if !self.builder.is_empty() {
            let batch = self.builder.record_batch()?;
            self.batches.push(batch);
        }
        Ok(())
    }

    /// Converts the MutableTable into an ImmutableTable
    pub fn to_immutable(mut self) -> Result<ImmutableTable, ArrowError> {
        self.finish()?;

        Ok(ImmutableTable {
            name: self.builder.name().to_string(),
            schema: self.builder.schema(),
            batches: self.batches,
        })
    }
}

#[derive(Debug)]
pub struct RecordBatchBuilder {
    table_name: String,
    schema: Arc<Schema>,
    builder: StructBuilder,
}

impl RecordBatchBuilder {
    pub fn new(table_name: String, schema: Schema, builder: StructBuilder) -> Self {
        Self {
            table_name,
            schema: Arc::new(schema),
            builder,
        }
    }
    #[inline]
    pub fn append(&mut self, elem: impl ArrowOps) -> Result<(), ArrowError> {
        elem.append(&mut self.builder)?;
        self.builder.append(true)
    }
    pub fn name(&self) -> &str {
        &self.table_name
    }
    pub fn len(&self) -> usize {
        self.builder.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    pub fn set_name(&mut self, name: &str) {
        self.table_name = name.to_string();
    }
}

/// An Immutable Table
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
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
}

/// Restore a ImmutableTable from a RawTable
impl TryFrom<RawTable> for ImmutableTable {
    type Error = ArrowError;

    fn try_from(table: RawTable) -> Result<Self, Self::Error> {
        let s = schema_from_bytes(&table.schema).map_err(|e| ArrowError::IoError(e.to_string()))?;
        let schema_ref = Arc::new(s);
        let dict_fields = Vec::new();

        let mut batches = Vec::with_capacity(table.batches.len());

        for raw in table.batches {
            let message = arrow::ipc::root_as_message(&raw.ipc_message)
                .map_err(|e| ArrowError::IoError(e.to_string()))?;

            match message.header_type() {
                arrow::ipc::MessageHeader::RecordBatch => {
                    if let Some(batch) = message.header_as_record_batch() {
                        let record_batch = read_record_batch(
                            &raw.arrow_data,
                            batch,
                            schema_ref.clone(),
                            &dict_fields,
                        )
                        .map_err(|e| ArrowError::IoError(e.to_string()))?;
                        batches.push(record_batch);
                    } else {
                        return Err(ArrowError::IoError(
                            "Failed to match RecordBatch".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(ArrowError::IoError(
                        "Matched unexpected ipc message".to_string(),
                    ))
                }
            }
        }

        Ok(ImmutableTable {
            name: table.name,
            schema: schema_ref,
            batches,
        })
    }
}

/// A Raw version of [ImmutableTable] that can be persisted to disk or sent over the wire.
#[derive(prost::Message, Clone)]
pub struct RawTable {
    #[prost(string)]
    pub name: String,
    #[prost(bytes)]
    pub schema: Vec<u8>,
    #[prost(message, repeated)]
    pub batches: Vec<RawRecordBatch>,
}

impl TryFrom<ImmutableTable> for RawTable {
    type Error = ArrowError;

    fn try_from(table: ImmutableTable) -> Result<Self, Self::Error> {
        let ipc = IpcDataGenerator::default();
        let write_options = IpcWriteOptions::default();
        let mut tracker = DictionaryTracker::new(false);

        let encoded_data = ipc.schema_to_bytes(&*table.schema(), &write_options);
        let raw_schema = encoded_data.ipc_message;

        let mut raw_batches: Vec<RawRecordBatch> = Vec::with_capacity(table.batches.len());

        for batch in table.batches.iter() {
            let (_, encoded_data) = ipc
                .encoded_batch(&batch, &mut tracker, &write_options)
                .map_err(|e| ArrowError::IoError(e.to_string()))?;

            raw_batches.push(RawRecordBatch {
                ipc_message: encoded_data.ipc_message,
                arrow_data: encoded_data.arrow_data,
            });
        }

        Ok(RawTable {
            name: table.name.clone(),
            schema: raw_schema,
            batches: raw_batches,
        })
    }
}

/// A Raw version of an Arrow RecordBatch
#[derive(prost::Message, Clone)]
pub struct RawRecordBatch {
    #[prost(bytes)]
    pub ipc_message: Vec<u8>,
    #[prost(bytes)]
    pub arrow_data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToArrow;
    use datafusion::datasource::TableProvider;

    #[derive(Arrow, Clone)]
    pub struct Event {
        pub id: u64,
        pub data: f32,
    }

    #[test]
    fn table_serde_test() {
        let mut table = Event::table();
        let events = 1548;
        for i in 0..events {
            let event = Event {
                id: i as u64,
                data: 1.0,
            };
            table.append(event).unwrap();
        }

        let immutable: ImmutableTable = table.to_immutable().unwrap();
        let raw_table: RawTable = RawTable::try_from(immutable).unwrap();
        let back_to_immutable: ImmutableTable = ImmutableTable::try_from(raw_table).unwrap();
        // create a mem table and check that we have correct number of rows...
        let mem_table = back_to_immutable.mem_table().unwrap();
        assert_eq!(mem_table.statistics().num_rows, Some(1548));
    }
}
