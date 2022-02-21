use crate::data::arrow::ToArrow;
use arrow::{
    array::{ArrayBuilder, StructBuilder},
    datatypes::Schema,
    error::ArrowError,
    ipc::{
        convert::*,
        reader::{read_record_batch, FileReader},
        writer::{DictionaryTracker, FileWriter, IpcDataGenerator, IpcWriteOptions},
    },
    record_batch::RecordBatch,
};
use parquet::arrow::ParquetFileArrowReader;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::SerializedFileReader;
use parquet::{arrow::arrow_writer::ArrowWriter, errors::ParquetError};
use std::fs::File;
use std::path::Path;
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
    pub fn append(&mut self, elem: impl ToArrow, timestamp: Option<u64>) -> Result<(), ArrowError> {
        if self.builder.len() == RECORD_BATCH_SIZE {
            let batch = self.builder.record_batch()?;
            self.batches.push(batch);
        }

        self.builder.append(elem, timestamp)?;
        Ok(())
    }
    /// Load elements into the table from an Iterator
    #[inline]
    pub fn load(
        &mut self,
        elems: impl IntoIterator<Item = impl ToArrow>,
    ) -> Result<(), ArrowError> {
        for elem in elems {
            self.append(elem, None)?;
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
    pub fn immutable(mut self) -> Result<ImmutableTable, ArrowError> {
        self.finish()?;

        Ok(ImmutableTable {
            name: self.builder.name().to_string(),
            schema: self.builder.schema(),
            batches: self.batches,
        })
    }

    #[inline]
    pub fn batches(&mut self) -> Result<Vec<RecordBatch>, ArrowError> {
        self.finish()?;
        let mut batches = Vec::new();
        std::mem::swap(&mut batches, &mut self.batches);
        Ok(batches)
    }

    #[inline]
    pub fn raw_batches(&mut self) -> Result<Vec<RawRecordBatch>, ArrowError> {
        self.finish()?;
        let batches = self.batches()?;
        to_raw_batches(batches)
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
    pub fn append(&mut self, elem: impl ToArrow, timestamp: Option<u64>) -> Result<(), ArrowError> {
        elem.append(&mut self.builder, timestamp)?;
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
    pub(crate) name: String,
    pub(crate) schema: Arc<Schema>,
    pub(crate) batches: Vec<RecordBatch>,
}

impl ImmutableTable {
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|r| r.num_rows()).sum()
    }
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
}

#[inline]
pub fn to_record_batches(
    schema: Arc<Schema>,
    raw_batches: Vec<RawRecordBatch>,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let dict_fields = Vec::new();
    let mut batches = Vec::with_capacity(raw_batches.len());
    for raw in raw_batches {
        let message = arrow::ipc::root_as_message(&raw.ipc_message)
            .map_err(|e| ArrowError::IoError(e.to_string()))?;

        match message.header_type() {
            arrow::ipc::MessageHeader::RecordBatch => {
                if let Some(batch) = message.header_as_record_batch() {
                    let record_batch =
                        read_record_batch(&raw.arrow_data, batch, schema.clone(), &dict_fields)
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
    Ok(batches)
}

#[inline]
pub fn to_raw_batches(batches: Vec<RecordBatch>) -> Result<Vec<RawRecordBatch>, ArrowError> {
    let mut raw_batches = Vec::with_capacity(batches.len());
    let ipc = IpcDataGenerator::default();
    let write_options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);

    for batch in batches {
        let (_, encoded_data) = ipc
            .encoded_batch(&batch, &mut tracker, &write_options)
            .map_err(|e| ArrowError::IoError(e.to_string()))?;

        raw_batches.push(RawRecordBatch {
            ipc_message: encoded_data.ipc_message,
            arrow_data: encoded_data.arrow_data,
        });
    }

    Ok(raw_batches)
}

/// Restore a ImmutableTable from a RawTable
impl TryFrom<RawTable> for ImmutableTable {
    type Error = ArrowError;

    fn try_from(table: RawTable) -> Result<Self, Self::Error> {
        let s = schema_from_bytes(&table.schema).map_err(|e| ArrowError::IoError(e.to_string()))?;
        let schema = Arc::new(s);
        let batches = to_record_batches(schema.clone(), table.batches)?;

        Ok(ImmutableTable {
            name: table.name,
            schema,
            batches,
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
                .encoded_batch(batch, &mut tracker, &write_options)
                .map_err(|e| ArrowError::IoError(e.to_string()))?;

            raw_batches.push(RawRecordBatch {
                ipc_message: encoded_data.ipc_message,
                arrow_data: encoded_data.arrow_data,
            });
        }

        Ok(RawTable {
            name: table.name,
            schema: raw_schema,
            batches: raw_batches,
        })
    }
}

#[allow(unused)]
pub fn write_arrow_file(path: impl AsRef<Path>, table: ImmutableTable) -> Result<(), ArrowError> {
    let file = File::create(path)?;
    let mut writer = FileWriter::try_new(file, &table.schema)?;
    for batch in table.batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

#[allow(unused)]
pub fn arrow_file_reader(path: impl AsRef<Path>) -> Result<FileReader<File>, ArrowError> {
    let file = File::open(path)?;
    FileReader::try_new(file)
}

#[allow(unused)]
pub fn write_parquet_file(
    path: impl AsRef<Path>,
    table: ImmutableTable,
    compression: bool,
) -> Result<(), ParquetError> {
    let file = File::create(path)?;
    let props = if compression {
        WriterProperties::builder()
            .set_compression(Compression::ZSTD)
            .build()
    } else {
        WriterProperties::builder().build()
    };

    let mut writer = ArrowWriter::try_new(file, table.schema, Some(props))?;
    for batch in table.batches {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

#[allow(unused)]
pub fn parquet_arrow_reader(
    path: impl AsRef<Path>,
) -> Result<ParquetFileArrowReader, ParquetError> {
    let file = File::open(path)?;
    let file_reader = SerializedFileReader::new(file)?;
    Ok(ParquetFileArrowReader::new(Arc::new(file_reader)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToArrow;
    use parquet::arrow::ArrowReader;
    use tempfile::tempdir;

    #[derive(Arrow, Clone)]
    pub struct Event {
        pub id: u64,
        pub data: f32,
    }

    fn test_table() -> MutableTable {
        let mut table = Event::table();
        let events = 1548;
        for i in 0..events {
            let event = Event {
                id: i as u64,
                data: 1.0,
            };
            table.append(event, None).unwrap();
        }
        table
    }

    #[test]
    fn arrow_file_test() {
        let table = test_table();
        let immutable = table.immutable().unwrap();
        let total_rows = immutable.total_rows();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("arrow_write");
        let reader_path = file_path.clone();
        // verify write
        assert!(write_arrow_file(file_path, immutable).is_ok());

        // verify rows
        let reader = arrow_file_reader(reader_path).unwrap();
        let rows: usize = reader.map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(rows, total_rows);
    }
    #[test]
    fn parquet_file_test() {
        let table = test_table();
        let immutable = table.immutable().unwrap();
        let schema = immutable.schema();
        let total_rows = immutable.total_rows();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("parquet_write");
        let reader_path = file_path.clone();

        // verify write
        assert!(write_parquet_file(file_path, immutable, true).is_ok());

        // verify schema
        let mut reader = parquet_arrow_reader(reader_path).unwrap();
        let reader_schema = reader.get_schema().unwrap();
        assert_eq!(schema, Arc::new(reader_schema));

        // verify rows
        let mut batch_reader = reader.get_record_reader(total_rows).unwrap();
        let batch = batch_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), total_rows);
    }

    #[test]
    fn table_serde_test() {
        let table = test_table();
        let immutable: ImmutableTable = table.immutable().unwrap();
        let total_rows = immutable.total_rows();
        let raw_table: RawTable = RawTable::try_from(immutable).unwrap();
        let back_to_immutable: ImmutableTable = ImmutableTable::try_from(raw_table).unwrap();
        assert_eq!(back_to_immutable.total_rows(), total_rows);
    }
}
