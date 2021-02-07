use arcon::prelude::*;
use arrow::util::pretty;
use datafusion::{datasource::MemTable, prelude::*};

#[derive(Arcon, Arrow, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct MyData {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(float, tag = "2")]
    pub data: f32,
    #[prost(bool, tag = "3")]
    pub flag: bool,
    #[prost(sint64, tag = "4")]
    pub other: i64,
}

impl MyData {
    pub fn new(id: u64, data: f32, flag: bool, other: i64) -> Self {
        Self {
            id,
            data,
            flag,
            other,
        }
    }
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let mut table = ArrowTable::from(vec![
        MyData::new(1, 1.5, true, -100),
        MyData::new(2, 2.5, false, 190),
    ]);

    let batch = table.record_batch()?;
    let mut ctx = ExecutionContext::new();
    let provider = MemTable::try_new(table.schema(), vec![vec![batch]])?;

    ctx.register_table("mydata_table", Box::new(provider));
    let df = ctx.table("mydata_table")?;

    let filter = col("id").eq(lit(1));

    let df = df.select_columns(vec!["id", "other"])?.filter(filter)?;
    let results = df.collect().await?;

    pretty::print_batches(&results)?;

    Ok(())
}
