use arcon::prelude::*;
use arrow::util::pretty;

#[derive(ArconState)]
pub struct State<B: Backend> {
    #[table = "mydata"]
    values: EagerValue<MyData, B>,
}

impl<B: Backend> StateConstructor for State<B> {
    type BackendType = B;

    fn new(backend: Arc<Self::BackendType>) -> Self {
        Self {
            values: EagerValue::new("_values", backend),
        }
    }
}

#[cfg_attr(feature = "arcon_serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "unsafe_flight", derive(abomonation_derive::Abomonation))]
#[derive(Arcon, Arrow, prost::Message, Clone)]
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
    #[prost(string, tag = "5")]
    pub some_str: String,
    #[prost(bytes, tag = "6")]
    pub bytes: Vec<u8>,
}

impl MyData {
    pub fn new(id: u64, data: f32, flag: bool, other: i64) -> Self {
        Self {
            id,
            data,
            flag,
            other,
            some_str: String::from("my_string"),
            bytes: vec![0, 1, 0, 1, 0],
        }
    }
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let mut table = ArrowTable::from(vec![
        MyData::new(1, 1.5, true, -100),
        MyData::new(2, 2.5, false, 190),
    ]);

    let mut ctx = ExecutionContext::new();
    let provider = table.mem_table()?;
    ctx.register_table(table.name(), Box::new(provider));

    let df = ctx.table("mydata")?;

    let filter = col("id").eq(lit(1));

    let df = df
        .select_columns(&["id", "other", "some_str"])?
        .filter(filter)?;
    let results = df.collect().await?;

    println!("DATAFRAME:");
    pretty::print_batches(&results)?;

    let sql_query = ctx.sql("SELECT id,other,some_str FROM mydata WHERE id = 1")?;
    let sql_result = sql_query.collect().await?;

    println!("SQL:");
    pretty::print_batches(&sql_result)?;

    Ok(())
}
