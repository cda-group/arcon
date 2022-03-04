#[arcon::app(name = "file")]
fn main() {
    LocalFileSource::new("file_path")
        .to_stream(|conf| conf.set_arcon_time(ArconTime::Process))
        .filter(|x| *x > 50)
        .map(|x: i32| x * 10)
        .print()
}
