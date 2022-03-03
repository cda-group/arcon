#[arcon::app(name = "file")]
fn main() {
    (0..10u64)
        .to_stream(|conf| conf.set_arcon_time(ArconTime::Process))
        .filter(|x| *x > 50)
        .map(|x| x * 10)
        .print()
}
