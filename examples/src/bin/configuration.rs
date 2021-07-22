use arcon::prelude::*;

fn main() {
    let path = "guide/examples/arcon.conf";
    let conf = ArconConf::from_file(path);
    let _pipeline = Pipeline::with_conf(conf);
}
