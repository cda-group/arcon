use arcon::prelude::*;

fn main() {
    let path = "guide/examples/arcon.conf";
    let conf = ApplicationConf::from_file(path);
    let _app = Application::with_conf(conf);
}
