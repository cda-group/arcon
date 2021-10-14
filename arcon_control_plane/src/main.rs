#[macro_use]
extern crate clap;
use arcon::control_plane::{conf::ControlPlaneConf, ControlPlane};
use clap::{App, AppSettings, Arg};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("arcon_control_plane")
        .setting(AppSettings::ColoredHelp)
        .version(crate_version!())
        .about("Control Plane for Coordinating Arcon Applications")
        .arg(
            Arg::with_name("conf")
                .help("Path to Config File")
                .long("conf file")
                .short("c")
                .takes_value(true),
        )
        .get_matches();

    let conf = match matches.value_of("conf") {
        Some(path) => ControlPlaneConf::from_file(path),
        None => ControlPlaneConf::default(),
    };

    let plane = ControlPlane::new(conf);

    plane.run();

    Ok(())
}
