use arcon::prelude::*;

fn main() {
    let mut app = Application::default()
        .file("file_source_data", |cfg| {
            cfg.set_arcon_time(ArconTime::Process);
        })
        .flat_map(|x| (0..x))
        .to_console();

    app.start();
    app.await_termination();
}
