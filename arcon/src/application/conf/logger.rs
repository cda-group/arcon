use serde::Deserialize;
use slog::{o, Drain, Fuse, Logger};
use slog_async::Async;
use std::{fs::OpenOptions, sync::Arc};

/// Alias for logger in Arcon
pub type ArconLogger = Logger<std::sync::Arc<Fuse<Async>>>;

pub const ARCON_LOG_NAME: &str = "arcon.log";
pub const KOMPACT_LOG_NAME: &str = "kompact.log";

/// Defines a logger type
#[derive(Deserialize, Clone, Copy, Debug)]
pub enum LoggerType {
    /// Logs output directly to the terminal
    Terminal,
    /// Logs output to file
    File,
}

impl Default for LoggerType {
    fn default() -> Self {
        LoggerType::Terminal
    }
}
pub fn term_logger() -> ArconLogger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();

    slog::Logger::root_typed(
        Arc::new(drain),
        o!(
        "location" => slog::PushFnValue(|r: &slog::Record<'_>, ser: slog::PushFnValueSerializer<'_>| {
            ser.emit(format_args!("{}:{}", r.file(), r.line()))
        })),
    )
}

pub fn file_logger(log_path: &str) -> ArconLogger {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .unwrap();

    let decorator = slog_term::PlainDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();

    slog::Logger::root_typed(
        Arc::new(drain),
        o!(
        "location" => slog::PushFnValue(|r: &slog::Record<'_>, ser: slog::PushFnValueSerializer<'_>| {
            ser.emit(format_args!("{}:{}", r.file(), r.line()))
        })),
    )
}
