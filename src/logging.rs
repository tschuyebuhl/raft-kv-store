use slog::{Drain, Logger, o};

pub fn stdlog_to_slog() -> Logger {
    let drain = slog_stdlog::StdLog.fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(4096)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .fuse();
    Logger::root(drain, o!())
}
