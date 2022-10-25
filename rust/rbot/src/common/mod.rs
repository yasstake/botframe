use log::{LevelFilter, SetLoggerError};
use simple_logger::SimpleLogger;

pub mod time;
pub mod order;


pub fn init_log() {
    let _ = SimpleLogger::new().with_level(LevelFilter::Debug).init();
}
