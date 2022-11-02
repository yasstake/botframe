
use pyo3::pyfunction;
use log::{LevelFilter};
use simple_logger::SimpleLogger;

pub mod time;
pub mod order;

#[pyfunction]
pub fn init_log() {
    let _ = SimpleLogger::new().with_level(LevelFilter::Debug).init();
}
