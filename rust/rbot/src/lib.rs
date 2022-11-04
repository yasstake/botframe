// Copyright (C) @yasstake
// All rights reserved. Absolutely NO warranty.

pub mod common;
pub mod sim;
mod db;
mod exchange;
mod fs;

use pyo3::prelude::*;
use common::{
    order::{Order, OrderSide},
    time::time_string,
    init_log,
};
use exchange::ftx::FtxMarket;

use common::time::*;



/// A Python module implemented in Rust.
#[pymodule]
fn rbot(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_log, m)?)?;

    // time util
    m.add_function(wrap_pyfunction!(time_string, m)?)?;
    m.add_function(wrap_pyfunction!(NOW, m)?)?;    
    m.add_function(wrap_pyfunction!(DAYS, m)?)?;
    m.add_function(wrap_pyfunction!(HHMM, m)?)?;

    // classes
    m.add_class::<Order>()?;
    m.add_class::<OrderSide>()?;
    m.add_class::<FtxMarket>()?;    

    Ok(())
}

