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


/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}



/// A Python module implemented in Rust.
#[pymodule]
fn rbot(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(time_string, m)?)?;
    m.add_function(wrap_pyfunction!(init_log, m)?)?;
    m.add_class::<Order>()?;
    m.add_class::<OrderSide>()?;
    m.add_class::<FtxMarket>()?;
    Ok(())
}

#[pymodule]
fn raw(_py: Python, m: &PyModule) -> PyResult<()> {
    // m.add_class::()

    Ok(())
}
