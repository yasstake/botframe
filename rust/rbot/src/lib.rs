// Copyright (C) @yasstake
// All rights reserved. Absolutely NO warranty.

use common::{
    order::{Currency, MarketType, Order, OrderSide},
    time::time_string,
};
use pyo3::prelude::*;

pub mod common;
pub mod sim;
mod db;

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
    m.add_class::<Order>()?;
    m.add_class::<MarketType>()?;
    m.add_class::<OrderSide>()?;
    m.add_class::<Currency>()?;
    m.add_class::<MarketType>()?;
    Ok(())
}
