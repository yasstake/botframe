
use pyo3::prelude::*;


pub mod db;

use crate::db::*;


/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn test() -> PyResult<()>{
    db::main_2();
 
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn sandbox(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}