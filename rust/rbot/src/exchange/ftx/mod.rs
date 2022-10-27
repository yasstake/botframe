mod message;
mod rest;


use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};


use pyo3::*;

use crate::common::order::Trade;

#[pyclass]
pub struct Ftx {
    pub dummy: bool,
    tx: Sender<Vec<Trade>>,
    rx: Receiver<Vec<Trade>>,
}


#[pymethods]
impl Ftx {
    #[new]
    pub fn new(dummy: bool) -> Self {
        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();        
        return Ftx {
            dummy,
            tx,
            rx 
        }
    }

    pub fn load_log(&self, ndays: i32) {

    }
    

}


