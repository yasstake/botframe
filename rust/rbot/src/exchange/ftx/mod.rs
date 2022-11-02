mod message;
mod rest;


use std::f64::consts::E;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::thread;



use pyo3::*;
use pyo3::prelude::*;
use pyo3::exceptions::PySystemError;
use pyo3::exceptions::PyValueError;


use crate::common::order::Trade;
use crate::common::time::MicroSec;
use crate::db::df;
use crate::fs::db_full_path;
use crate::db::sqlite::TradeTable;
use self::rest::download_trade_callback_ndays;

use polars::prelude::DataFrame;
use polars::prelude::Float64Type;
use crate::db::df::KEY;

use numpy::IntoPyArray;
use numpy::PyArray2;



#[pyclass]
pub struct FtxMarket {
    name: String,
    pub dummy: bool,
    db: TradeTable,
}


#[pymethods]
impl FtxMarket {
    #[new]
    pub fn new(market_name: &str, dummy: bool) -> Self {
        let db_name = db_full_path("FTX", &market_name);

        let db = TradeTable::open(db_name.to_str().unwrap()).expect("cannot open db");
        db.create_table_if_not_exists();

        return FtxMarket {
            name: market_name.to_string(),
            dummy,
            db,
        }
    }

    pub fn info(&mut self) -> String{
        return self.db.info();
    }

    pub fn load_log(&mut self, ndays: i32) -> String {
        let market = self.name.to_string();
        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();        

        let _handle = thread::spawn(move || {
            download_trade_callback_ndays(market.as_str(), ndays,
                |trade| {
                    let _ = tx.send(trade);
            });
        });

        loop {
            match rx.recv() {
                Ok(trades) => {let _ = &self.db.insert_records(&trades);},
                Err(e) => {break;}
            }
        }

        return self.db.info();
    }

    pub fn select_trades(&mut self, from_time: MicroSec, to_time: MicroSec) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.db.select_array(from_time, to_time);

        let r = Python::with_gil(|py| {

            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });    

        return Ok(r);
    }
}




