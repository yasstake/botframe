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
use crate::common::time::NOW;
use crate::common::time::DAYS;
use crate::common::time::SEC;

use crate::db::df;
use crate::fs::db_full_path;
use crate::db::df::TradeBuffer;
use crate::db::sqlite::TradeTable;
use self::rest::{download_trade_callback_ndays, download_trade_chunks_callback};


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
    df: DataFrame,
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
            df: TradeBuffer::new().to_dataframe(),
        }
    }

    pub fn info(&mut self) -> String{
        return self.db.info();
    }

    pub fn load_log(&mut self, ndays: i32, force: bool) -> String {
        let market = self.name.to_string();
        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();        

        if force {
            log::debug!("Force donwload for {} days", ndays);
            let _handle = thread::spawn(move || {
                download_trade_callback_ndays(market.as_str(), ndays,
                    |trade| {
                        let _ = tx.send(trade);
                });
            });
        }
        else {
            log::debug!("Diff donwload for {} days", ndays);            
            let chunks = self.db.select_gap_chunks(
                NOW() - DAYS(ndays as i64),
                0,
                SEC(20)
            );

            log::debug!("{:?}", chunks);

            let _handle = thread::spawn(move || {
                download_trade_chunks_callback(market.as_str(), &chunks, 
                    |trade| {
                        let _ = tx.send(trade);
                });
            });
        }

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

    pub fn load_to_df(&mut self, from_time: MicroSec, to_time: MicroSec) {
        self.df = self.db.select_df(from_time, to_time);
    }
}




#[cfg(test)]
mod test_exchange_ftx{
    use crate::common::init_log;
    use crate::common::time::DAYS;
    use crate::db::df::ohlcv_df;

    use super::*;
    #[test]
    fn test_download_missing_chunks() {
        init_log();

        let mut ftx = FtxMarket::new("BTC-PERP", true);

        ftx.load_log(60, false);
    }


    #[test]
    fn test_load_to_df() {
        let mut ftx = FtxMarket::new("BTC-PERP", true);

        ftx.load_to_df(NOW()-DAYS(1), 0);

        println!("{:?}", ftx.df);

        let ohlcv = ohlcv_df(&ftx.df, 0, 0, 10);

        println!("{:?}", ohlcv);
    }

}