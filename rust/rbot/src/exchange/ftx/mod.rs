mod message;
mod rest;


use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::thread;



use pyo3::*;

use crate::common::order::Trade;
use crate::fs::db_full_path;
use crate::db::sqlite::TradeTable;
use crate::exchange::ftx::rest::download_trade_call;


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
            download_trade_call(market.as_str(), ndays, 
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


}




