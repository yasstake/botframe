

use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use rusqlite::{params, Connection, Result, Error, params_from_iter};
use crate::common::order::Trade;
use crate::common::time::{MicroSec, FLOOR, to_seconds, NOW, time_string};
use crate::OrderSide;

#[derive(Debug, Clone, Copy)]
pub struct Ohlcvv {
    pub time: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub vol: f64,
    pub sell_vol: f64,
    pub sell_count: f64,
    pub buy_vol: f64,
    pub buy_count: f64,
    pub start_time: f64,
    pub end_time: f64,
}

impl Ohlcvv {
    pub fn new() -> Self {
        Ohlcvv { 
            time: 0.0, 
            open: 0.0, 
            high: 0.0, 
            low: 0.0, 
            close: 0.0, 
            vol: 0.0, 
            sell_vol: 0.0,
            sell_count: 0.0, 
            buy_vol: 0.0,
            buy_count: 0.0,
            start_time: 0.0,
            end_time: 0.0 
        }
    }

    pub fn append(&mut self, trade: &Trade) {
        if self.start_time == 0.0 || (trade.time as f64) < self.start_time {
            self.start_time = trade.time as f64;
            self.open = trade.price;
        }

        if self.end_time == 0.0 || self.end_time < trade.time as f64 {
            self.end_time = trade.time as f64;
            self.close = trade.price;
        }

        if self.high < trade.price {
            self.high = trade.price;
        }

        if trade.price < self.low || self.low == 0.0 {
            self.low = trade.price;
        }

        self.vol += trade.size;

        if  trade.order_side == OrderSide::Sell {
            self.sell_vol += trade.size;
            self.sell_count += 1.0;
        }
        else if trade.order_side == OrderSide::Buy {
            self.buy_vol += trade.size;
            self.buy_count += 1.0;
        }

    }
}

pub struct TradeTable {
    connection: Connection,
}

impl TradeTable {
    pub fn open(name: &str) -> Result<Self, Error> {
        let result = Connection::open(name);

        match result {
            Ok(conn) => {
                return Ok(TradeTable {
                    connection: conn,
                })
            },
            Err(e) => {
                println!("{:?}", e);
                return Err(e);
            }
        }
    }

    pub fn create_table_if_not_exists(&self) {
        let _r = self.connection.execute(
            "CREATE TABLE IF NOT EXISTS trades (
                time_stamp    INTEGER,
                action  TEXT,
                price   NUMBER,
                size    NUMBER,
                liquid  BOOL DEFAULT FALSE,
                id      TEXT primary key
            )",
            (),
        );

        let _r = self.connection.execute(
            "CREATE index if not exists time_index on trades(time_stamp)",
            (),
        );

    }

    pub fn drop_table(&self) {
        let _r = self.connection.execute(
            "drop table trades",
            ()
        );
    }

    pub fn recreate_table(&self) {
        self.create_table_if_not_exists();
        self.drop_table();
        self.create_table_if_not_exists();
    }

    //
    // 時間選択は左側は含み、右側は含まない。
    // 0をいれたときは全件検索
    pub fn select_time(&mut self, from_time: MicroSec, to_time: MicroSec) {
        let mut sql = "";
        let mut param= vec![];

        if 0 < to_time {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp and time_stamp < $2";
            param = vec![from_time, to_time];
        }
        else {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp ";
            param = vec![from_time];
        }

        let mut statement = self.connection.prepare(sql).unwrap();

        let _transaction_iter = statement.query_map(params_from_iter(param.iter()), |row| {

            let bs_str: String = row.get_unwrap(1);
            let bs = OrderSide::from_str(bs_str.as_str());

            Ok(Trade {
                time: row.get_unwrap(0),
                price: row.get_unwrap(2),
                size: row.get_unwrap(3),
                order_side: bs,
                liquid: row.get_unwrap(4),
                id: row.get_unwrap(5)
            })
        }).unwrap();

        for trade in _transaction_iter {
            println!("{:?}", trade);
        }
    }

    /// Tickバーをベースにしたohlcvを生成する。
    /// TODO: not yet to implement
    pub fn select_ohlcv_tick(&mut self, to_time: MicroSec, windows_sec: i64, num_bar: i64) -> Vec<Ohlcvv> {
     //   let from_time = 
     vec![]
    }

    pub fn info(&mut self) -> String {
        //let sql = "select min(time_stamp), max(time_stamp), count(*) from trades";
        let sql = "select min(time_stamp), max(time_stamp), count(*) from trades";        
        
        let r = self.connection.query_row(sql, [], |row|{
            let min: i64 = row.get_unwrap(0);
            let max: i64 = row.get_unwrap(1);
            let count: i64 = row.get_unwrap(2);

            Ok(format!("{{\"start\": {}, \"end\": {}, \"count\": {}}}", time_string(min), time_string(max), count))
        });

        // println!("{}", r.unwrap());

        return r.unwrap();
    }

    pub fn select_ohlcvv(&mut self, from_time: MicroSec, to_time: MicroSec, windows_sec: i64) -> Vec<Ohlcvv> {
        let mut sql = "";
        let mut param= vec![];

        if 0 < to_time {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp and time_stamp < $2";
            param = vec![from_time, to_time];
        }
        else {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp ";
            param = vec![from_time];
        }

        let mut statement = self.connection.prepare(sql).unwrap();

        let time_iters= statement.query_map(params_from_iter(param.iter()), |row| {
            let bs_str: String = row.get_unwrap(1);
            let bs = OrderSide::from_str(bs_str.as_str());

            Ok(Trade {
                time: row.get_unwrap(0),
                price: row.get_unwrap(2),
                size: row.get_unwrap(3),
                order_side: bs,
                liquid: row.get_unwrap(4),
                id: row.get_unwrap(5)
            })
        }).unwrap();

        let mut ohlcvv: Vec<Ohlcvv> = vec![];
        let mut chunk: Ohlcvv = Ohlcvv::new();

        // TODO: implement time window
        for trade in time_iters {
            if chunk.time == 0.0 {
                
                
            }

            let t = trade.unwrap();
            chunk.append(&t);
            // chunk = Ohlcvv::new();
        }

        ohlcvv.push(chunk);
        return ohlcvv;

    }
    
    pub fn select_ohlcvv2(&mut self, from_time: MicroSec, to_time: MicroSec, windows_sec: i64) -> Vec<Ohlcvv> {
        let mut ohlcvv: Vec<Ohlcvv> = vec![];

        let mut chunk = Ohlcvv::new();

        self.select(from_time, to_time, |trade: &Trade| {
            let trade_chunk_time = to_seconds(FLOOR(trade.time, windows_sec));
            if chunk.time == 0.0 {
                chunk.time = trade_chunk_time;
            } else if chunk.time != trade_chunk_time {
                ohlcvv.push(chunk);
                chunk = Ohlcvv::new();
            }
            
            chunk.append(&trade);
        });

        if chunk.time != 0.0 {
            ohlcvv.push(chunk);
        }

        return ohlcvv;
    }

    pub fn select<F>(&mut self, from_time: MicroSec, to_time: MicroSec, mut f: F) where F: FnMut(&Trade) {
        let mut sql = "";
        let mut param= vec![];

        if 0 < to_time {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp and time_stamp < $2 order by time_stamp";
            param = vec![from_time, to_time];
        }
        else {
            //sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp order by time_stamp";
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp";
            param = vec![from_time];
        }

        let mut statement = self.connection.prepare(sql).unwrap();        

        let start_time = NOW();

        let _transaction_iter = statement.query_map(params_from_iter(param.iter()), |row| {

            let bs_str: String = row.get_unwrap(1);
            let bs = OrderSide::from_str(bs_str.as_str());

            Ok(Trade {
                time: row.get_unwrap(0),
                price: row.get_unwrap(2),
                size: row.get_unwrap(3),
                order_side: bs,
                liquid: row.get_unwrap(4),
                id: row.get_unwrap(5)
            })
        }).unwrap();

        log::debug!("create iter {} microsec", NOW() - start_time);

        for trade in _transaction_iter {
            match trade {
                Ok(mut t) => {
                    f(&t);
                },
                Err(e) => log::error!("{:?}", e)
            }
        }
    }


    /*
    pub fn select(&mut self, from_time: MicroSec, to_time: MicroSec) -> Rows {
        let mut sql = "";
        let mut param= vec![];

        if 0 < to_time {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp and time_stamp < $2";
            param = vec![from_time, to_time];
        }
        else {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp ";
            param = vec![from_time];
        }

        let mut statement = self.connection.prepare(sql).unwrap();

        // let time_iters= statement.query_map(params_from_iter(param.iter()), conv);
        //return Box::new(statement.query(params_from_iter(param.iter())));
        let iters = statement.query(params_from_iter(param.iter())).unwrap();

        return iters;
    }
    */

    pub fn insert_records(&mut self, trades: &Vec<Trade>) -> Result<(), Error> {
        let mut tx = self.connection.transaction()?;

        let sql =   r#"insert or replace into trades (time_stamp, action, price, size, liquid, id)
                                values (?1, ?2, ?3, ?4, ?5, ?6) "#;

        for rec in trades {
            let _size = tx.execute(sql, params![
                rec.time,
                rec.order_side.to_string(),
                rec.price,
                rec.size,
                rec.liquid,
                rec.id
            ]);
        }
        tx.commit()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///   Test Suite
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


#[cfg(test)]
mod test_transaction_table {
    use std::sync::mpsc;
    use std::sync::mpsc::{Receiver, Sender};
    use std::thread;

    
    use crate::fs::db_full_path;
    use crate::common::time::NOW;
    use crate::common::init_log;

    use super::*;

    #[test]
    fn test_open() {
        TradeTable::open("test.db");
    }

    #[test]
    fn test_create_table_and_drop() {
        let tr = TradeTable::open("test.db").unwrap();

        tr.create_table_if_not_exists();
        tr.drop_table();
    }

    #[test]
    fn test_insert_table() {
        let mut tr = TradeTable::open("test.db").unwrap();
        &tr.recreate_table();

        let rec1 = Trade::new(1, 10.0, 10.0, OrderSide::Buy, false, "abc1".to_string());
        let rec2 = Trade::new(2, 10.1, 10.2, OrderSide::Buy, false, "abc2".to_string());
        let rec3 = Trade::new(3, 10.2, 10.1, OrderSide::Buy, false, "abc3".to_string());

        &tr.insert_records(&vec![rec1, rec2, rec3]);
    }

    #[test]
    fn test_select_table() {
        test_insert_table();

        let mut table = TradeTable::open("test.db").unwrap();
        println!("0-0");
        table.select_time(0, 0);
        println!("1-0");
        table.select_time(1, 0);
        println!("2-0");
        table.select_time(2, 0);
        println!("1-3");
        table.select_time(1, 3);
    }

    #[test]
    fn test_select_fn() {
        test_insert_table();

        let mut table = TradeTable::open("test.db").unwrap();
        println!("0-0");

        table.select(0, 0, |row|{println!("{:?}",row)});
    }

    #[test]
    fn test_info() {
        let db_name = db_full_path("FTX", "BTC-PERP");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        println!("{}", db.info());
    }


    #[test]
    fn test_select_ohlcv() {
        let db_name = db_full_path("FTX", "BTC-PERP");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let ohlcv = db.select_ohlcvv(0, 0, 120);

        println!("{:?}", ohlcv);
    }

    #[test]
    fn test_select_ohlcv2() {
        let db_name = db_full_path("FTX", "BTC-PERP");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start = NOW();
        let ohlcv = db.select_ohlcvv2(0, 0, 6000);
        
        println!("{:?} / {} microsec", ohlcv, NOW()-start);
    }

    #[test]
    fn test_select_ohlcv3() {
        init_log();

        let db_name = db_full_path("FTX", "BTC-PERP");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start = NOW();
        let ohlcv = db.select(0, 0, |_trade|{});
        
        println!("{:?} / {} microsec", ohlcv, NOW()-start);
    }


    #[test]
    fn test_send_channel() {
        let (tx, rx): (Sender<Trade>, Receiver<Trade>) = mpsc::channel();

        let th = thread::Builder::new().name("FTx".to_string());
        
        let handle = th.spawn(move || {
            for i in 0..100 {
                let trade = Trade::new(i, 10.0, 10.0, OrderSide::Buy, false, "abc1".to_string());
                println!("<{:?}", trade);
                let _= tx.send(trade);
            }
        }).unwrap();

        for recv_rec in rx {
            println!(">{:?}", recv_rec);
        }

        handle.join().unwrap();
    }


    #[test]
    fn test_send_channel_02() {
        let (tx, rx): (Sender<Trade>, Receiver<Trade>) = mpsc::channel();

        let handle = thread::spawn(move || {
            for recv_rec in rx {
                println!(">{:?}", recv_rec);
            }
        });

        for i in 0..100 {
            let trade = Trade::new(i, 10.0, 10.0, OrderSide::Buy, false, "abc1".to_string());
            println!("<{:?}", trade);
            tx.send(trade);
        }

        // handle.join().unwrap();　// 送信側がスレッドだとjoinがうまくいかない。
        thread::sleep(std::time::Duration::from_secs(5));
    }




// 10秒以上間があいている場所の検出SQL
// select time_stamp, sub_time from(select time_stamp, time_stamp - lag(time_stamp, 1, 0) OVER (order by time_stamp) sub_time  from trades order by time_stamp) where sub_time > 100000000;

}

