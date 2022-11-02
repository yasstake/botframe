use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use crate::common::order::{TimeChunk, Trade};
use crate::common::time::{time_string, to_seconds, MicroSec, FLOOR, MICRO_SECOND, NOW};
use crate::OrderSide;
use polars::prelude::DataFrame;
use rusqlite::{params, params_from_iter, Connection, Error, Result};

use super::df::OhlcvBuffer;
use crate::db::df::TradeBuffer;
use log::log_enabled;
use log::Level::Debug;

use crate::db::df::KEY;
use polars::prelude::Float64Type;

use numpy::IntoPyArray;
use numpy::PyArray2;

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
            end_time: 0.0,
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

        if trade.order_side == OrderSide::Sell {
            self.sell_vol += trade.size;
            self.sell_count += 1.0;
        } else if trade.order_side == OrderSide::Buy {
            self.buy_vol += trade.size;
            self.buy_count += 1.0;
        }
    }
}

fn check_skip_time(mut trades: &Vec<Trade>) {
    if log::log_enabled!(Debug) {
        let mut last_time: MicroSec = 0;
        let mut last_id: String = "".to_string();

        for t in trades {
            if last_time != 0 {
                if 1 * MICRO_SECOND < t.time - last_time {
                    log::debug!("GAP {} / {:?}", t.time - last_time, t);
                }
            }

            if last_id == t.id {
                log::debug!("DUPE {:?}", t);
            }

            last_time = t.time;
            last_id = t.id.clone();
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
            Ok(conn) => return Ok(TradeTable { connection: conn }),
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
        let _r = self.connection.execute("drop table trades", ());
    }

    pub fn recreate_table(&self) {
        self.create_table_if_not_exists();
        self.drop_table();
        self.create_table_if_not_exists();
    }

    // 時間選択は左側は含み、右側は含まない。
    // 0をいれたときは全件検索
    pub fn select<F>(&mut self, from_time: MicroSec, to_time: MicroSec, mut f: F)
    where
        F: FnMut(&Trade),
    {
        let mut sql = "";
        let mut param = vec![];

        if 0 < to_time {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp and time_stamp < $2 order by time_stamp";
            param = vec![from_time, to_time];
        } else {
            //sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp order by time_stamp";
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp";
            param = vec![from_time];
        }

        let mut statement = self.connection.prepare(sql).unwrap();

        let start_time = NOW();

        let _transaction_iter = statement
            .query_map(params_from_iter(param.iter()), |row| {
                let bs_str: String = row.get_unwrap(1);
                let bs = OrderSide::from_str(bs_str.as_str());

                Ok(Trade {
                    time: row.get_unwrap(0),
                    price: row.get_unwrap(2),
                    size: row.get_unwrap(3),
                    order_side: bs,
                    liquid: row.get_unwrap(4),
                    id: row.get_unwrap(5),
                })
            })
            .unwrap();

        log::debug!("create iter {} microsec", NOW() - start_time);

        for trade in _transaction_iter {
            match trade {
                Ok(mut t) => {
                    f(&t);
                }
                Err(e) => log::error!("{:?}", e),
            }
        }
    }

    pub fn select_df(&mut self, from_time: MicroSec, to_time: MicroSec) -> DataFrame {
        let mut buffer = TradeBuffer::new();

        self.select(from_time, to_time, |trade| {
            buffer.push_trade(trade);
        });

        return buffer.to_dataframe();
    }

    pub fn select_array(&mut self, from_time: MicroSec, to_time: MicroSec) -> ndarray::Array2<f64> {
        let trades = self.select_df(from_time, to_time);

        let array: ndarray::Array2<f64> = trades
            .select(&[
                KEY::time_stamp,
                KEY::price,
                KEY::size,
                KEY::order_side,
                KEY::liquid,
            ])
            .unwrap()
            .to_ndarray::<Float64Type>()
            .unwrap();

        array
    }

    /// TODO: not yet to implement
    pub fn select_ohlcv_tick(
        &mut self,
        to_time: MicroSec,
        windows_sec: i64,
        num_bar: i64,
    ) -> Vec<Ohlcvv> {
        //   let from_time =
        vec![]
    }

    pub fn info(&mut self) -> String {
        let sql = "select min(time_stamp), max(time_stamp), count(*) from trades";

        let r = self.connection.query_row(sql, [], |row| {
            let min: i64 = row.get_unwrap(0);
            let max: i64 = row.get_unwrap(1);
            let count: i64 = row.get_unwrap(2);

            Ok(format!(
                "{{\"start\": {}, \"end\": {}, \"count\": {}}}",
                time_string(min),
                time_string(max),
                count
            ))
        });

        return r.unwrap();
    }

    /// select min(start) time_stamp in db
    pub fn start_time(&self) -> Result<MicroSec, Error> {
        let sql = "select min(time_stamp) from trades";

        let r = self.connection.query_row(sql, [], |row| {
            let min: i64 = row.get_unwrap(0);
            Ok(min)
        });

        return r;
    }

    /// select max(end) time_stamp in db
    pub fn end_time(&self) -> Result<MicroSec, Error> {
        let sql = "select max(time_stamp) from trades";

        let r = self.connection.query_row(sql, [], |row| {
            let max: i64 = row.get_unwrap(0);
            Ok(max)
        });

        return r;
    }

    /// Find un-downloaded data time chunks.
    pub fn select_gap_chunks(
        &self,
        from_time: MicroSec,
        mut to_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        if to_time == 0 {
            to_time = NOW();
        }

        let mut chunk = self.find_time_chunk_from(from_time, to_time, allow_size);

        // no data in db
        if chunk.len() == 0 {
            return vec![TimeChunk {
                start: from_time,
                end: to_time,
            }];
        }

        // find in db
        let mut c = self.select_time_chunks_in_db(from_time, to_time, allow_size);
        chunk.append(&mut c);

        // find after db time
        let mut c = self.find_time_chunk_to(from_time, to_time, allow_size);
        chunk.append(&mut c);

        return chunk;
    }

    /// Find un-downloaded data chunks before db data.
    /// If db has no data, returns []
    pub fn find_time_chunk_from(
        &self,
        from_time: MicroSec,
        to_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let end_time = match self.start_time() {
            Ok(t) => t,
            Err(e) => to_time,
        };

        if from_time + allow_size <= end_time {
            return vec![TimeChunk {
                start: from_time,
                end: end_time,
            }];
        } else {
            return vec![];
        }
    }

    pub fn find_time_chunk_to(
        &self,
        from_time: MicroSec,
        to_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let end_time = match self.end_time() {
            Ok(t) => t,
            Err(e) => to_time,
        };

        if end_time + allow_size < to_time {
            return vec![TimeChunk {
                start: end_time,
                end: to_time,
            }];
        } else {
            return vec![];
        }
    }

    /// TODO: if NO db data, returns []
    pub fn select_time_chunks_in_db(
        &self,
        from_time: MicroSec,
        to_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let mut chunks: Vec<TimeChunk> = vec![];

        // make first chunk
        let start_time = match self.start_time() {
            Ok(t) => t,
            Err(e) => from_time,
        };

        // find select db gaps

        let sql = r#"
        select time_stamp, sub_time from (
            select time_stamp, time_stamp - lag(time_stamp, 1, 0) OVER (order by time_stamp) sub_time  
            from trades order by time_stamp) 
            where $1 < sub_time and $2 < time_stamp
        "#;

        let mut statement = self.connection.prepare(sql).unwrap();
        let mut param = vec![allow_size, start_time];

        let chunk_iter = statement
            .query_map(params_from_iter(param.iter()), |row| {
                let start_time: MicroSec = row.get_unwrap(0);
                let missing_width: MicroSec = row.get_unwrap(1);

                Ok(TimeChunk {
                    start: start_time,
                    end: start_time + missing_width,
                })
            })
            .unwrap();

        for chunk in chunk_iter {
            if chunk.is_ok() {
                chunks.push(chunk.unwrap());
            }
        }

        return chunks;
    }

    pub fn select_ohlcvv(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        windows_sec: i64,
    ) -> Vec<Ohlcvv> {
        let mut sql = "";
        let mut param = vec![];

        if 0 < to_time {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp and time_stamp < $2";
            param = vec![from_time, to_time];
        } else {
            sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp ";
            param = vec![from_time];
        }

        let mut statement = self.connection.prepare(sql).unwrap();

        let time_iters = statement
            .query_map(params_from_iter(param.iter()), |row| {
                let bs_str: String = row.get_unwrap(1);
                let bs = OrderSide::from_str(bs_str.as_str());

                Ok(Trade {
                    time: row.get_unwrap(0),
                    price: row.get_unwrap(2),
                    size: row.get_unwrap(3),
                    order_side: bs,
                    liquid: row.get_unwrap(4),
                    id: row.get_unwrap(5),
                })
            })
            .unwrap();

        let mut ohlcvv: Vec<Ohlcvv> = vec![];
        let mut chunk: Ohlcvv = Ohlcvv::new();

        // TODO: implement time window
        for trade in time_iters {
            if chunk.time == 0.0 {}

            let t = trade.unwrap();
            chunk.append(&t);
            // chunk = Ohlcvv::new();
        }

        ohlcvv.push(chunk);
        return ohlcvv;
    }

    pub fn select_ohlcvv2(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        windows_sec: i64,
    ) -> Vec<Ohlcvv> {
        let mut ohlcvv: Vec<Ohlcvv> = vec![];

        let mut chunk = Ohlcvv::new();

        self.select(from_time, to_time, |trade: &Trade| {
            let trade_chunk_time = FLOOR(trade.time, windows_sec) as f64;
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

    pub fn select_ohclv_df(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        windows_sec: i64,
    ) -> DataFrame {
        let ohlcvv = self.select_ohlcvv2(from_time, to_time, windows_sec);

        let mut buffer = OhlcvBuffer::new();

        buffer.push_trades(ohlcvv);

        return buffer.to_dataframe();
    }

    pub fn insert_records(&mut self, trades: &Vec<Trade>) -> Result<(), Error> {
        let mut tx = self.connection.transaction()?;

        let trades_len = trades.len();
        let mut insert_len = 0;

        check_skip_time(trades);

        let sql = r#"insert or replace into trades (time_stamp, action, price, size, liquid, id)
                                values (?1, ?2, ?3, ?4, ?5, ?6) "#;

        for rec in trades {
            let result = tx.execute(
                sql,
                params![
                    rec.time,
                    rec.order_side.to_string(),
                    rec.price,
                    rec.size,
                    rec.liquid,
                    rec.id
                ],
            );

            match result {
                Ok(size) => {
                    insert_len += size;
                }
                Err(e) => {
                    println!("insert error {}", e);
                    return Err(e);
                }
            }
        }

        let result = tx.commit();

        if result.is_err() {
            return result;
        }

        if trades_len != insert_len {
            println!("insert mismatch {} {}", trades_len, insert_len);
        }

        Ok(())
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

    use crate::common::init_log;
    use crate::common::time::time_string;
    use crate::common::time::DAYS;
    use crate::common::time::HHMM;
    use crate::common::time::MICRO_SECOND;
    use crate::common::time::NOW;
    use crate::fs::db_full_path;

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
    fn test_select_fn() {
        test_insert_table();

        let mut table = TradeTable::open("test.db").unwrap();
        println!("0-0");

        table.select(0, 0, |row| println!("{:?}", row));
    }

    #[test]
    fn test_select_array() {
        let db_name = db_full_path("FTX", "BTC-PERP");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let array = db.select_array(0, 0);

        println!("{:?}", array);
    }

    #[test]
    fn test_info() {
        let db_name = db_full_path("FTX", "BTC-PERP");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        println!("{}", db.info());
    }

    #[test]
    fn test_start_time() {
        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start_time = db.start_time();

        let s = start_time.unwrap();

        println!("{}({})", time_string(s), s);
    }

    #[test]
    fn test_select_gap_chunks() {
        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.select_gap_chunks(NOW() - DAYS(10), NOW(), 1_000_000 * 10);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!(
                "{:?}-{:?} ?start_time={:?}&end_time={:?}",
                time_string(c.start),
                time_string(c.end),
                (c.start / 1_000_000) as i32,
                (c.end / 1_000_000) as i32
            );
        }
    }

    #[test]
    fn test_select_time_chunk_from() {
        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.find_time_chunk_from(NOW() - DAYS(10), NOW(), 1_000_000 * 30);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_select_time_chunk_to() {
        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.find_time_chunk_to(NOW() - DAYS(10), NOW(), 1_000_000 * 120);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_select_time_chunks() {
        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.select_time_chunks_in_db(NOW() - HHMM(10, 0), NOW(), 1_000_000 * 15);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
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

        println!("{:?} / {} microsec", ohlcv, NOW() - start);
    }

    #[test]
    fn test_select_ohlcv3() {
        init_log();

        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start = NOW();
        let ohlcv = db.select(0, 0, |_trade| {});

        println!("{:?} / {} microsec", ohlcv, NOW() - start);
    }

    #[test]
    fn test_select_ohlcv4() {
        init_log();

        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start = NOW();
        let ohlcv = db.select_ohclv_df(0, 0, 10);

        println!("{:?}", ohlcv);
        println!("{} microsec", NOW() - start);
    }

    #[test]
    fn test_send_channel() {
        let (tx, rx): (Sender<Trade>, Receiver<Trade>) = mpsc::channel();

        let th = thread::Builder::new().name("FTX".to_string());

        let handle = th
            .spawn(move || {
                for i in 0..100 {
                    let trade =
                        Trade::new(i, 10.0, 10.0, OrderSide::Buy, false, "abc1".to_string());
                    println!("<{:?}", trade);
                    let _ = tx.send(trade);
                }
            })
            .unwrap();

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

    #[test]
    fn test_find_gap() {
        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let mut last_time = 0;
        db.select(0, 0, |trade| {
            if last_time != 0 {
                let gap = trade.time - last_time;
                if 5 * MICRO_SECOND < gap {
                    println!("MISS {} {} {}", trade.time, time_string(trade.time), gap);
                }
            }

            last_time = trade.time;
        });
    }

    #[test]
    fn test_select_df() {
        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let df = db.select_df(0, 0);

        println!("{:?}", df);
    }

    // 10秒以上間があいている場所の検出SQL
    // select time_stamp, sub_time from(select time_stamp, time_stamp - lag(time_stamp, 1, 0) OVER (order by time_stamp) sub_time  from trades order by time_stamp) where sub_time > 100000000;
}
