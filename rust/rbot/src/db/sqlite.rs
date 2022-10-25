use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use rusqlite::{params, Connection, Result, Statement, Transaction, Params, Error, MappedRows, Row, params_from_iter};
use crate::common::order::Trade;
use crate::common::time::MicroSec;
use crate::OrderSide;



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

    pub fn insert_records(&mut self, trades: Vec<Trade>) {
        let mut statement = self.connection.prepare(
            r#"insert or replace into trades (time_stamp, action, price, size, liquid, id)
                     values (?1, ?2, ?3, ?4, ?5, ?6)
                "#
        ).unwrap();

        for rec in trades {
            println!("{:?}", rec);
            let _size = statement.execute(params![
                rec.time,
                rec.order_side.to_string(),
                rec.price,
                rec.size,
                rec.liquid,
                rec.id
            ]);
        }
    }
}




#[cfg(test)]
mod test_transaction_table {
    use std::sync::mpsc;
    use std::sync::mpsc::{Receiver, Sender};
    use std::thread;
    use chrono::Duration;
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

        &tr.insert_records(vec![rec1, rec2, rec3]);
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
    fn test_send_channel() {
        let (tx, rx): (Sender<Trade>, Receiver<Trade>) = mpsc::channel();

        let handle = thread::spawn(move || {
            for i in 0..100 {
                let trade = Trade::new(i, 10.0, 10.0, OrderSide::Buy, false, "abc1".to_string());
                println!("<{:?}", trade);
                tx.send(trade);
            }
        });

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
}

