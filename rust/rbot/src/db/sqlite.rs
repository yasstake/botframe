
use rusqlite::{params, Connection, Result, Statement, Transaction, Params, Error, MappedRows, Row, params_from_iter};
use crate::common::order::Trade;
use crate::common::time::NanoSec;
use crate::OrderSide;

struct TradeTable {
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

    pub fn insert_records(&mut self, trades: Vec<Trade>) {

        let mut statement = self.connection.prepare(
            r#"insert or replace into trades (time_stamp, action, price, size, liquid, id)
                     values (?1, ?2, ?3, ?4, ?5, ?6)
                "#
        ).unwrap();

        for rec in trades {
            println!("{:?}", rec);
            let _size = statement.execute(params![
                rec.time_ns,
                rec.bs.to_string(),
                rec.price,
                rec.size,
                rec.liquid,
                rec.id
            ]);
        }
    }

    //
    // 時間選択は左側は含み、右側は含まない。
    // 0をいれたときは全件検索
    pub fn select_time(&mut self, from_time: NanoSec, to_time: NanoSec) {
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
                time_ns: row.get_unwrap(0),
                price: row.get_unwrap(2),
                size: row.get_unwrap(3),
                bs: bs,
                liquid: row.get_unwrap(4),
                id: row.get_unwrap(5)
            })
        }).unwrap();

        for trade in _transaction_iter {
            println!("{:?}", trade);
        }
    }
}



#[cfg(test)]
mod test_transaction_table {
    use std::sync::mpsc;
    use std::sync::mpsc::{Receiver, Sender};
    use std::thread;
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
    }
}


/*
struct ExecuteRec {
    time_ms: i64,
    bs: String,
    price: f64,
    size: f64,
    liquid: bool,
    id: String
}

#[derive(Debug)]
struct ExecuteTable {
    connection: Connection,
}

const INSERT_SQL: &str = "INSERT INTO exec(time, action, price, size, liquid, id) VALUES(?1, ?2, ?3, ?4, ?5, ?6)";


impl ExecuteTable {
    fn new(name: &str) -> Result<ExecuteTable> {

        let db_name = format!("{}.db", name);
        let db_path= ProjectDirs::from("net", "takibi", "rbot").unwrap().data_dir().join(db_name);

        match Connection::open(db_path) {
            Ok(conn) => {
                return  Ok(ExecuteTable {
                    connection: conn,
                });
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    fn create_if_not_exsits_table(&self) -> Result<usize, rusqlite::Error> {
        return self.connection.execute(
            "CREATE TABLE IF NOT EXISTS exec (
                time    INTEGER,
                action  TEXT,
                price   NUMBER,
                size    NUMBER,
                liquid  BOOL DEFAULT FALSE,
                id      TEXT
            )",
            (),
        );
    }

    fn drop_table(&self) -> Result<usize, rusqlite::Error> {
        return self.connection.execute(
            "DROP TABLE exec",
            ()
        );
    }

    fn transanction(&mut self) -> Transaction {
        return self.connection.transaction().unwrap();
    }

    fn insert(&self, rec: &ExecuteRec) -> Result<usize, rusqlite::Error> {
        let mut statement = self.connection.prepare(INSERT_SQL)?;

        return statement.execute((rec.time_ms, rec.bs.as_str(), rec.price, rec.size, rec.liquid, rec.id.as_str()));
    }


    fn batch_insert(&mut self, batch: Vec<&ExecuteRec>) -> Result<usize, rusqlite::Error> {
        let tx = self.transanction();
        let mut statement = tx.prepare(INSERT_SQL)?;

        let mut size: usize = 0;
        for rec in batch {
            size += statement.execute((rec.time_ms, rec.bs.as_str(), rec.price, rec.size, rec.liquid, rec.id.as_str()))?;
        }

        // tx.commit();

        Ok(size)
    }




}

#[cfg(test)]
mod ExecuteTableTest {
    use super::*;

    #[test]
    fn test_create() {
        match ExecuteTable::new("BB-BTCUSD") {
            Ok(table) => {
                println!("{:?}", table);
            },
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }

    #[test]
    fn test_create_table() {
        match ExecuteTable::new("BB-BTCUSD") {
            Ok(table) => {
                let r = table.create_if_not_exsits_table();
                assert!(r.is_ok());
            },
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }


    #[test]
    fn test_insert_table() {
        match ExecuteTable::new("BB-BTCUSD") {
            Ok(table) => {
                let r = table.create_if_not_exsits_table();
                assert!(r.is_ok());

                let rec = ExecuteRec{time_ms: 1, bs: "b".to_string(), price: 10.0, size: 10.0, liquid: false, id: "a".to_string()};
                let r = table.insert(&rec);

                assert!(r.is_ok());
            },
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }




}


fn connect() -> Connection{
    return Connection::open_in_memory().unwrap();
}

fn connect_tmp() -> Connection{
    return Connection::open("/tmp/exec.db").unwrap();
}


fn create_table(conn: &mut Connection) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS exec (
            time    INTEGER,
            action  TEXT,
            price   NUMBER,
            size    NUMBER,
            id      TEXT
        )",
        (),
    ).unwrap();
}


struct PreparedStatement<'conn> {
    insert_statement: Statement<'conn>
}

impl <'conn> PreparedStatement<'conn> {
    pub fn new<'a>(conn: &'a Connection) -> PreparedStatement<'a> {
        PreparedStatement {
            insert_statement: conn.prepare(
                "INSERT INTO exec(time, action, price, size, id)
                    VALUES(?1, ?2, ?3, ?4, ?5)"
            ).unwrap()
        }
    }

    pub fn insert(&mut self, time_ms: i64, action: &str, price: f64, size: f64, id: &str) {
        self.insert_statement.execute((time_ms, action, price, size, id)).unwrap();
    }
}

fn perform_transaction(conn: &Transaction) {
    let mut statement  = PreparedStatement::new(&conn);
    for i in 0..1_000_000 {
        statement.insert(i, "B", 100.0, 10.0, "safd");
    }

}


#[test]
fn create_test() {
    let mut conn = connect();

    create_table(&mut conn);
    // perform_transaction(&mut conn);
}

#[test]
fn insert_test() {
//    let mut conn = connect();
    let mut conn = connect_tmp();

    create_table(&mut conn);

    let mut tx = conn.transaction().unwrap();
    perform_transaction(&tx);
    tx.commit();
}



*/




