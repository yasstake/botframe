
use rusqlite::{Connection, Result, Statement, Transaction, Params};
use directories::ProjectDirs;

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


