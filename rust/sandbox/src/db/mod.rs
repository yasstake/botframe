use rusqlite::{Connection, Result, Statement, Transaction, Params};

pub mod exec;

#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

#[test]
fn test_make_db() {}

#[test]
fn main_test() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    conn.execute(
        "CREATE TABLE person (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            data  BLOB
        )",
        (), // empty list of parameters.
    )?;

    let mut tx = conn.transaction()?;

    for i in 0..1_000_000 {
        let me = Person {
            id: i,
            name: "Steven".to_string(),
            data: None,
        };

        tx.execute(
            "INSERT INTO person (name, data) VALUES (?1, ?2)",
            (&me.name, &me.data),
        )?;
    }

    tx.commit();


    let mut stmt = conn.prepare("SELECT id, name, data FROM person where id=1000")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }
    Ok(())
}

struct MyAppState {
    db: Connection,
}


impl MyAppState {
    fn new() -> MyAppState {
        let db = Connection::open(":memory:").unwrap();
        MyAppState { db: db }
    }
}

struct PreparedStatement<'conn> {
    statement: Statement<'conn>,
}

impl<'conn> PreparedStatement<'conn> {
    pub fn new<'a>(conn: &'a Connection, sql: &str) -> PreparedStatement<'a> {
        PreparedStatement {
            statement: conn.prepare(sql).unwrap(),
        }
    }
/*
    fn execute(&self, params: &Params) {
        self.statement.execute(params);
    }
  */      
    fn query_some_info(&mut self, arg: i64) -> i64 {
        let mut result_iter = self.statement.query(&[&arg]).unwrap();
        let result: i64= result_iter.next().unwrap().unwrap().get(0).unwrap();

        result
    }
}


pub fn main_2() {
    let mut db = Connection::open(":memory:").unwrap();    
    let tx = db.transaction().unwrap();
    
    let mut prepared_stmt = PreparedStatement::new(&tx, "SELECT ? + 1");
    for i in 0..100 {
        let result = prepared_stmt.query_some_info(i);
        println!("{}", result);
    }

    // tx.commit();
}





struct TransactionDb<'a> {
    conn: Connection,
    insert_statement: Statement<'a>
}

/*
impl <'a>TransactionDb<'a> {
    fn open() -> Self {
        let conn = Connection::open_in_memory().unwrap(); 
        let statement= conn.prepare("INSERT INTO person (name, data) VALUES (?, ?)").unwrap();
        
        return TransactionDb {
            conn: conn,
            insert_statement: statement
        }
    }


}
*/


fn prepare_insert(conn: &mut Connection) -> Result<Statement> {
    let mut statement = conn.prepare("INSERT INTO person (name, data) VALUES (?, ?)");

    return statement;
}

fn prepare_select(conn: &mut Connection) -> Result<Statement> {
    let mut statement = conn.prepare("SELECT id, name, data FROM person where id=1000");

    return statement;
}

#[test]
fn main_test2() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    conn.execute(
        "CREATE TABLE person (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            data  BLOB
        )",
        (), // empty list of parameters.
    )?;

    let mut tx = conn.transaction()?;
    // let mut statement = prepare_insert(&mut tx)?;

    for i in 0..1_000_000 {
        let me = Person {
            id: i,
            name: "Steven".to_string(),
            data: None,
        };
        let mut statement = tx.prepare("INSERT INTO person (name, data) VALUES (?, ?)")?;
        statement.execute((me.name, me.data))?;
    }

    tx.commit();

    Ok(())
}

/*
    let (conn, stmt) = prepare_select(&mut conn);

    let person_iter = stmt.unwrap().query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }
    Ok(())
}
*/
