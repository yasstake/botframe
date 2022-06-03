
fn main() {

}

/*
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
1651449601,BTCUSD,Sell,258,38458.00,MinusTick,a0dd4504-db3c-535f-b43b-4de38f581b79,670861.7192781736,258,0.006708617192781736
*/

#[macro_use] extern crate polars;
use polars::prelude::*;
use polars_lazy::prelude::*;
use polars::frame::DataFrame;
use polars::prelude::Result as PolarResult;
use polars::prelude::SerReader;


fn create_schema() -> Schema {
    let s = Schema::new(vec! [
        Field::new("id1", DataType::UInt64),
        Field::new("id2", DataType::UInt64),        
        Field::new("timestamp", DataType::Date64),
        Field::new("price", DataType::Float64),
        Field::new("volume", DataType::Float64),                
        Field::new("side", DataType::UInt16),
    ]);

    

    return s;
}

#[test]
fn create_test_data() {


    let mut col1 :Vec<i64> = vec![];

    col1.push(1);
    col1.push(1);
    col1.push(1);
    col1.push(1);            

    let s1 = Series::new("col1", col1);

    let mut col2 :Vec<i64> = vec![];
    col2.push(1);
    col2.push(1);
    col2.push(1);
    col2.push(1);   
    let s2= Series::new("col2", col2);    
    
    /*
    let df = df!(
        "id" => &col1,
        "data" => &col2
    );
    */
}


use std::env;
use std::fs;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use flate2::bufread::GzDecoder;


#[test]
fn load_file() {
    // --snip--
    let filename = "./TESTDATA/BTCUSD2022-05-02.csv.gz";

    let path = Path::new(filename);

    let f = File::open(path).unwrap();
    let mut buf_read = std::io::BufReader::new(f);
    let mut gzip_reader = std::io::BufReader::new(GzDecoder::new(buf_read)).lines();

    for l in gzip_reader {
        let ln = l.unwrap();

        println!("{}", ln)
    }
}



#[derive(Debug)]
enum TradeType {
    Buy = 1,
    Sell = 2
}


#[derive(Debug)]
struct TradeRecord {
    id1: i64,
    id2: i64,
    trade_time_ns: i64,
    size: f32,
    price: f32,
    side: TradeType,
}


impl TradeRecord {
    fn is_valid_header(rec: &str) -> bool {
        let header = ["timestamp", "symbol", "side", "size", "price", "tickDirection",
                                "trdMatchID","grossValue","homeNotional","foreignNotional"];

        let row = rec.split(",");
        for (i, col) in row.enumerate() {
            if header[i] != col {
                return false;
            } 
        }

        return true;
    }

    fn from_csv_rec(rec: &str) -> TradeRecord {
        let row = rec.split(",");

        for col in row {
            println!("{}", col);
        }

        /*
        id1: i64,
        id2: i64,
        trade_time_ns: i64,
        size: f32,
        price: f32,
        side: i16,
        */
        return TradeRecord{
            id1: 1651535588,
            id2: 1651535588,
            trade_time_ns: 1651535588,
            size: 5.0,
            price: 38511.50,
            side: TradeType::Buy
        };
    }
}

#[test]
fn test_is_valid_hedader() {
    const HEADER1: &str = "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional";
    assert!(TradeRecord::is_valid_header(HEADER1));

    const HEADER2: &str = "stamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional";
    assert!(TradeRecord::is_valid_header(HEADER2) == false);
}


#[test]
fn test_from_csv_rec() {
    const TRADE_LINE: &str = "1651449616,BTCUSD,Buy,1,38463.50,ZeroMinusTick,cb731e0e-55e6-551f-81c8-286b9e20e361,2599.867406762255,1,2.599867406762255e-05";

    let rec = TradeRecord::from_csv_rec(TRADE_LINE);
    println!("{:?}", &rec);
}
