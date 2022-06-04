/*
Bybitのメッセージフォーマットについて

約定履歴は、過去ログ、RESTAPI、WSの３つの方法で取得できるが
それぞれ、取得可能時間、メッセージフォーマットが異なる。


（３つをマージして使うのは困難）

・過去ログ (昨日以前のものが取得可能)
＜サンプル＞
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
1651449601,BTCUSD,Sell,258,38458.00,MinusTick,a0dd4504-db3c-535f-b43b-4de38f581b79,670861.7192781736,258,0.006708617192781736


・RESTAPI　（おおむね直近３０分程度のログが取得できる）
＜サンプル＞
{"ret_code":0,"ret_msg":"OK","ext_code":"","ext_info":"","result":[{"id":66544931,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:53.165Z"},{"id":66544930,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:53.058Z"},{"id":66544929,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.954Z"},{"id":66544928,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.85Z"},{"id":66544927,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.747Z"},{"id":66544926,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.646Z"},{"id":66544925,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.536Z"},

https://api.bybit.com/v2/public/trading-records?symbol=BTCUSD

KLine形式で１分足ならばもっと長期間のログが取得可能。


・WS（リアルタイム：過去は取得付加）

{"topic":"ParseTradeMessage.BTCUSD",
 "data":[
       {"trade_time_ms":1619398389868,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":2000,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"8241a632-9f07-5fa0-a63d-06cefd570d75","cross_seq":6169452432},
       {"trade_time_ms":1619398389947,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":200,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"ff87be41-8014-5a33-b4b1-3252a6422a41","cross_seq":6169452432}]}
]
}



*/

use anyhow::Context;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("{msg:}")]
struct ParseError {
    msg: String,
}

use crate::exchange::Trade;
use crate::exchange::{BUY, SELL};

//------------------------------------------------------------------------
// Parse log file
// example:
//
// timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
//1651449601,BTCUSD,Sell,258,38458.00,MinusTick,a0dd4504-db3c-535f-b43b-4de38f581b79,670861.7192781736,258,0.006708617192781736

fn is_valid_log_header(rec: &str) -> bool {
    let header = [
        "timestamp",
        "symbol",
        "side",
        "size",
        "price",
        "tickDirection",
        "trdMatchID",
        "grossValue",
        "homeNotional",
        "foreignNotional",
    ];

    let row = rec.split(",");
    for (i, col) in row.enumerate() {
        if header[i] != col {
            return false;
        }
    }

    return true;
}

pub fn parse_log_rec(rec: &str) -> anyhow::Result<Trade> {
    const NUM_OF_REC: i32 = 10;
    let row = rec.split(",");

    let mut time_ns: i64 = 0;
    let mut price: f32 = 0.0;
    let mut size: f32 = 0.0;
    let mut bs: i32 = 0;
    let mut id: String = "".to_string();

    for (i, col) in row.enumerate() {
        match i {
            0 => {
                /*timestamp*/
                time_ns = col.parse::<i64>()?;
            }
            1 => { /* symbol IGNORE */ }
            2 => {
                /* side */
                match col {
                    "Buy" => bs = BUY,
                    "Sell" => bs = SELL,
                    _ => return Err(anyhow!("log record error {} {}", col, rec)),
                }
            }
            3 => {
                /* size */
                size = col.parse::<f32>()?;
            }
            4 => {
                /* price */
                price = col.parse::<f32>()?;
            }
            5 => { /* tickDirection IGNORE */ }
            6 => {
                /* trdMatchID */
                id = col.to_string();
            }
            7 => { /* grossValue IGNORE */ }
            8 => { /* homeNotional IGNORE */ }
            9 => { /* foreignNotional IGNORE */ }
            _ => {
                /* ERROR */
                return Err(anyhow!("log record error {} {}", col, rec));
            }
        }
    }

    return Ok(Trade {
        time_ns: time_ns,
        price: price,
        size: size,
        bs: bs,
        id: id,
    });
}

#[test]
fn test_is_valid_hedader() {
    const HEADER1: &str = "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional";
    assert!(is_valid_log_header(HEADER1));

    const HEADER2: &str = "stamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional";
    assert!(is_valid_log_header(HEADER2) == false);
}

#[test]
fn test_from_csv_rec() {
    const TRADE_LINE: &str = "1651449616,BTCUSD,Buy,1,38463.50,ZeroMinusTick,cb731e0e-55e6-551f-81c8-286b9e20e361,2599.867406762255,1,2.599867406762255e-05";

    let rec = parse_log_rec(TRADE_LINE).unwrap();

    println!("{:?}", &rec);
}

//------------------------------------------------------------------------
// WebSocket JSON format
// Transaction
// Sample
//
// {"topic":"ParseTradeMessage.BTCUSD",
// "data":[
//       {"trade_time_ms":1619398389868,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":2000,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"8241a632-9f07-5fa0-a63d-06cefd570d75","cross_seq":6169452432},
//       {"trade_time_ms":1619398389947,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":200,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"ff87be41-8014-5a33-b4b1-3252a6422a41","cross_seq":6169452432}]}
//　]
//}

//------------------------------------------------------------------------
// GUID
pub struct TransactionId {
    id: u128,
}

impl TransactionId {
    pub fn to_ids(self) -> (i64, i64) {
        let upper = (self.id >> 64) as i64;
        let lower = self.id as i64;

        return (upper, lower);
    }

    fn from_str(id: &str) -> Result<TransactionId, ParseError> {
        fn hex_to_char(c: char) -> Result<i32, ParseError> {
            let num: i32;
            match c {
                '0' => num = 0,
                '1' => num = 1,
                '2' => num = 2,
                '3' => num = 3,
                '4' => num = 4,
                '5' => num = 5,
                '6' => num = 6,
                '7' => num = 7,
                '8' => num = 8,
                '9' => num = 9,
                'a' => num = 10,
                'b' => num = 11,
                'c' => num = 12,
                'd' => num = 13,
                'e' => num = 14,
                'f' => num = 15,
                'A' => num = 10,
                'B' => num = 11,
                'C' => num = 12,
                'D' => num = 13,
                'E' => num = 14,
                'F' => num = 15,
                _ => {
                    println!("Out of hex range ->{}", c);
                    return Err(ParseError {
                        msg: String::from(c),
                    });
                }
            }

            Ok(num)
        }

        fn parse_hex_string(h: &str) -> Result<u128, ParseError> {
            let mut num: u128 = 0;

            for c in h.chars() {
                num <<= 4;
                let hex = hex_to_char(c)? as u128;
                num += hex;
            }

            Ok(num)
        }

        //　　　　　　　"00c706e1-ba52-5bb0-98d0-bf694bdc69f7";
        //            |   |   ||   ||   ||   ||   |   |
        // hyphens  : |   |   8|  13|  18|  23|   |   |
        // positions: 0   4    9   14   19   24  28  32   36
        let mut hex_id: u128 = 0;

        let id1_str: &str = &id[0..8];
        let n = parse_hex_string(id1_str)?;
        hex_id += n;

        let id2_str: &str = &id[9..13];
        hex_id <<= 4 * 4;
        let n = parse_hex_string(id2_str)?;
        hex_id += n;

        let id3_str: &str = &id[14..18];
        hex_id <<= 4 * 4;
        let n = parse_hex_string(id3_str)?;
        hex_id += n;

        let id4_str: &str = &id[19..23];
        hex_id <<= 4 * 4;
        let n = parse_hex_string(id4_str)?;
        hex_id += n;

        let id5_str: &str = &id[24..];
        hex_id <<= 4 * 12;
        let n = parse_hex_string(id5_str)?;
        hex_id += n;

        Ok(TransactionId { id: hex_id })
    }

    pub fn to_str(self) -> String {
        format!("{:032x}", self.id)
    }
}

#[test]
fn test_transaction_id() {
    const ID: &str = "00c706e1-ba52-5bb0-98d0-bf694bdc69f7";
    const ID2: &str = "00c706e1ba525bb098d0bf694bdc69f7";
    let id = TransactionId::from_str(ID).unwrap();
    let id_str = id.to_str();
    println!("{}-{}", ID, id_str);

    assert_eq!(ID2, id_str);

    // illeagal format case
    const ID3: &str = "00c706e1-ba52-5bb0-98d0-bf694bdc69fz";
    let id = TransactionId::from_str(ID3);

    assert!(id.is_err());
}

#[test]
fn test_all() {}
