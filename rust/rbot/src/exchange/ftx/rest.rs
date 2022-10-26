use std::thread::sleep;
use std::time::Duration;
use crate::common::time::{DAYS, HHMM, MicroSec, NOW, to_seconds, time_string};
use crate::common::order::Trade;
use crate::exchange::ftx::message::{FtxTrade, FtxTradeMessage};
use crate::db::sqlite::TradeTable;
use log;

const FTX_REST_ENDPOINT: &str = "https://ftx.com/api";

const BTCMARKET: &str = "BTC-PERP";


fn download_trade_ndays(market_name: &str, ndays: i64, db: &mut TradeTable) {
    log::debug!("download_trade_ndays {}", ndays);
    let start_time = NOW() - DAYS(ndays) - HHMM(0, 10);
    let mut end_time = NOW() + HHMM(0, 5);   // 5分後を指定し最新を取得。

    loop {
        let timer_start = NOW();

        log::debug!("download trade to: {}", time_string(end_time));

        let trades = download_trade(market_name, start_time, end_time);
        let trade_len = trades.len();
        end_time = trades[trade_len -1].time;

        db.insert_records(&trades);

        let lap_time = NOW() - timer_start;
        log::debug!("{} trades / {} [us]", trade_len, lap_time);

        if trade_len  <= 100 ||  end_time <= start_time {
            break;
        }

        sleep(Duration::from_millis(10));
    }
}

fn download_trade_call<F>(market_name: &str, ndays: i64, mut f: F) where F: FnMut(Vec<Trade>) {
    log::debug!("download_trade_ndays {}", ndays);
    let start_time = NOW() - DAYS(ndays) - HHMM(0, 10);
    let mut end_time = NOW() + HHMM(0, 5);   // 5分後を指定し最新を取得。

    loop {
        let timer_start = NOW();

        log::debug!("download trade to: {}", time_string(end_time));

        let mut trades = download_trade(market_name, start_time, end_time);
        let trade_len = trades.len();
        end_time = trades[trade_len -1].time;

        f(trades);

        let lap_time = NOW() - timer_start;
        log::debug!("{} trades / {} [us]", trade_len, lap_time);

        if trade_len  <= 100 ||  end_time <= start_time {
            break;
        }

        sleep(Duration::from_millis(10));
    }
}



/// TODO: エラーハンドリング（JSONエラー/503エラーの場合、現在はPanicしてしまう）
/// start_time, to_timeは秒単位のため開始は切り捨て、終了は切り上げする。
/// そのため、重複が発生するのであとでIDで重複削除する必要がある。
fn download_trade(market_name: &str, from_microsec: MicroSec, to_microsec: MicroSec ) -> Vec<Trade> {
    let start_sec = to_seconds(from_microsec) as i64;
    let end_sec = to_seconds(to_microsec) as i64 + 1;

    let url = format!("{}/markets/{}/trades?start_time={}&end_time={}", FTX_REST_ENDPOINT, market_name, start_sec, end_sec);
    log::debug!("{}", url);

    let response = reqwest::blocking::get(url);

    return match response {
        Ok(response) => {
            match response.text() {
                Ok(res) => {
                    match serde_json::from_str::<FtxTradeMessage>(res.as_str()) {
                        Ok(mut message) => message.get_trades(),
                        Err(e) => {
                            log::warn!("log history format(json) error = {}/{}", e, res);
                            vec![]
                        }
                    }
                }
                Err(e) => {
                    log::warn!("log history format(json) error = {}", e);
                    vec![]
                }
            }
        },
        Err(e) => {
            log::warn!("download history error = {:?}", e);
            vec![]
        }
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///   Test Suite
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod test_ftx_client {
    use super::*;
    use std::sync::mpsc;
    use std::sync::mpsc::{Receiver, Sender};
    use std::thread;
    use std::io::Cursor;
    use crate::common::init_log;
    use crate::common::order::OrderSide;
    use crate::common::time::{DAYS, NOW};
    use crate::exchange::ftx::message::FtxTradeMessage;
    use crate::fs::db_full_path;
    use crate::time_string;
    use crate::db::sqlite::TradeTable;

    #[test]
    fn test_download_trade() {
        init_log();
        let to_time = NOW() + HHMM(0, 5);
        let from_time = to_time - DAYS(10);

        let trades = download_trade(BTCMARKET,from_time, to_time);
        log::debug!("FROM: {:?} {:?}", trades[trades.len() -1].time, time_string(trades[trades.len() -1].time));
        log::debug!("TO  : {:?} {:?}", trades[0].time, time_string(trades[0].time));
        log::debug!("Trade len = {:?}", trades.len());
        log::debug!("FIRST= {:?}", trades[0]);
        log::debug!("LAST = {:?}", trades[trades.len() -1]);

        let to_time = trades[trades.len() - 1].time;
        let trades = download_trade(BTCMARKET,from_time, to_time);
        log::debug!("FROM: {:?} {:?}", trades[trades.len() -1].time, time_string(trades[trades.len() -1].time));
        log::debug!("TO  : {:?} {:?}", trades[0].time, time_string(trades[0].time));
        log::debug!("Trade len = {:?}", trades.len());
        log::debug!("FIRST= {:?}", trades[0]);
        log::debug!("LAST = {:?}", trades[trades.len() -1]);

        let to_time = trades[trades.len() - 1].time;
        let trades = download_trade(BTCMARKET,from_time, to_time);
        log::debug!("FROM: {:?} {:?}", trades[trades.len() -1].time, time_string(trades[trades.len() -1].time));
        log::debug!("TO  : {:?} {:?}", trades[0].time, time_string(trades[0].time));
        log::debug!("Trade len = {:?}", trades.len());
        log::debug!("FIRST= {:?}", trades[0]);
        log::debug!("LAST = {:?}", trades[trades.len() -1]);

    }


/*
    #[tokio::test]
    async fn test_download_ndays () {
        init_log();

        log::debug!("begin test");
        let callback = |t: Vec<Trade>| {/*println!("{:?}", t);*/};

        download_trade_ndays(BTCMARKET, 0, callback).await;
        log::debug!("end test");

        log::debug!("begin test");
        let callback = |t: Vec<Trade>| {/*println!("{:?}", t);*/};

        download_trade_ndays(BTCMARKET, 1, callback).await;
        log::debug!("end test");
    }
*/

    #[test]
    fn store_db() {
        init_log();

        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        db.create_table_if_not_exists();

        download_trade_ndays(BTCMARKET, 1, &mut db);
    }

    #[test]
    fn call_back() {
        init_log();

        download_trade_call(BTCMARKET, 0, |trade| {println!("{:?}", trade)});
    }

    #[test]
    fn call_back_db() {
        init_log();

        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        db.create_table_if_not_exists();

        download_trade_call(BTCMARKET, 1, |trade| {db.insert_records(&trade)});
    }

    #[test]
    fn call_back_db_thread() {
        init_log();

        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();

        // Txがあるかぎり、rxはエラーを出さない。
        //let tx2 = tx.clone();
        
        let handle = thread::spawn(move || {
            download_trade_call(BTCMARKET, 0, |trade| {tx.send(trade);});
        });

        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        db.create_table_if_not_exists();

        loop {
            match rx.recv() {
                Ok(trades) => db.insert_records(&trades),
                Err(e) => {println!("{:?}", e); break;}
            }
        }
    }



}
