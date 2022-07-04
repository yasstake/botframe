// Copyright(c) yasstake 2022. All rights reserved. (no warranty)

use polars_core::prelude::DataFrame;

use chrono::Duration;
use chrono::Utc;
use chrono::Datelike;

use crate::exchange::Market;
use crate::exchange::MarketInfo;
use crate::exchange::MaketAgent;
use crate::exchange::Trade;

use crate::bb::log::load_log_file;

use crate::bb::log::MarketType;


pub struct Bb {
    pub market: Market,
    market_type: MarketType,
}

impl Bb {
    pub fn new() -> Bb {
        return Bb {
            market: Market::new(),
            market_type: MarketType::BTCUSD
        };
    }

    pub fn set_market_type(&mut self, market_type: MarketType) {
        self.market_type = market_type;
    }

    pub fn get_market_type(&mut self) -> MarketType {
        return self.market_type.clone();
    }

    pub async fn download_exec_log(&mut self, yyyy: i32, mm: i32, dd: i32) {
        fn insert_callback(m: &mut Market, t: &Trade) {
            m.append_trade(t);
        }
        // then load log
        load_log_file(&self.market_type, yyyy, mm, dd, insert_callback, &mut self.market).await;
    }

    // 過去N日分のログをダウンロードし、Maketクラスへロードする。
    // 数個のログを確認したところ、概ねUTCで２：３０ごろに前日分のログができている。
    // 年のため生成までに４時間とみたてて計算する。
    pub async fn download_exec_log_ndays(&mut self, ndays: i32) {
        self.reset_df();

        let last_day = Utc::now() - Duration::days(1) - Duration::hours(4); // 4H to delivery log.

        for i in (0..ndays).rev() {
            let log_date = last_day - Duration::days(i as i64);
            let year = log_date.year();
            let month = log_date.month() as i32;
            let day = log_date.day() as i32;

            self.download_exec_log(year, month, day).await;
        }
    }
}

use polars_core::error::Result;

// Delegate to self.market  <Market>type
impl MarketInfo for Bb {
    fn _df(&mut self) -> DataFrame {
        return self.market._df();
    }

    fn _ohlcv(&mut self, current_time_ms: i64, width_sec: i64, count: i64) -> ndarray::Array2<f64>{
        return self.market._ohlcv(current_time_ms, width_sec, count);
    }

    fn start_time(&self) -> i64 {
        return self.market.start_time();
    }
    fn end_time(&self) -> i64 {
        return self.market.end_time();
    }

    fn for_each(&mut self, call_back: fn(time: i64, kind: &str, price: f64, size: f64), start_time_ms: i64, end_time_ms: i64){
        self.market.for_each(call_back, start_time_ms, end_time_ms);
    }

    fn reset_df(&mut self) {
        self.market.reset_df();
    }

    fn save(&mut self, file_name: &str)  -> Result<()> {
        return self.market.save(file_name);
    }

    fn load(&mut self, file_name: &str) -> Result<()> {
        return self.market.load(file_name);
    }

}


#[tokio::test]
async fn test_download_log_for_five_days() {
    // make instance of market

    let mut bb = Bb::new();

    bb.download_exec_log_ndays(5).await;
}



#[tokio::test]
async fn test_download_log() {
    // make instance of market
    let mut market = Market::new();

    fn insert_callback(m: &mut Market, t: &Trade) {
        m.append_trade(t);
    }
    // then load log
    load_log_file(&MarketType::BTCUSD, 2022, 6, 1, insert_callback, &mut market).await;
    load_log_file(&MarketType::BTCUSD, 2022, 6, 2, insert_callback, &mut market).await;
    load_log_file(&MarketType::BTCUSD, 2022, 6, 3, insert_callback, &mut market).await;
}

/*
#[tokio::main]
#[test]
pub async fn make_market() {
    // make instance of market
    let mut market = Market::new();

    fn insert_callback(m: &mut Market, t: &Trade) {
        m.append_trade(t);
        // println!("{} {} {} {}",t.time_ns, t.bs, t.price, t.size)
    }

    // then load log
    load_log_file(2022, 6, 1, insert_callback, &mut market).await;
    load_log_file(2022, 6, 2, insert_callback, &mut market).await;
    load_log_file(2022, 6, 3, insert_callback, &mut market).await;

    // insert log to market
}
*/


#[tokio::test]
async fn test_load_data_and_print() {
    let mut market = Bb::new();

    market.download_exec_log_ndays(2).await;
 
    market.market._print_head_history();
    market.market._print_tail_history();    

    println!("size={}", market.market.history_size());
}

