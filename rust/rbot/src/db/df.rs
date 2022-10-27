

use polars::prelude::DataFrame;
use polars::prelude::Series;
use polars::prelude::NamedFrom;
use chrono::NaiveDateTime;
use crate::common::time::{MICRO_SECOND, MicroSec};

use super::sqlite::Ohlcvv;


///
/// SQL DBには、Tickデータを保存する。
/// インメモリーDBとしてPolarsを利用し、１秒足のOHLCV拡張を保存する。
/// 定期的に最新の情報をメモリに更新する（時間（秒）がインデックスキーとして上書きする）
struct TradeBuffer {
    pub time: Vec<f64>,
    pub open: Vec<f64>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub close: Vec<f64>,
    pub vol: Vec<f64>,
    pub sell_vol: Vec<f64>,
    pub sell_count: Vec<f64>,
    pub buy_vol: Vec<f64>,
    pub buy_count: Vec<f64>,
    pub start_time: Vec<f64>,
    pub end_time: Vec<f64>,
}

impl TradeBuffer {
    const fn new() -> TradeBuffer {
        return TradeBuffer {
            time: Vec::new(),
            open: Vec::new(),
            high: Vec::new(),
            low: Vec::new(),
            close: Vec::new(),
            vol: Vec::new(),
            sell_vol: Vec::new(),
            sell_count: Vec::new(),
            buy_vol: Vec::new(),
            buy_count: Vec::new(),
            start_time: Vec::new(),
            end_time: Vec::new(),
        };
    }

    fn clear(&mut self) {
        self.time.clear();
        self.open.clear();
        self.high.clear();
        self.low.clear();
        self.close.clear();
        self.vol.clear();
        self.sell_vol.clear();
        self.sell_count.clear();
        self.buy_vol.clear();
        self.buy_count.clear();
        self.start_time.clear();
        self.end_time.clear();
    }

    fn push_trade(&mut self, trade: &Ohlcvv) {
        self.time.push(trade.time);
        self.open.push(trade.open);
        self.high.push(trade.high);
        self.low.push(trade.low);
        self.close.push(trade.close);
        self.vol.push(trade.vol);
        self.sell_vol.push(trade.sell_vol);
        self.sell_count.push(trade.sell_count);
        self.buy_vol.push(trade.buy_vol);
        self.buy_count.push(trade.buy_count);
        self.start_time.push(trade.start_time);
        self.end_time.push(trade.end_time);
    }

    fn convert_datetime(t: Vec<f64>) -> Vec<NaiveDateTime> {
//        let datetime: Vec<NaiveDateTime> = 
        let datetime = 
            t.iter()
            .map(|x| 
                NaiveDateTime::from_timestamp((*x as MicroSec) / MICRO_SECOND, 
               (( (*x as MicroSec) % MICRO_SECOND) * 1_000) as u32).try_into().unwrap()) // sec, nano_sec
            .collect();

        return datetime;
    }

    fn to_dataframe(&self) -> DataFrame{
        let time = Series::new("time", TradeBuffer::convert_datetime(self.time.to_vec()));
        let open = Series::new("open", self.open.to_vec());
        let high = Series::new("high", self.high.to_vec());
        let low = Series::new("low", self.low.to_vec());
        let close = Series::new("close", self.close.to_vec());
        let vol = Series::new("vol", self.vol.to_vec());
        let sell_vol = Series::new("sell_vol", self.sell_vol.to_vec());
        let sell_count = Series::new("sell_count", self.sell_count.to_vec());
        let buy_vol = Series::new("buy_vol", self.buy_vol.to_vec());
        let buy_count = Series::new("buy_count", self.buy_count.to_vec());
        let start_time = Series::new("start_time", TradeBuffer::convert_datetime(self.start_time.to_vec()));
        let end_time = Series::new("end_time", TradeBuffer::convert_datetime(self.end_time.to_vec()));

        let df = DataFrame::new(vec![
            time,
            open,
            high,
            low,
            close,
            vol,
            sell_vol,
            sell_count,
            buy_vol,
            buy_count,
            start_time,
            end_time,
        ]).unwrap();

        return df;        
    }
}



#[cfg(test)]
mod test_data_frame {




}