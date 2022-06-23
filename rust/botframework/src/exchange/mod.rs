
use chrono::NaiveDateTime;

use ndarray::Data;
use polars::prelude::ChunkApply;
use polars::prelude::ChunkCompare;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::prelude::Series;
use polars::prelude::SortOptions;

// pub const BUY: &str = "B";
// pub const SELL: &str = "S";

pub mod session;
pub mod order;


#[derive(Debug)]
pub struct Trade {
    pub time_ns: i64,
    pub price: f64,
    pub size: f64,
    pub bs: String,         // 本来はOrderTypeで実装するべきだが、Porlarsへいれるため汎用形のStringを採用。
    pub id: String,
}

#[derive(Debug)]
struct TradeBlock {
    time_ns: Vec<i64>,
    price: Vec<f64>,
    size: Vec<f64>,
    bs: Vec<String>,
    id: Vec<String>,
}

impl TradeBlock {
    const fn new() -> TradeBlock {
        return TradeBlock {
            time_ns: Vec::new(),
            price: Vec::new(),
            size: Vec::new(),
            bs: Vec::new(),
            id: Vec::new(),
        };
    }

    fn clear(&mut self) {
        self.time_ns.clear();
        self.price.clear();
        self.size.clear();
        self.bs.clear();
        self.id.clear();
    }

    fn append_trade(&mut self, trade: &Trade) {
        self.time_ns.push(trade.time_ns);
        self.price.push(trade.price);
        self.size.push(trade.size);
        self.bs.push(trade.bs.to_string());
        self.id.push(trade.id.clone());
    }

    fn to_data_frame(&mut self) -> DataFrame {
        let av: Vec<NaiveDateTime> = self
            .time_ns
            .iter()
            .map(|x| NaiveDateTime::from_timestamp(*x/1_000_000, (*x%1_000_000)as u32))
            .collect();

        let time = Series::new("time", av);
        let price = Series::new("price", &self.price);
        let size = Series::new("size", &self.size);
        let bs = Series::new("bs", &self.bs);
        let id = Series::new("id", &self.id);

        let df = DataFrame::new(vec![time, price, size, bs, id]).unwrap();

        return df;
    }
}

use numpy::ndarray;

pub fn select_df(df: &DataFrame, start_time_ms: i64, end_time_ms: i64) -> DataFrame {
    let mask = df.column("time").unwrap().gt_eq(start_time_ms).unwrap()
        & df.column("time").unwrap().lt(end_time_ms).unwrap();

    let df = df.filter(&mask).unwrap();

    return df;
}


pub fn ohlcv_df_from_raw(df: &DataFrame, mut current_time_ms: i64, width_sec: i64, count: i64) -> DataFrame {
    if current_time_ms <= 0 {
        current_time_ms = df.column("time").unwrap().max().unwrap();
    }

    let width_ms = width_sec * 1_000;

    let start_time_ms = ((current_time_ms / width_ms) + 1) * width_ms - (width_ms * (count as i64));
    let end_time_ms = current_time_ms;

    return ohlcv_from_df(df, start_time_ms, end_time_ms, width_sec);
}

fn ohlcv_from_df(df: &DataFrame, start_time_ms: i64, end_time_ms: i64, width_sec: i64) -> DataFrame {
    let mut df = select_df(df, start_time_ms, end_time_ms);
    let t = df.column("time").unwrap();
    
    let vec_t:Vec<NaiveDateTime> = 
        t
        .datetime()
        .expect("Type Error")
        .into_iter()
        .map(|x|
             (
                NaiveDateTime::from_timestamp((x.unwrap()/1_000/width_sec) * width_sec, 0)
            )
        )
        .collect();

    let new_t: Series = Series::new("time_slot", vec_t);

    df.replace("time", new_t);

    let df = df.lazy();

    let df = df
        .groupby([col("time")])
        .agg([
            col("price").first().alias("open"),
            col("price").max().alias("high"),
            col("price").min().alias("low"),
            col("price").last().alias("close"),
            col("size").sum().alias("vol"),
        ])
        .sort("time", SortOptions{descending:true, nulls_last:false})
        .collect()
        .unwrap();

    return df;
}


pub trait MaketAgent {
    fn on_event(&self, kind: &str, time: i64, price: f32, size: f32);
}

pub trait MarketInfo {
    fn _df(&mut self) -> DataFrame;
    fn _ohlcv(&mut self, current_time_ms: i64, width_sec: i64, count: i64) -> ndarray::Array2<f64>;
    fn start_time(&self) -> i64;
    fn end_time(&self) -> i64;
    fn for_each(&mut self, call_back: fn(time: i64, kind: &str, price: f64, size: f64), start_time_ms: i64, end_time_ms: i64);
}

pub struct Market {
    // Use DataFrame
    trade_history: DataFrame,
    trade_buffer: TradeBlock,
    session: SessionValue
}

impl Market {
    pub fn new() -> Market {
        let mut trade_block = TradeBlock::new();
        return Market {
            trade_history: trade_block.to_data_frame(),
            trade_buffer: TradeBlock::new(),
            session: SessionValue::new()
        };
    }

    pub fn append_trade(&mut self, trade: &Trade) {
        self.trade_buffer.append_trade(trade);
    }

    pub fn flush_add_trade(&mut self) {
        match self
            .trade_history
            .vstack(&self.trade_buffer.to_data_frame())
        {
            Ok(df) => {
                println!("append{}", df.shape().0);
                self.trade_history = df;
                //self.trade_buffer = TradeBlock::new();
                self.trade_buffer.clear();
            }
            Err(err) => {
                println!("Err {}", err)
            }
        }
    }

    pub fn history_size(&mut self) -> i64 {
        let rec_no: i64 = self.trade_history.height() as i64;

        return rec_no;
    }

    // 重複レコードを排除する
    // DataFrameの作り直しとなるので比較的重い処理。
    // （また古い処理らしい）
    pub fn drop_duplicate_history(&mut self) {
        self.trade_history = self.trade_history.drop_duplicates(true, None).unwrap();
    }

    // TODO: ms -> ns?
    pub fn select_df(&mut self, mut start_time_ms: i64, mut end_time_ms: i64) -> DataFrame {
        if start_time_ms == 0 {
            start_time_ms = self.start_time();
        } else if start_time_ms < 0 {
            start_time_ms = self.start_time() - start_time_ms;
        }

        if end_time_ms == 0 {
            end_time_ms = self.end_time();
        } else if end_time_ms < 0 {
            end_time_ms = self.end_time() + end_time_ms
        }
        
        return select_df(&self._df(), start_time_ms, end_time_ms);
    }

    pub fn _print_head_history(&mut self) {
        println!("{}", self.trade_history.head(Some(5)));
    }

    pub fn _print_tail_history(&mut self) {
        println!("{}", self.trade_history.tail(Some(5)));
    }

    pub fn get_session(&mut self) -> &mut SessionValue {
        return &mut self.session;
    }
}

use polars::prelude::Float64Type;
use polars::prelude::TemporalMethods;
use polars_lazy::dsl::IntoLazy;
use polars_lazy::prelude::col;

use polars::chunked_array::comparison::*;



impl MarketInfo for Market {
    fn _df(&mut self) -> DataFrame {
        // TODO: clone の動作を確認する。-> Deep cloneではあるが、そこそこ早い可能性あり。
        // またコピーなので更新はしても、本体へは反映されない。
        // https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.clone.html
        return self.trade_history.clone();
    }

    // プライベート用
    fn _ohlcv(&mut self, current_time_ms: i64, width_sec: i64, count: i64) -> ndarray::Array2<f64> {
        let df = &self.trade_history;

        let df = ohlcv_df_from_raw(df, current_time_ms, width_sec, count);

        let array: ndarray::Array2<f64> = df
            .select(&["time", "open", "high", "low", "close", "vol"])
            .unwrap()
            .to_ndarray::<Float64Type>()
            .unwrap();

        return array;
    }

    // TODO: error handling calling before data load
    fn start_time(&self) -> i64 {
        let time_s = self.trade_history.column("time").unwrap();
        return time_s.min().unwrap();
    }

    // TODO: error handling calling before data load    
    fn end_time(&self) -> i64 {
        let time_s = self.trade_history.column("time").unwrap();
        return time_s.max().unwrap();
    }

    fn for_each(&mut self, call_back: fn(time: i64, kind: &str, price: f64, size: f64), start_time_ms: i64, end_time_ms: i64) {
        let df = self.select_df(start_time_ms, end_time_ms);
    
        let time_s = &df["time"];
        let price_s = &df["price"];
        let size_s = &df["size"];
        let bs_s = &df["bs"];
    
        let time = &time_s.timestamp(TimeUnit::Milliseconds).unwrap();
        let price =  price_s.f64().unwrap();
        let size = size_s.f64().unwrap();
        let bs = bs_s.utf8().unwrap();
    
        let  z =  time.into_iter().zip(price).zip(size).zip(bs);
    
        for (((t, p), s), b)  in z {
            call_back(t.unwrap(), b.unwrap(), p.unwrap(), s.unwrap());
            //println!("{:?} {:?} {:?} {:?}", t.unwrap(), p.unwrap(), s.unwrap(), b.unwrap());
        }
    }

}








////////////////////////////////////////////////////////////////////////////////
///    TEST SECION
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
use order::OrderType;


#[test]
fn test_history_size_and_dupe_load() {
    let mut market = Market::new();

    for i in 0..3000000 {
        let trade = Trade {
            time_ns: 10000 * i,
            price: 1.0,
            size: 1.1,
            bs: OrderType::Buy.to_str().to_string(),
            id: "asdfasf".to_string(),
        };

        market.append_trade(&trade);
    }
    market.flush_add_trade();

    let size = market.history_size();
    assert!(size == 3000000);

    println!("size {}", market.history_size());

    for i in 0..3000000 {
        let trade = Trade {
            time_ns: i * 100,
            price: 1.0,
            size: 1.1,
            bs: OrderType::Buy.to_str().to_string(),
            id: "asdfasf".to_string(),
        };

        market.append_trade(&trade);
    }
    market.flush_add_trade();
    let size = market.history_size();

    market._print_head_history();

    println!("size {}", market.history_size());

    market.drop_duplicate_history();
    let size = market.history_size();
    println!("size {}", market.history_size());
}

#[test]
fn test_add_trad_and_flush() {
    let mut market = Market::new();

    for i in 0..3000000 {
        let trade = Trade {
            time_ns: i * 100,
            price: 1.0,
            size: 1.1,
            bs: OrderType::Buy.to_str().to_string(),
            id: "asdfasf".to_string(),
        };

        market.append_trade(&trade);
    }
    market.flush_add_trade();
}

#[test]
fn test_make_history() {
    let mut market = Market::new();

    for i in 0..3_000_000 {
        let trade = Trade {
            time_ns: i * 1_000_000,
            price: 1.0,
            size: 1.1,
            bs: OrderType::Buy.to_str().to_string(),
            id: "asdfasf".to_string(),
        };

        market.append_trade(&trade);
    }
    market.flush_add_trade();

    //market._print_head_history();    
    //market._print_tail_history();

    assert_eq!(market.history_size(), 3_000_000);

    assert_eq!(market.start_time(), 0);
    assert_eq!(market.end_time(), 2_999_999*1_000);   // time is in msec 
}

#[test]
fn test_df_select() {
    let mut market = Market::new();

    for i in 0..(24*60*60) {
        let trade = Trade {
            time_ns: i * 1_000_000,
            price: 1.0,
            size: 1.1,
            bs: OrderType::Buy.to_str().to_string(),
            id: "asdfasf".to_string(),
        };

        market.append_trade(&trade);
    }
    market.flush_add_trade();

    market._print_head_history();    
    market._print_tail_history();

    let df = market.select_df(1, 999);
    assert_eq!(df.height(), 0);

    let df = market.select_df(0, 999);
    assert_eq!(df.height(), 1);

    let df = market.select_df(1, 1_000);
    assert_eq!(df.height(), 0);

    let df = market.select_df(0, 1_000);
    assert_eq!(df.height(), 1);

    println!("{}", df.tail(Some(5)));
}

#[cfg(test)]
use crate::bb::log::load_dummy_data;

#[test]
fn test_make_olhc() {
    let mut m = load_dummy_data();

    let start_time = m.start_time();
    println!("start_time {}", start_time);
    let last_time = m.end_time();
    println!("end_time {}", last_time);

    let rec_no = m.history_size();
    println!("hisorysize={}", rec_no);

    let df = ohlcv_df_from_raw(&m.trade_history, last_time, 1, 1000);
    println!("{}", df.head(Some(12)));

    let df = ohlcv_df_from_raw(&m.trade_history, last_time, 2, 1000);
    println!("{}", df.head(Some(12)));

    let df = ohlcv_df_from_raw(&m.trade_history, last_time, 120, 1000);
    println!("{}", df.head(Some(12)));
    println!("120TOTAL={}", df.sum());    

    let df = ohlcv_df_from_raw(&m.trade_history, last_time, 10, 1000);
    println!("{}", df.head(Some(12)));
    println!("10TOTAL={}", df.sum());

    let df = ohlcv_df_from_raw(&m.trade_history, last_time, 5, 1000);
    println!("{}", df.head(Some(12)));
    println!("5TOTAL={}", df.sum());

    let array = m._ohlcv(last_time, 5, 1000);
    println!("{}", array);

}

#[test]
fn test_add_trade() {
    let mut tb = TradeBlock::new();

    for i in 0..3000000 {
        let t = Trade {
            time_ns: i * 100,
            price: 1.0,
            size: 1.1,
            bs: OrderType::Buy.to_str().to_string(),
            id: "asdfasf".to_string(),
        };

        tb.append_trade(&t);
    }

    println!("{}", tb.id.len());
}

#[test]
fn test_to_data_frame() {
    let mut tb = TradeBlock::new();

    for i in 0..3000000 {
        let t = Trade {
            time_ns: i * 100,
            price: 1.0,
            size: 1.1,
            bs: OrderType::Buy.to_str().to_string(),
            id: "asdfasf".to_string(),
        };

        tb.append_trade(&t);
    }
    println!("{}", tb.id.len());

    let _tb = tb.to_data_frame();
}

use polars::prelude::DataType;
use polars::prelude::TimeUnit;

use self::session::SessionValue;

#[test]
fn test_df_loop () {
    let mut market = Market::new();

    for i in 0..3000000 {
        let trade = Trade {
            time_ns: 10000 * i,
            price: 1.0,
            size: 1.1,
            bs: OrderType::Buy.to_str().to_string(),
            id: "asdfasf".to_string(),
        };

        market.append_trade(&trade);
    }
    market.flush_add_trade();


    let start_time_ms = market.start_time() + 10 * 1_000;
    let end_time_ms = market.end_time();

    let df = market.select_df(start_time_ms, end_time_ms);
    let l = df.height();

    let time_s = &df["time"];
    let price_s = &df["price"];
    let size_s = &df["size"];
    let bs_s = &df["bs"];

    println!("dfsize= {}", l);

    let time = &time_s.timestamp(TimeUnit::Milliseconds).unwrap();
    let price =  price_s.f64().unwrap();
    let size = size_s.f64().unwrap();
    let bs = bs_s.utf8().unwrap();

    time.into_iter().map(|f| {println!("{}", f.unwrap());});

    
//    price.into_iter().map(|f| {println!("{}", f.unwrap());});            

    price.into_iter().map(|f|{ println!("{:?}", f);} );


    let  z =  time.into_iter().zip(price).zip(size).zip(bs);

    for (((t, p), s), b)  in z {
        println!("{:?} {:?} {:?} {:?}", t.unwrap(), p.unwrap(), s.unwrap(), b.unwrap());
        break;
    }




    //time.into_iter().zip(price.into_iter()).zip(size.into_iter());


    match (time_s.dtype(), price_s.dtype(), size_s.dtype()) {
        (DataType::Datetime(TimeUnit::Milliseconds, None),  DataType::Float64, DataType::Float64) => {
            let time = time_s.timestamp(TimeUnit::Milliseconds).unwrap();
            let price =  price_s.f64().unwrap();
            let size = size_s.f64().unwrap();

            //println!("{:?}", time);
            //println!("{:?}", price);
            //println!("{:?}", size);            


            // time.into_iter().map(|f| {println!("{}", f.unwrap());});

            price.into_iter().map(|f| {println!("{}", f.unwrap());});            

            time.into_iter().zip(price.into_iter()).zip(size.into_iter())
            .map(|f| {println!("{:?}", f); });

        }
        _ => {
            println!("err {:?}, {:?}, {:?}", time_s.dtype(), price_s.dtype(), size_s.dtype());
            assert!(false, "illeagaltype")
        }
    }




}