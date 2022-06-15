use chrono::NaiveDateTime;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::prelude::Series;

pub const BUY: i32 = 1;
pub const SELL: i32 = 2;

#[derive(Debug)]
pub struct Trade {
    pub time_ns: i64,
    pub price: f32,
    pub size: f32,
    pub bs: i32,
    pub id: String,
}

struct TradeBlock {
    time_ns: Vec<i64>,
    price: Vec<f32>,
    size: Vec<f32>,
    bs: Vec<i32>,
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

    fn add_trade(&mut self, trade: Trade) {
        self.time_ns.push(trade.time_ns);
        self.price.push(trade.price);
        self.size.push(trade.size);
        self.bs.push(trade.bs);
        self.id.push(trade.id);
    }

    fn to_data_frame(&mut self) -> DataFrame {
        let av: Vec<NaiveDateTime> = self
            .time_ns
            .iter()
            .map(|x| NaiveDateTime::from_timestamp(*x, 0))
            .collect();

        let time_s = Series::new("time", av);
        let price = Series::new("price", &self.price);
        let size = Series::new("size", &self.size);
        let bs = Series::new("bs", &self.bs);
        let id = Series::new("id", &self.id);

        let df = DataFrame::new(vec![time_s, price, size, bs, id]).unwrap();

        return df;
    }
}

#[test]
fn test_add_trade() {
    let mut tb = TradeBlock::new();

    for i in 0..3000000 {
        let t = Trade {
            time_ns: i * 100,
            price: 1.0,
            size: 1.1,
            bs: BUY,
            id: "asdfasf".to_string(),
        };

        tb.add_trade(t);
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
            bs: BUY,
            id: "asdfasf".to_string(),
        };

        tb.add_trade(t);
    }
    println!("{}", tb.id.len());

    let _tb = tb.to_data_frame();
}

pub struct Market {
    // Use DataFrame
    trade_history: DataFrame,
    trade_buffer: TradeBlock,
}

impl Market {
    pub fn new() -> Market {
        let mut trade_block = TradeBlock::new();
        return Market {
            trade_history: trade_block.to_data_frame(),
            trade_buffer: TradeBlock::new(),
        };
    }

    pub fn add_trade(&mut self, trade: Trade) {
        self.trade_buffer.add_trade(trade);
    }

    pub fn flush_add_trade(&mut self) {
        match self
            .trade_history
            .vstack(&self.trade_buffer.to_data_frame())
        {
            Ok(df) => {
                self.trade_history = df;
                self.trade_buffer = TradeBlock::new();
            }
            Err(err) => {
                println!("Err {}", err)
            }
        }
    }

    pub fn df(&mut self) -> DataFrame {
        // TODO: clone の動作を確認する。-> Deep cloneではあるが、そこそこ早い可能性あり。
        // またコピーなので更新はしても、本体へは反映されない。
        // https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.clone.html
        return self.trade_history.clone();
    }

    pub fn history_size(&mut self) -> i64 {
        let (rec_no, col_no) = self.trade_history.shape();

        println!("shape ({} {})", rec_no, col_no);

        return rec_no as i64;
    }

    // 重複レコードを排除する
    // DataFrameの作り直しとなるので比較的重い処理。
    // （また古い処理らしい）
    pub fn drop_duplicate_history(&mut self) {
        self.trade_history = self.trade_history.drop_duplicates(true, None).unwrap();
    }

    fn _print_head_history(&mut self) {
        println!("{}", self.trade_history.head(Some(5)));
    }
    /*
    pub fn ohlcv(&mut self) -> numpy::PyArray2<

    >
    */
}

#[test]
fn test_history_size_and_dupe_load() {
    let mut market = Market::new();

    for i in 0..3000000 {
        let trade = Trade {
            time_ns: 10000 * i,
            price: 1.0,
            size: 1.1,
            bs: BUY,
            id: "asdfasf".to_string(),
        };

        market.add_trade(trade);
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
            bs: BUY,
            id: "asdfasf".to_string(),
        };

        market.add_trade(trade);
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
            bs: BUY,
            id: "asdfasf".to_string(),
        };

        market.add_trade(trade);
    }
    market.flush_add_trade();
}

#[test]
fn test_make_history() {
    let mut market = Market::new();

    for i in 0..3000000 {
        let trade = Trade {
            time_ns: i * 100,
            price: 1.0,
            size: 1.1,
            bs: BUY,
            id: "asdfasf".to_string(),
        };

        market.add_trade(trade);
    }
    market.flush_add_trade();
}
