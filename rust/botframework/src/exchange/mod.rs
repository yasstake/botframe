use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::prelude::Series;

pub const BUY: i32 = 1;
pub const SELL: i32 = 2;

#[derive(Debug)]
pub struct Trade {
    pub time_ns: i64, // TODO: change to polas data type.
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
        let time_s = Series::new("time_ns", &self.time_ns);
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
        // append history
        //self.trade_history.
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
