use crate::common::order::Trade;
use crate::common::time::{to_naivedatetime, MicroSec, MICRO_SECOND, NANO_SECOND, SEC};
use chrono::NaiveDateTime;
use polars::prelude::ChunkCompare;
use polars::prelude::DataFrame;
use polars::prelude::Duration;
use polars::prelude::DynamicGroupOptions;
use polars::prelude::NamedFrom;
use polars::prelude::Series;
use polars::prelude::BooleanType;
use polars::prelude::ChunkedArray;
use polars_core::prelude::SortOptions;
use polars_lazy::dsl::IntoLazy;
use polars_lazy::prelude::col;
use polars_time::ClosedWindow;

use super::sqlite::Ohlcvv;

#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
pub mod KEY {
    pub const time_stamp: &str = "time_stamp";

    // for trade
    pub const price: &str = "price";
    pub const size: &str = "size";
    pub const order_side: &str = "order_side";
    pub const liquid: &str = "liquid";
    pub const id: &str = "id";

    // for ohlcv
    pub const open: &str = "open";
    pub const high: &str = "high";
    pub const low: &str = "low";
    pub const close: &str = "close";
    pub const vol: &str = "vol";
    pub const sell_vol: &str = "sell_vol";
    pub const sell_count: &str = "sell_count";
    pub const buy_vol: &str = "buy_vol";
    pub const buy_count: &str = "buy_count";
    pub const start_time: &str = "start_time";
    pub const end_time: &str = "end_time";
    pub const count: &str = "count";
}

/// Cutoff from_time to to_time(not include)
pub fn select_df(df: &DataFrame, from_time: MicroSec, to_time: MicroSec) -> DataFrame {
    if from_time == 0 && to_time == 0 {
        log::debug!("preserve select df");
        return df.clone();
    }

    let mask: ChunkedArray<BooleanType>;

    if from_time == 0 {
        mask = df.column(KEY::time_stamp).unwrap().lt(to_time).unwrap();
    }
    else if to_time == 0 {
        mask = df.column(KEY::time_stamp).unwrap().gt_eq(from_time).unwrap();
    }
    else {
        mask = df.column(KEY::time_stamp).unwrap().gt_eq(from_time).unwrap()
        & df.column(KEY::time_stamp).unwrap().lt(to_time).unwrap();
    }

    let df = df.filter(&mask).unwrap();

    return df;
}

pub fn start_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::time_stamp).unwrap().min()
}

pub fn end_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::time_stamp).unwrap().max()
}

pub fn merge_df(df1: &DataFrame, df2: &DataFrame) -> DataFrame {
    let df2_start_time = start_time_df(df2);

    if df2_start_time.is_some() {
        let df = select_df(df1, 0, df2_start_time.unwrap());
        return df.vstack(df2).unwrap();
    }
    else {
        return df1.clone();
    }
}

pub fn ohlcv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> DataFrame {
    let df = select_df(df, start_time, end_time);

    return df
        .lazy()
        .groupby_dynamic(
            [col(KEY::order_side)],
            DynamicGroupOptions {
                index_column: KEY::time_stamp.into(),
                every: Duration::new(SEC(time_window)), // グループ間隔
                period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
                offset: Duration::parse("0m"),
                truncate: true,            // タイムスタンプを切り下げてまとめる。
                include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
            },
        )
        .agg([
            col(KEY::price).first().alias(KEY::open),
            col(KEY::price).max().alias(KEY::high),
            col(KEY::price).min().alias(KEY::low),
            col(KEY::price).last().alias(KEY::close),
            col(KEY::size).sum().alias(KEY::vol),
            col(KEY::price).count().alias(KEY::count)
        ])
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
            },
        )
        .collect()
        .unwrap();
}


pub fn ohlcv_from_ohlcv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> DataFrame {
    let df = select_df(df, start_time, end_time);

    return df
        .lazy()
        .groupby_dynamic(
            [col(KEY::order_side)],
            DynamicGroupOptions {
                index_column: KEY::time_stamp.into(),
                every: Duration::new(SEC(time_window)), // グループ間隔
                period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
                offset: Duration::parse("0m"),
                truncate: true,            // タイムスタンプを切り下げてまとめる。
                include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
            },
        )
        .agg([
            col(KEY::open).first().alias(KEY::open),
            col(KEY::high).max().alias(KEY::high),
            col(KEY::low).min().alias(KEY::low),
            col(KEY::close).last().alias(KEY::close),
            col(KEY::vol).sum().alias(KEY::vol),
            col(KEY::count).count().alias(KEY::count)
        ])
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
            },
        )
        .collect()
        .unwrap();
}


///
/// SQL DBには、Tickデータを保存する。
/// インメモリーDBとしてPolarsを利用し、１秒足のOHLCV拡張を保存する。
/// 定期的に最新の情報をメモリに更新する（時間（秒）がインデックスキーとして上書きする）
pub struct OhlcvBuffer {
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

impl OhlcvBuffer {
    pub fn new() -> Self {
        return OhlcvBuffer {
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

    pub fn clear(&mut self) {
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

    pub fn push_trades(&mut self, trades: Vec<Ohlcvv>) {
        for trade in trades {
            self.push_trade(&trade);
        }
    }

    pub fn push_trade(&mut self, trade: &Ohlcvv) {
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


    pub fn to_dataframe(&self) -> DataFrame {
        let time = Series::new(KEY::time_stamp, self.time.to_vec());
        let open = Series::new(KEY::open, self.open.to_vec());
        let high = Series::new(KEY::high, self.high.to_vec());
        let low = Series::new(KEY::low, self.low.to_vec());
        let close = Series::new(KEY::close, self.close.to_vec());
        let vol = Series::new(KEY::vol, self.vol.to_vec());
        let sell_vol = Series::new(KEY::sell_vol, self.sell_vol.to_vec());
        let sell_count = Series::new(KEY::sell_count, self.sell_count.to_vec());
        let buy_vol = Series::new(KEY::buy_vol, self.buy_vol.to_vec());
        let buy_count = Series::new(KEY::buy_count, self.buy_count.to_vec());
        let start_time = Series::new(KEY::start_time, self.start_time.to_vec());
        let end_time = Series::new(KEY::end_time, self.end_time.to_vec());

        let df = DataFrame::new(vec![
            time, open, high, low, close, vol, sell_vol, sell_count, buy_vol, buy_count,
            start_time, end_time,
        ])
        .unwrap();

        return df;
    }
}

/// Ohlcvのdfを内部にキャッシュとしてもつDataFrameクラス。
/// ・　生DFのマージ（あたらしいdfの期間分のデータを削除してから追加。重複がないようにマージする。
/// ・　OHLCVの生成
pub struct OhlcvDataFrame {
    df: DataFrame,
}

impl OhlcvDataFrame {
    /*
    // TODO: DFの最初の時間とおわりの時間を取得する。
    pub fn start_time() {

    }

    pub fn end_time() {

    }
    */

    //TODO: カラム名の変更
    pub fn select(&self, mut start_time_ms: i64, mut end_time_ms: i64) -> Self {
        if end_time_ms == 0 {
            end_time_ms = self.df.column("timestamp").unwrap().max().unwrap();
        }

        let mask = self
            .df
            .column("timestamp")
            .unwrap()
            .gt(start_time_ms)
            .unwrap();
        self.df
            .column("timestamp")
            .unwrap()
            .lt_eq(end_time_ms)
            .unwrap();

        let df = self.df.filter(&mask).unwrap();

        return OhlcvDataFrame { df };
    }
}

/*
pub fn ohlcv_df_from_ohlc(
    df: &DataFrame,
    mut current_time_ms: i64,
    width_sec: i64,
    count: i64,
) -> DataFrame {
    if current_time_ms <= 0 {
        current_time_ms = df.column("timestamp").unwrap().max().unwrap();
    }

    let width_ms = width_sec * 1_000;

    // println!("count{} width_sec{} width_ms{}", count, width_sec, width_ms);

    let mut start_time_ms = 0;
    if count != 0 {
        current_time_ms - (width_ms * (count as i64));
    }

    //return ohlcv_from_ohlcv(&df, start_time_ms, end_time_ms, width_sec);
    return ohlcv_from_ohlcv_dynamic(&mut df, width_sec);
}

///
/// TODO: グループ化の単位を個数に変えられないか？　→ Dollbarへの拡張を可能とするため。
fn ohlcv_from_ohlcv_dynamic(
    df: &mut DataFrame,
    width_sec: i64,
) -> DataFrame {
    // TODO  あとで性能を計測する。
//    let df = df.clone();

    return df
        .lazy()
        .groupby_dynamic(
            vec![],
            DynamicGroupOptions {
                index_column: "timestamp".into(),
                every: Duration::new(width_sec * NANO_SECOND), // グループ間隔
                period: Duration::new(width_sec * NANO_SECOND), // データ取得の幅（グループ間隔と同じでOK)
                offset: Duration::new(0),
                truncate: true,            // タイムスタンプを切り下げてまとめる。
                include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
            },
        )
        .agg([
            col("open").first().alias("open"),
            col("high").max().alias("high"),
            col("low").min().alias("low"),
            col("close").last().alias("close"),
            col("vol").sum().alias("vol"),
        ])
        .sort(
            "timestamp",
            SortOptions {
                descending: false,
                nulls_last: false,
            },
        )
        .collect()
        .unwrap();
}

*/

pub struct TradeBuffer {
    pub time_stamp: Vec<MicroSec>,
    pub price: Vec<f64>,
    pub size: Vec<f64>,
    pub order_side: Vec<bool>,
    pub liquid: Vec<bool>,
}

impl TradeBuffer {
    pub fn new() -> Self {
        return TradeBuffer {
            time_stamp: Vec::new(),
            price: Vec::new(),
            size: Vec::new(),
            order_side: Vec::new(),
            liquid: Vec::new(),
        };
    }

    pub fn clear(&mut self) {
        self.time_stamp.clear();
        self.price.clear();
        self.size.clear();
        self.order_side.clear();
        self.liquid.clear();
    }

    pub fn push_trades(&mut self, trades: Vec<Trade>) {
        for trade in trades {
            self.push_trade(&trade);
        }
    }

    pub fn push_trade(&mut self, trade: &Trade) {
        self.time_stamp.push(trade.time);
        self.price.push(trade.price);
        self.size.push(trade.size);
        self.order_side.push(trade.order_side.is_buy_side());
        self.liquid.push(trade.liquid);
    }

    pub fn to_dataframe(&self) -> DataFrame {
        let time_stamp = Series::new(KEY::time_stamp, self.time_stamp.to_vec());
        let price = Series::new(KEY::price, self.price.to_vec());
        let size = Series::new(KEY::size, self.size.to_vec());
        let order_side = Series::new(KEY::order_side, self.order_side.to_vec());
        let liquid = Series::new(KEY::liquid, self.liquid.to_vec());

        let df = DataFrame::new(vec![time_stamp, price, size, order_side, liquid]).unwrap();

        return df;
    }
}
