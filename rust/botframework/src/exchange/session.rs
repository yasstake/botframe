use smartcore::linalg::high_order;

use crate::exchange::Market;
use crate::exchange::MarketInfo;

use std::collections::VecDeque;
use std::rc::Rc;

use super::BUY;

// Status life cycle
//   "CREATED" -> "CLOSE" or "CANCEL"

pub struct Order {
    order_id: String, // YYYY-MM-DD-SEQ
    create_time: i64, // in ms
    valid_until: i64, // in ms
    price: f64,
    size: f64, // in USD
    taker: bool,        // takerの場合true, falseの場合はmakerとなる。
    _partial_work: f64, // 約定したかず。０になったら全部約定。イベントは０の全部約定時のみ発生。
}

impl Order {
    fn new(order_id: String, // YYYY-MM-DD-SEQ
           create_time: i64, // in ms
           valid_until: i64, // in ms
           price: f64,
            size: f64, // in USD
            taker: bool) -> Self{
                return Order{
                    order_id: order_id,
                    create_time: create_time,
                    valid_until: valid_until,
                    price: price,
                    size: size,
                    taker: taker,
                    _partial_work: size
                };
    } 

}

pub struct ClosedOrder {
    order_id: String,
    create_time: f64,
    status: String,
    price: f64,
    sell_size: f64, // in usd
    buy_size: f64,  // in usd
    fee: f64,
    total: f64,
}

pub struct Position {
    price: f64,
    size: f64,  // in BTC
}

impl Position {
    fn new() -> Self {
        return Position {
            price: 0.0,
            size: 0.0,
        };
    }
}

// TODO: ユーザのオリジナルなインジケータを保存できるようにする。
pub struct Indicator {
    time: i64,
    key: String,
    value: f64,
}

///
/// オーダー処理の基本
///     make_order(buy_or_sell, price, size)
///     ＜初期化状態の確認＞
///         last_sell_price, last_buy_priceともに０ではない。
///         current_timeが０ではない。
/// 
///         NG->エラー
/// 
///     ＜残高の確認＞
///         avairable_balance よりsizeが小さい（余裕がある）
///             oK -> avairable_balanceからorder_marginへへSize分を移動させてオーダー処理実行
///             not-> エラー
/// 
///     ＜価格の確認＞
///         価格が0: ->  最終約定価格にセット
///         価格が板の反対側：→ Takerとして、オーダーリストへ登録
///         価格が板の内側：　→　makerとしてオーダーリストへ登録
/// 
///     ＜戻り値＞
///     オーダーIDをつくり、返却
/// 



pub struct SessionValue {
    _session_id: String,
    _order_index: i64,
    _start_offset: i64,
    last_sell_price: f64,
    last_buy_price: f64,
    current_time: i64,
    long_orders: Vec<Order>,
    shot_orders: Vec<Order>,
    order_history: Vec<ClosedOrder>,
    long_position: Position,
    short_position: Position,
    indicators: Vec<Indicator>,
    avairable_balance: f64,         // オーダー可能資産
    order_margin: f64               // オーダー中資産
}

impl SessionValue {
    ///　ログイベントを処理してセッション情報を更新する。
    ///  0. Tick更新イベントを発生させる。
    ///  1. 時刻のUpdate
    ///  ２。マーク価格の更新
    /// 　2. オーダ中のオーダーを更新する。
    /// 　　　　　期限切れオーダーを削除する。
    /// 　　　　　現在のオーダーから執行可能な量を _partial_workから引き算し０になったらオーダ完了（一部約定はしない想定）
    /// 　オーダー
    ///
    /// データがそろうまではFalseをかえす。ウォーミングアップ完了後Trueへ更新する。
    /// 

    /* TODO: マージンの計算とFundingRate計算はあとまわし */
    fn exec_event(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64) {}

    ///
    /// price x sizeのオーダを発行できるか確認する。
    fn check_margin(&self, price: f64, volume: f64) -> bool {
        const LEVERRAGE: f64 = 1.0;         // TODO: まずはレバレッジ１倍固定から始める。
        let order_amount = price * volume;
        
        order_amount = order_amount / LEVERRAGE;

        return order_amount < self.avairable_balance;
    }
    /// オーダーをオーダーリストへ追加する。
    /// _partial_workはオーダーした量と同じ値をセットする。
    /// オーダーエントリー後は値段と時間でソートする。
    fn insert_order(&self, side: &str, price: f64, size: f64, duration_ms: i64) -> bool {
        let order_id = self.generate_id();
        let order = Order::new(order_id, self.current_time, self.current_time + duration_ms, price, size, false); 

        match side {
            BUY => {
                // check if the order become taker of maker

                // insert order list

                // sort order
            }
            SELL => {
                // check if the order become taker of maker
            }
            _ => {
                println!("Unknown order type {} / use B or S", side);
            }
        }

        return false;
    }
}

pub trait Agent {
    fn on_tick(&self, session: &Market, time_ms: i64);
    fn on_exec(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64);
    fn on_order(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64);
}

pub enum SessionEventType {
    None,
    OrderComplete,
    OrderExpire,
    Liquidation,
}


pub trait Session {
    fn new(start_offset: i64) -> Self;
    fn generate_id(&self) -> String;
    fn get_timestamp_ms(&self) -> i64;
    fn make_order(&self, side: &str, price: f64, volume: f64, duration_ms: i64) -> Order;
    fn get_active_orders(&self) -> [Order];
    fn get_posision(&self) -> (Position, Position); // long/short
    fn diposit(&self, balance: f64);
    fn get_balance(&self) -> f64;
    fn get_avairable_balance(&self) -> f64;
    fn set_indicator(&self, key: &str, value: f64); // TODO: implement later
    fn result() -> String; // evaluate session result
    fn on_exec(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64) -> SessionEventType;
    // fn run(&self, agent: &dyn Agent, from_time_ms: i64, time_interval_ms: i64) -> bool;
    // fn ohlcv(&mut self, width_sec: i64, count: i64) -> ndarray::Array2<f64>;    
}


impl Session for SessionValue {
    fn new(start_offset: i64) -> Self {
        return SessionValue {
            _session_id: "0000".to_string(),        // TODO: implemnet multisession      
            _order_index: 0,
            _start_offset: start_offset,
            last_sell_price: 0.0,
            last_buy_price: 0.0,
            current_time: 0,
            long_orders: vec![],
            shot_orders: vec![],
            order_history: vec![],
            long_position: Position::new(),
            short_position: Position::new(),
            indicators: vec![],
            avairable_balance: 0.0,
            order_margin: 0.0
        };
    }

    fn generate_id(&self) -> String {
        self._order_index += 1;
        let index = self._order_index;

        let upper = index / 1000;
        let lower: i64 = index % 1000;        

        let id = format!{"{:04}-{:012}-{:03}",self._session_id, upper, lower};
        
        return id.to_string();
    }

    /*
        fn get_timestamp_ms(&self) -> i64;
        fn make_order(&self, side: &str, price: f64, volume: f64, duration_ms: i64) -> Order;
        fn get_active_orders(&self) -> [Order];
        fn get_posision(&self) -> (Position, Position);     // long/short
        fn set_balance(&self, balance: f64);
        fn get_balance(&self) -> f64;
        fn set_indicator(&self, key: &str, value: f64);     // TODO: implement later
        fn result() -> String;                              // evaluate session result
        fn on_exec(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64);
        // fn run(&self, agent: &dyn Agent, from_time_ms: i64, time_interval_ms: i64) -> bool;
        // fn ohlcv(&mut self, width_sec: i64, count: i64) -> ndarray::Array2<f64>;
    */
}
