use smartcore::linalg::high_order;

use std::collections::VecDeque;


use crate::exchange::Market;
use crate::exchange::MarketInfo;

use crate::exchange::order::Order;
use crate::exchange::order::OrderStatus;
use crate::exchange::order::OrderType;

use crate::exchange::order::ClosedOrder;


pub struct Position {
    price: f64,
    size: f64, // in BTC
}

impl Position {
    fn new() -> Self {
        return Position {
            price: 0.0,
            size: 0.0,
        };
    }

    fn update_position(&mut self, order: Order) {
        if self.size == 0.0 {
            self.price = order.price;
            self.size = order.size;
        } else {
            let new_size = self.size + order.size;

            let notion = self.size / self.price + order.size / order.price;
            self.price = new_size / notion;
            self.size = new_size;

            println!("notion={} new_size={}", notion, new_size);
        }
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
    wallet_balance: f64, // 入金額
}

impl SessionValue {
    fn new(start_offset: i64) -> Self {
        return SessionValue {
            _session_id: "0000".to_string(), // TODO: implemnet multisession
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
            wallet_balance: 0.0,
        };
    }

    fn generate_id(&mut self) -> String {
        self._order_index += 1;
        let index = self._order_index;

        let upper = index / 1000;
        let lower: i64 = index % 1000;

        let id = format! {"{:04}-{:012}-{:03}",self._session_id, upper, lower};

        return id.to_string();
    }

    fn get_center_price(&self) -> f64 {
        if self.last_buy_price == 0.0 || self.last_sell_price == 0.0 {
            return 0.0;
        }

        return (self.last_buy_price + self.last_sell_price) / 2.0;
    }

    fn get_position_margin(&self) -> f64 {
        let center_price = self.get_center_price();

        let long_margin = (center_price - self.long_position.price)                       // 購入単価 - 現在単価
            * (self.long_position.size / self.long_position.price); // 購入数量

        println!("long_margin={}", long_margin);

        let short_margin = (self.short_position.price - center_price)                    // 購入単価 - 現在単価
            * (self.short_position.size / self.short_position.price); // 購入数量

        println!("short_margin={}", short_margin);

        return long_margin + short_margin;
    }

    fn insert_long_position(order: Order) {}

    fn insert_short_position(order: Order) {}

    fn get_avairable_balance() -> f64 {
        return 0.0;
    }

    /// price x sizeのオーダを発行できるか確認する。
    ///   if unrealised_pnl > 0:
    ///      available_balance = wallet_balance - (position_margin + occ_closing_fee + occ_funding_fee + order_margin)
    ///      if unrealised_pnl < 0:
    ///          available_balance = wallet_balance - (position_margin + occ_closing_fee + occ_funding_fee + order_margin) + unrealised_pnl

    /*
    fn check_margin(&self, price: f64, volume: f64) -> bool {
        const LEVERRAGE: f64 = 1.0; // TODO: まずはレバレッジ１倍固定から始める。
        let mut order_amount = price * volume;

        order_amount = order_amount / LEVERRAGE;

        // return order_amount < self.get;
    }
    */

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

    /// オーダーをオーダーリストへ追加する。
    /// _partial_workはオーダーした量と同じ値をセットする。
    /// オーダーエントリー後は値段と時間でソートする。
    fn insert_order(&mut self, side: &str, price: f64, size: f64, duration_ms: i64) -> bool {
        let order_id = self.generate_id();
        // let order = Order::new(order_id, self.current_time, self.current_time + duration_ms, price, size, false);

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

pub trait Session {
    fn get_timestamp_ms(&self) -> i64;
    fn make_order(&self, side: &str, price: f64, size: f64, duration_ms: i64) -> OrderStatus;
    /*
    fn get_active_orders(&self) -> [Order];
    fn get_posision(&self) -> (Position, Position); // long/short
    fn diposit(&self, balance: f64);
    fn get_balance(&self) -> f64;
    fn get_avairable_balance(&self) -> f64;
    fn set_indicator(&self, key: &str, value: f64); // TODO: implement later
    fn result() -> String; // evaluate session result
    fn on_exec(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64) -> SessionEventType;
    */
    // fn run(&self, agent: &dyn Agent, from_time_ms: i64, time_interval_ms: i64) -> bool;
    // fn ohlcv(&mut self, width_sec: i64, count: i64) -> ndarray::Array2<f64>;
}

impl Session for SessionValue {
    fn get_timestamp_ms(&self) -> i64 {
        return self.current_time;
    }

    fn make_order(&self, side: &str, price: f64, size: f64, duration_ms: i64) -> OrderStatus {
        return OrderStatus::NoMoney;
    }

    /*
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

///-------------------------------------------------------------------------------------
/// TEST SECTION
/// ------------------------------------------------------------------------------------

#[cfg(test)]
mod TestPosition {
    use super::*;
    #[test]
    fn test_update_position() {
        let mut position = Position::new();

        let order = Order::new(
            1,
            "neworder".to_string(),
            OrderType::Sell,
            100,
            100.0,
            200.0,
            false,
        );
        position.update_position(order);
        assert_eq!(position.price, 100.0);
        assert_eq!(position.size, 200.0);

        let order = Order::new(
            1,
            "neworder".to_string(),
            OrderType::Buy,
            100,
            200.0,
            100.0,
            false,
        );
        position.update_position(order);
        assert_eq!(position.price, 300.0 / 2.5);
        assert_eq!(position.size, 300.0);
    }
}

#[cfg(test)]
mod TestSessionValue {
    use super::*;
    #[test]
    fn test_new() {
        let session = SessionValue::new(1);
    }

    #[test]
    fn test_SessionValue() {
        let mut session = SessionValue::new(1);

        let id = session.generate_id();
        println!("{}", id);
        let id = session.generate_id();
        println!("{}", id);

        let current_time = session.get_timestamp_ms();
        println!("{}", current_time);

        // test center price
        session.last_buy_price = 100.0;
        session.last_sell_price = 100.5;
        assert_eq!(session.get_center_price(), 100.25);

        // test margin
        session.long_position.price = 100.0;
        session.long_position.size = 100.0;
        session.short_position.price = 200.0;
        session.short_position.size = 200.0;
        assert_eq!(session.get_position_margin(), 0.25 + 99.75);

        // test center price
        session.last_buy_price = 200.0;
        session.last_sell_price = 200.0;
        assert_eq!(session.get_center_price(), 200.0);

        // test margin
        session.long_position.price = 400.0;
        session.long_position.size = 100.0;
        session.short_position.price = 100.0;
        session.short_position.size = 200.0;
        assert_eq!(session.get_position_margin(), -50.0 + (-200.0));
    }
}
