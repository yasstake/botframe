use smartcore::linalg::high_order;

use crate::exchange::Market;
use crate::exchange::MarketInfo;

use std::collections::VecDeque;
use std::rc::Rc;

use super::BUY;

// Status life cycle
//   "CREATED" -> "CLOSE" or "CANCEL"

#[derive(Debug)]
pub struct Order {
    order_id: String, // YYYY-MM-DD-SEQ
    create_time: i64, // in ms
    valid_until: i64, // in ms
    price: f64,
    size: f64,          // in USD
    taker: bool,        // takerの場合true, falseの場合はmakerとなる。
    _partial_work: f64, // 約定したかず。０になったら全部約定。イベントは０の全部約定時のみ発生。
}

impl Order {
    fn new(
        order_id: String, // YYYY-MM-DD-SEQ
        create_time: i64, // in ms
        valid_until: i64, // in ms
        price: f64,
        size: f64, // in USD
        taker: bool,
    ) -> Self {
        return Order {
            order_id: order_id,
            create_time: create_time,
            valid_until: valid_until,
            price: price,
            size: size,
            taker: taker,
            _partial_work: size,
        };
    }
}

/// 未実現オーダーリストを整理する。
/// ・　オーダーの追加
/// ・　オーダの削除（あとで実装）
/// ・　オーダー中のマージン計算
/// ・　オーダーのExpire
/// ・　オーダーの約定
pub struct Orders {
    buy_order: bool,
    q: Vec<Order>,
}

use std::cmp::Ordering;

impl Orders {
    fn new(buy_order: bool) -> Self {
        return Orders {
            buy_order: buy_order,
            q: vec![],
        };
    }

    fn push(&mut self, order: Order) {
        self.q.push(order);
        self.sort();
    }

    fn margin(&mut self) -> f64 {
        let mut m: f64 = 0.0;

        for order in &self.q {
            m += order.size;
        }

        return m;
    }

    // Sellオーダーを約定しやすい順番に整列させる
    //   *やすい順番
    //   早い順番
    fn sell_comp(a: &Order, b: &Order) -> Ordering {
        if a.price < b.price {
            return Ordering::Less;
        } else if a.price > b.price {
            return Ordering::Greater;
        } else if a.create_time < b.create_time {
            return Ordering::Less;
        }
        return Ordering::Equal;
    }

    // buyオーダーを約定しやすい順番に整列させる
    //   *高い順番
    //   早い順番
    fn buy_comp(a: &Order, b: &Order) -> Ordering {
        if a.price < b.price {
            return Ordering::Greater;
        } else if a.price > b.price {
            return Ordering::Less;
        } else if a.create_time < b.create_time {
            return Ordering::Less;
        }
        return Ordering::Equal;
    }

    fn sort(&mut self) {
        if self.buy_order {
            // 高い方・古い方から並べる
            self.q.sort_by(Orders::buy_comp);
        } else {
            // 安い方・古い方から並べる
            self.q.sort_by(Orders::sell_comp);
        }
    }

    // TODO: not implemented
    fn execute(&mut self, time_ms: i64, action: &str, price: f64, size: f64) {}
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

pub enum SessionEvent {
    None,
    Enqueue,
    OrderComplete,
    OrderExpire,
    Liquidation,
    NoMoney,
}

pub trait Session {
    fn get_timestamp_ms(&self) -> i64;
    fn make_order(&self, side: &str, price: f64, size: f64, duration_ms: i64) -> SessionEvent;
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

    fn make_order(&self, side: &str, price: f64, size: f64, duration_ms: i64) -> SessionEvent {
        return SessionEvent::NoMoney;
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
mod TestOrders {
    use super::*;
    #[test]
    fn test_orders() {
        let mut orders = Orders::new(true);

        let o1 = Order::new("low price".to_string(), 1, 2, 100.0, 100.0, false);
        let o2 = Order::new("low price but later".to_string(), 3, 2, 100.0, 50.0, false);
        let o3 = Order::new("high price".to_string(), 2, 2, 200.0, 200.0, false);
        let o4 = Order::new("high price but first".to_string(), 1, 2, 200.0, 50.0, false);

        orders.push(o1);
        orders.push(o2);
        orders.push(o3);
        orders.push(o4);

        assert_eq!(orders.margin(), 400.0);

        assert_eq!(orders.q[0].price, 200.0);
        assert_eq!(orders.q[0].size, 50.0);
        assert_eq!(orders.q[1].price, 200.0);
        assert_eq!(orders.q[1].size, 200.0);
        assert_eq!(orders.q[2].price, 100.0);
        assert_eq!(orders.q[2].size, 100.0);
        assert_eq!(orders.q[3].price, 100.0);
        assert_eq!(orders.q[3].size, 50.0);
        println!("{:?}", orders.q);

        // Sell Order
        let mut orders = Orders::new(false);

        let o1 = Order::new("low price".to_string(), 1, 2, 100.0, 100.0, false);
        let o2 = Order::new("low price but later".to_string(), 3, 2, 100.0, 50.0, false);
        let o3 = Order::new("high price".to_string(), 2, 2, 200.0, 200.0, false);
        let o4 = Order::new("high price but first".to_string(), 1, 2, 200.0, 50.0, false);

        orders.push(o1);
        orders.push(o2);
        orders.push(o3);
        orders.push(o4);

        assert_eq!(orders.margin(), 400.0);

        assert_eq!(orders.q[2].price, 200.0);
        assert_eq!(orders.q[2].size, 50.0);
        assert_eq!(orders.q[3].price, 200.0);
        assert_eq!(orders.q[3].size, 200.0);
        assert_eq!(orders.q[0].price, 100.0);
        assert_eq!(orders.q[0].size, 100.0);
        assert_eq!(orders.q[1].price, 100.0);
        assert_eq!(orders.q[1].size, 50.0);
        println!("{:?}", orders.q);
    }
}

#[cfg(test)]
mod TestPosition {
    use super::*;
    #[test]
    fn test_update_position() {
        let mut position = Position::new();

        let order = Order::new("neworder".to_string(), 1, 100, 100.0, 200.0, false);
        position.update_position(order);
        assert_eq!(position.price, 100.0);
        assert_eq!(position.size, 200.0);

        let order = Order::new("neworder".to_string(), 1, 100, 200.0, 100.0, false);
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
