use polars::export::arrow::bitmap::or;
use smartcore::linalg::high_order;

use std::collections::VecDeque;

use crate::exchange::Market;
use crate::exchange::MarketInfo;

use crate::exchange::order::Order;
use crate::exchange::order::OrderStatus;
use crate::exchange::order::OrderType;
use crate::exchange::order::Orders;

use crate::exchange::order::OrderResult;

#[derive(Debug)]
pub struct Position {
    price: f64,
    size: f64,      // in USD
}

impl Position {
    fn new() -> Self {
        return Position {
            price: 0.0,
            size: 0.0,
        };
    }

    // calc volume in BTC
    fn calc_volume(&self) -> f64 {
        return self.size / self.price;
    }

    fn open_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        order.status = OrderStatus::OpenOrder;

        if self.size == 0.0 {
            self.price = order.open_price;
            self.size = order.size;
        } else {
            let new_size = self.size + order.size;
            let volume = self.calc_volume() + order.volume;
            self.price = new_size / volume;
            self.size = new_size;
            println!("volume={} new_size={}", volume, new_size);
        }

        return Ok(());
    }

    fn close_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        if self.size == 0.0 {
            // ポジションがない場合なにもしない。
            self.price = 0.0;  // 念の為（誤差解消のためポジション０のときはリセット）
            return Err(OrderStatus::NoAction);
        } else if self.size < order.size {
            // ポジション以上にクローズしようとした場合なにもしない（別途分割してクローズする）
            return Err(OrderStatus::OverPosition);
        }
        // オーダの全部クローズ（ポジションは残る）
        order.status = OrderStatus::CloseOrder;
        order.close_price = order.open_price;
        order.open_price = self.price;
        if order.order_type == OrderType::Buy {  // 買い注文でクローズ
            order.profit = (order.open_price - order.close_price) * order.volume;
        }
        else if order.order_type == OrderType::Sell {  //売り注文でクローズ
            order.profit = (order.close_price - order.open_price) * order.volume;
        }

        // ポジションの整理
        self.size -= order.size; 

        return Ok(());
    }
}

#[derive(Debug)]
pub struct Positions {
    long_position: Position,
    short_position: Position,
}

impl Positions {
    fn new() -> Self {
        return Positions {
            long_position: Position::new(),
            short_position: Position::new(),
        };
    }

    fn long_volume(&self) -> f64 {
        return self.long_position.calc_volume();
    }

    fn short_volume(&self) -> f64 {
        return self.short_position.calc_volume();
    }

    fn get_margin(&self, center_price: f64) -> f64 {
        let long_margin = (center_price - self.long_position.price)     // 購入単価 - 現在単価
             * self.long_volume();

        println!("long_margin={}", long_margin);

        let short_margin = (self.short_position.price - center_price)    // 購入単価 - 現在単価
            * self.short_volume();

        println!("short_margin={}", short_margin);

        return long_margin + short_margin;
    }

    /// ClosedOrderによりポジションを更新する。
    ///     1) 逆側のLong/Short ポジションがある分は精算(open_priceの書き換え)
    ///     2) Long/Short ポジションがない（０の場合は、新たにポジションを作る。
    ///     3  Long/Shortポジションが不足した場合はエラーOverPositionを戻すので小さく分割してやり直しする。
    fn update_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        match order.order_type {
            OrderType::Buy => {
                match self.short_position.close_position(order) {
                    Ok(()) => {return Ok(());}
                    Err(e) => {
                        if e == OrderStatus::NoAction {
                            return self.long_position.open_position(order);
                        }
                        else {return Err(e);}
                    }
                }; 
            }
            OrderType::Sell => {
                match self.long_position.close_position(order) {
                    Ok(()) => {return Ok(());}
                    Err(e) => {
                        if e == OrderStatus::NoAction {
                            return self.short_position.open_position(order);
                        }
                        else {return Err(e);}
                    }
                }
            },        
            _ => {
                return Err(OrderStatus::Error);
            }
        }
    }
}

// TODO: ユーザのオリジナルなインジケータを保存できるようにする。
#[derive(Debug)]
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

#[derive(Debug)]
pub struct SessionValue {
    _session_id: String,
    _order_index: i64,
    _start_offset: i64,
    last_sell_price: f64,
    last_buy_price: f64,
    current_time: i64,
    long_orders: Orders,
    shot_orders: Orders,
    positions: Positions,
    order_history: Vec<OrderResult>,
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
            long_orders: Orders::new(true),
            shot_orders: Orders::new(false),
            positions: Positions::new(),
            order_history: vec![],
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
    ///  0. AgentへTick更新イベントを発生させる。
    ///  1. 時刻のUpdate
    ///  ２。マーク価格の更新
    /// 　2. オーダ中のオーダーを更新する。
    /// 　　　　　期限切れオーダーを削除する。
    /// 　　　　　現在のオーダーから執行可能な量を _partial_workから引き算し０になったらオーダ完了（一部約定はしない想定）
    ///
    ///   3. 処理結果を履歴へ投入する。
    /// データがそろうまではFalseをかえす。ウォーミングアップ完了後Trueへ更新する。
    ///
    //ログに実行結果を追加

    ///
    ///  
    fn exec_event_update_time(
        &mut self,
        session: &dyn MarketInfo,
        time_ms: i64,
        action: OrderType,
        price: f64,
        size: f64,
    ) {
        self.current_time = time_ms;

        //  ２。マーク価格の更新。ログとはエッジが逆になる。
        match action {
            OrderType::Buy => self.last_sell_price = price,
            OrderType::Sell => self.last_buy_price = price,
            _ => {}
        }
    }

    /// オーダの期限切れ処理を行う。
    // TODO:　ログに追加する。
    fn exec_event_expire_order(&mut self, current_time_ms: i64) {
        match self.long_orders.expire(current_time_ms) {
            Ok(result) => {
                // TODO: add log
            }
            _ => { // do nothing(nothing to expire)
            }
        }

        match self.shot_orders.expire(current_time_ms) {
            Ok(result) => {
                // TODO: add log
            }
            _ => { // do nothing(nothing to expire)
            }
        }
    }

    //　売りのログのときは、買いオーダーを処理
    // 　買いのログの時は、売りオーダーを処理。
    // 最後にログに追加する。
    fn exec_event_execute_order(
        &mut self,
        current_time_ms: i64,
        order_type: OrderType,
        price: f64,
        size: f64,
    ) {
        match order_type {
            OrderType::Buy => {
                self.long_orders.execute(current_time_ms, price, size);
            }
            OrderType::Sell => {
                self.shot_orders.execute(current_time_ms, price, size);
            }
            _ => {}
        }
    }

    /* TODO: マージンの計算とFundingRate計算はあとまわし */
    fn exec_event(
        &mut self,
        session: &dyn MarketInfo,
        time_ms: i64,
        action: OrderType,
        price: f64,
        size: f64,
    ) {
        // Agentへコールバックする。
        //  0. AgentへTick更新イベントを発生させる。
        //  1. 時刻のUpdate

        //　売り買いの約定が発生していないときは未初期化のためリターン（なにもしない）
        if self.last_buy_price == 0.0 || self.last_buy_price == 0.0 {
            return;
        }

        // 　2. オーダ中のオーダーを更新する。
        // 　　　　　期限切れオーダーを削除する。
        //          毎秒１回実施する（イベントを間引く）

        //現在のオーダーから執行可能な量を _partial_workから引き算し０になったらオーダ完了（一部約定はしない想定）

        //ログに実行結果を追加
    }

    /// オーダーをオーダーリストへ追加する。
    /// _partial_workはオーダーした量と同じ値をセットする。
    /// オーダーエントリー後は値段と時間でソートする。
    ///
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

    fn calc_profit(order: &mut OrderResult) {
        order.fee = order.size * 0.0006;
        order.profit -= order.fee;
    }

    fn log_order_result(&mut self, order: &mut OrderResult) {
        let mut order_result = order.clone();
        order_result.timestamp = self.current_time;

        Self::calc_profit(&mut order_result);

        self.order_history.push(order_result);
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
fn test_build_closed_order(order_type: OrderType, price: f64, size: f64) -> OrderResult {
    let sell_order01 = Order::new(
        1,
        "neworder".to_string(),
        order_type,
        true,
        100,
        price,
        size,
        false,
    );

    let sell_close01 = OrderResult::from_order(2, &sell_order01, OrderStatus::CloseOrder);

    return sell_close01;
}

#[cfg(test)]
fn test_build_orders() -> Vec<OrderResult> {
    use serde::ser::SerializeMap;

    let sell_order01 = Order::new(
        1,
        "neworder".to_string(),
        OrderType::Sell,
        true,
        100,
        200.0,
        200.0,
        false,
    );

    let sell_close01 = OrderResult::from_order(2, &sell_order01, OrderStatus::CloseOrder);

    let mut sell_close02 = sell_close01.clone();
    sell_close02.order_id = "aa".to_string();
    let sell_close03 = sell_close01.clone();
    let sell_close04 = sell_close01.clone();
    let sell_close05 = sell_close01.clone();

    let buy_order = Order::new(
        1,
        "buyorder".to_string(),
        OrderType::Buy,
        true,
        100,
        50.0,
        100.0,
        false,
    );
    let buy_close01 = OrderResult::from_order(2, &buy_order, OrderStatus::CloseOrder);

    let buy_close02 = buy_close01.clone();
    let buy_close03 = buy_close01.clone();
    let buy_close04 = buy_close01.clone();
    let buy_close05 = buy_close01.clone();

    return vec![
        sell_close01,
        sell_close02,
        sell_close03,
        sell_close04,
        sell_close05,
        buy_close01,
        buy_close02,
        buy_close03,
        buy_close04,
        buy_close05,
    ];
}

#[cfg(test)]
mod TestPosition {
    use super::*;

    #[test]
    pub fn test_update_position() {
        let mut orders = test_build_orders();

        let mut position = Position::new();
        // ポジションがないときはなにもしないテスト
        let result = position.close_position(&mut orders[0]);

        assert_eq!(result.err(), Some(OrderStatus::NoAction));

        // ポジションを作る。
        let r = position.open_position(&mut orders[1]);
        println!("{:?}  {:?}", position, r);

        // 購入平均単価へposition が更新されることの確認
        let r = position.open_position(&mut orders[2]);
        println!("{:?} {:?}", position, orders[2]);
        let r = position.open_position(&mut orders[3]);
        println!("{:?} {:?}", position, orders[3]);
        let r = position.open_position(&mut orders[4]);
        println!("{:?} {:?}", position, orders[4]);
        let r = position.open_position(&mut orders[5]);
        println!("{:?} {:?}", position, orders[5]);
        assert_eq!(position.size,  200.0 * 4.0 + 100.0);
        assert_eq!(position.price,  (900.0) / (100.0/50.0 + 200.0/200.0*4.0));

        // ポジションのクローズのテスト（小さいオーダーのクローズ
        let r = position.close_position(&mut orders[6]);
        println!("-- CLOSE ---");
        println!("{:?} {:?}", position, orders[6]);
        assert_eq!(position.size,  200.0 * 4.0 + 100.0 - 100.0);                // 数は減る
        assert_eq!(position.price,  (900.0) / (100.0/50.0 + 200.0/200.0*4.0));  // 単価は同じ



    }
}

#[cfg(test)]
mod TestPositions {
    use super::*;

    #[test]
    fn test_margin() {
        let mut session = Positions::new();

        // test margin
        session.long_position.price = 100.0;
        session.long_position.size = 100.0;
        session.short_position.price = 200.0;
        session.short_position.size = 200.0;
        assert_eq!(session.get_margin(100.25), 0.25 + 99.75);

        // test margin
        session.long_position.price = 400.0;
        session.long_position.size = 100.0; // 0.25 vol  * -200 = -50
        session.short_position.price = 100.0;
        session.short_position.size = 200.0; // 2.0 vol * -100 = -200
        assert_eq!(session.get_margin(200.0), -50.0 + (-200.0));
    }

    #[test]
    fn test_update_position() {
        // 新規だった場合はOpenOrderを返す。
        // クローズだった場合はCLoseOrderを返す。
        // クローズしきれなかった場合は、OpenとCloseを返す。
        // LongとShortをオーダーの中身を見て判断する。

        let mut data = test_build_orders();

        let mut session = Positions::new();

        session.update_position(&mut data[0]);
        println!("{:?}", session);
        session.update_position(&mut data[1]);
        println!("{:?}", session);
        session.update_position(&mut data[2]);
        println!("{:?}", session);
        session.update_position(&mut data[3]);
        println!("{:?}", session);
        session.update_position(&mut data[4]);
        println!("{:?}", session);
        session.update_position(&mut data[5]);
        println!("{:?}", session);
        session.update_position(&mut data[6]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
        session.update_position(&mut data[7]);
        println!("{:?}", session);
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

        // test center price
        session.last_buy_price = 200.0;
        session.last_sell_price = 200.0;
        assert_eq!(session.get_center_price(), 200.0);
    }
}
