




use crate::exchange::Market;
use crate::exchange::MarketInfo;

use crate::exchange::order::Order;
use crate::exchange::order::OrderStatus;
use crate::exchange::order::OrderType;
use crate::exchange::order::Orders;

use crate::exchange::order::OrderResult;

use log::debug;

#[derive(Debug)]
///　ポジションの１項目
/// 　Positionsでポジションリストを扱う。
pub struct Position {
    price: f64,
    size: f64, // in USD
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

    /// ポジションをオープンする。
    /// すでに約定は住んでいるはずなので、エラーは出ない。
    /// 新規にポジションの平均取得単価を計算する。
    fn open_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        order.status = OrderStatus::OpenPosition;

        if self.size == 0.0 {
            self.price = order.open_price;
            self.size = order.size;
        } else {
            let new_size = self.size + order.size;
            let volume = self.calc_volume() + order.volume;
            self.price = new_size / volume;
            self.size = new_size;
            log::debug!("volume={} new_size={}", volume, new_size);
        }

        return Ok(());
    }

    /// ポジションを閉じる。
    /// 閉じるポジションがない場合：　          なにもしない
    /// オーダーがポジションを越える場合：      エラーを返す（呼び出し側でオーダーを分割し、ポジションのクローズとオープンを行う）
    /// オーダーよりポジションのほうが大きい場合：オーダ分のポジションを解消する。
    fn close_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        if self.size == 0.0 {
            // ポジションがない場合なにもしない。
            self.price = 0.0; // （誤差蓄積解消のためポジション０のときはリセット）
            log::debug!("No Action");
            return Err(OrderStatus::NoAction);
        } else if self.size < order.size {
            // ポジション以上にクローズしようとした場合なにもしない（別途、クローズとオープンに分割して処理する）
            log::debug!("OverPosition {} {}", self.size, order.size);
            return Err(OrderStatus::OverPosition);
        }
        log::debug!("Normal Close");
        // オーダの全部クローズ（ポジションは残る）
        order.status = OrderStatus::ClosePosition;
        order.close_price = order.open_price;
        order.open_price = self.price;
        if order.order_type == OrderType::Buy {
            // 買い注文でクローズ
            order.profit = (order.close_price - order.open_price) * order.volume;
        } else if order.order_type == OrderType::Sell {
            //売り注文でクローズ
            order.profit = (order.open_price - order.close_price) * order.volume;
        }

        // ポジションの整理
        self.size -= order.size;

        if self.size == 0.0 {
            self.price = 0.0;
        }

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

    /// ポジションからできるマージンを計算する。
    /// 本来は手数料も込みだが、あとまわし　TODO: 手数料計算
    fn get_margin(&self, center_price: f64) -> f64 {
        let long_margin = (center_price - self.long_position.price)     // 購入単価 - 現在単価
             * self.long_volume();

        log::debug!("long_margin={}", long_margin);

        let short_margin = (self.short_position.price - center_price)    // 購入単価 - 現在単価
            * self.short_volume();

        log::debug!("short_margin={}", short_margin);

        return long_margin + short_margin;
    }


    fn update_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        match self.update_small_position(order) {
            Ok(()) => {
                return Ok(())
            },
            Err(e) => {
                if e == OrderStatus::OverPosition {
                    match self.split_order(order) {
                        Ok(mut child_order) => {
                            self.update_small_position(order);
                            self.update_small_position(&mut child_order);
        
                            return Ok(());                        
                        },
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                else {
                    return Err(e);
                }
               
            }
        }

    }

    /// ClosedOrderによりポジションを更新する。
    ///     1) 逆側のLong/Short ポジションがある分は精算(open_priceの書き換え)
    ///     2) Long/Short ポジションがない（０の場合は、新たにポジションを作る。
    ///     3  Long/Shortポジションが不足した場合はエラーOverPositionを戻すので小さく分割してやり直しする。
    fn update_small_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        match order.order_type {
            OrderType::Buy => match self.short_position.close_position(order) {
                Ok(()) => {
                    log::debug!("short position close");
                    return Ok(());
                }
                Err(e) => {
                    if e == OrderStatus::NoAction {
                        log::debug!("long position open");
                        return self.long_position.open_position(order);
                    } else {
                        return Err(e);
                    }
                }
            },
            OrderType::Sell => match self.long_position.close_position(order) {
                Ok(()) => {
                    log::debug!("long position close");
                    return Ok(());
                }
                Err(e) => {
                    if e == OrderStatus::NoAction {
                        log::debug!("short position open");
                        return self.short_position.open_position(order);
                    } else {
                        return Err(e);
                    }
                }
            },
            _ => {
                return Err(OrderStatus::Error);
            }
        }
    }

    // ポジションクローズできるサイズにオーダーを修正。
    // 残りのオーダを新たなオーダとして返却
    // クローズするためには、BuyのときにはShortの大きさが必要（逆になる）
    fn split_order(&self, order: &mut OrderResult) -> Result<OrderResult, OrderStatus> {
        let mut size = 0.0;

        match order.order_type {
            OrderType::Buy => {
                size = self.short_position.size;
            }
            OrderType::Sell => {
                size = self.long_position.size;
            }
            _ => {
                return Err(OrderStatus::Error);
            }
        }

        return order.split_child(size);
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
    sell_board_edge_price: f64,
    buy_board_edge_price: f64,
    current_time_ms: i64,
    long_orders: Orders,
    short_orders: Orders,
    positions: Positions,
    order_history: Vec<OrderResult>,
    tick_order_history: Vec<OrderResult>,
    indicators: Vec<Indicator>,
    wallet_balance: f64, // 入金額
}

impl SessionValue {
    pub fn new() -> Self {
        return SessionValue {
            _session_id: "0000".to_string(), // TODO: implemnet multisession
            _order_index: 0,
            sell_board_edge_price: 0.0,
            buy_board_edge_price: 0.0,
            current_time_ms: 0,
            long_orders: Orders::new(true),
            short_orders: Orders::new(false),
            positions: Positions::new(),
            order_history: vec![],
            tick_order_history: vec![],
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

    pub fn get_center_price(&self) -> f64 {
        if self.buy_board_edge_price == 0.0 || self.sell_board_edge_price == 0.0 {
            return 0.0;
        }

        return (self.buy_board_edge_price + self.sell_board_edge_price) / 2.0;
    }

    // TODO: 計算する。
    pub fn get_avairable_balance(&self) -> f64 {
        assert!(false, "not implemnted");
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
        current_time_ms: i64,
        order_type: OrderType,
        price: f64,
        _size: f64,
    ) {
        self.current_time_ms = current_time_ms;

        //  ２。マーク価格の更新。ログとはエージェント側からみるとエッジが逆になる。(TODO: 逆にするかも)
        match order_type {
            OrderType::Buy => {
                self.sell_board_edge_price = price;
            }
            OrderType::Sell => {
                self.buy_board_edge_price = price;
            }
            _ => {}
        }

        // 逆転したら補正。　(ほとんど呼ばれない想定)
        // 数値が初期化されていない場合２つの値は0になっているが、マイナスにはならないのでこれでOK.
        if self.buy_board_edge_price < self.sell_board_edge_price {
            log::debug!(
                "Force update price buy=> {}  /  sell=> {}",
                self.buy_board_edge_price, self.sell_board_edge_price
            );
            self.sell_board_edge_price = self.buy_board_edge_price;
        }
    }

    /// オーダの期限切れ処理を行う。
    /// エラーは返すが、エラーが通常のため処理する必要はない。
    /// 逆にOKの場合のみ、上位でログ処理する。
    fn exec_event_expire_order(
        &mut self,
        current_time_ms: i64,
    ) -> Result<OrderResult, OrderStatus> {
        // ロングの処理
        match self.long_orders.expire(current_time_ms) {
            Ok(result) => {
                return Ok(result);
            }
            _ => {
                // do nothing
            }
        }
        // ショートの処理
        match self.short_orders.expire(current_time_ms) {
            Ok(result) => {
                return Ok(result);
            }
            _ => {
                // do nothng
            }
        }
        return Err(OrderStatus::NoAction);
    }

    //　売りのログのときは、買いオーダーを処理
    // 　買いのログの時は、売りオーダーを処理。
    fn exec_event_execute_order(
        &mut self,
        current_time_ms: i64,
        order_type: OrderType,
        price: f64,
        size: f64,
    ) -> Result<OrderResult, OrderStatus> {
        match order_type {
            OrderType::Buy => {
                return self.short_orders.execute(current_time_ms, price, size);
            }
            OrderType::Sell => {
                return self.long_orders.execute(current_time_ms, price, size);
            }
            _ => return Err(OrderStatus::Error),
        }
    }

    fn update_position(&mut self, order_result: &mut OrderResult) -> Result<(), OrderStatus> {
        //ポジションに追加しする。
        //　結果がOpen,Closeポジションが行われるのでログに実行結果を追加

        match self.positions.update_small_position(order_result) {
            Ok(()) => {
                self.log_order_result(order_result);
                return Ok(())
            },
            Err(e) => {
                if e == OrderStatus::OverPosition {

                    match self.positions.split_order(order_result) {
                        Ok(mut child_order) => {
                            log::debug!("Split orders {:?} {:?}", order_result.size, child_order.size);

                            self.positions.update_small_position(order_result);
                            self.log_order_result(order_result);
                            self.positions.update_small_position(&mut child_order);
                            self.log_order_result(&mut child_order);
        
                            return Ok(());                        
                        },
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                else {
                    return Err(e);
                }
            }
        }


    }


    // order_resultのログを蓄積する（オンメモリ）
    // ログオブジェクトは配列にいれるためClone する。
    // TODO: この中でAgentへコールバックできるか調査
    fn log_order_result(&mut self, order: &OrderResult) {
        let mut order_result = order.clone();
        order_result.timestamp = self.current_time_ms;

        Self::calc_profit(&mut order_result);

        let tick_result = order_result.clone();
        self.order_history.push(order_result);
        self.tick_order_history.push(tick_result);
    }

    // トータルだけ損益を計算する。
    // log_order_resultの中で計算している。
    // TODO: もっと上位で設定をかえられるようにする。
    // MaketとTakerでも両率を変更する。
    fn calc_profit(order: &mut OrderResult) {
        order.fee = order.size * 0.0006;
        order.total_profit = order.profit - order.fee;
    }
}



pub trait Session {
    fn get_timestamp_ms(&self) -> i64;
    fn main_exec_event(&mut self, current_time_ms: i64, order_type: OrderType, price: f64, size: f64) -> &Vec<OrderResult>;
    fn make_order(&mut self, side: OrderType, price: f64, size: f64, duration_ms: i64) -> Result<(), OrderStatus>; 

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
    // fn ohlcv(&mut self, width_sec: i64, count: i64) -> ndarray::Array2<f64>;
}

impl Session for SessionValue {
    fn get_timestamp_ms(&self) -> i64 {
        return self.current_time_ms;
    }

    /* TODO: マージンの計算とFundingRate計算はあとまわし */
    fn main_exec_event(&mut self, current_time_ms: i64, order_type: OrderType, price: f64, size: f64) -> &Vec<OrderResult>{
        self.tick_order_history.clear();

        self.exec_event_update_time(current_time_ms, order_type, price, size);
        //  0. AgentへTick更新イベントを発生させる。
        //  1. 時刻のUpdate

        //　売り買いの約定が発生していないときは未初期化のためリターン
        // （なにもしない）
        if self.buy_board_edge_price == 0.0 || self.buy_board_edge_price == 0.0 {
            return &self.tick_order_history;
        }

        // 　2. オーダ中のオーダーを更新する。
        // 　　　　　期限切れオーダーを削除する。
        //          毎秒１回実施する（イベントを間引く）
        //      処理継続
        match self.exec_event_expire_order(current_time_ms) {
            Ok(result) => {
                self.log_order_result(&result);
            }
            _ => {
                // Do nothing
            }
        }

        //現在のオーダーから執行可能な量を _partial_workから引き算し０になったらオーダ成立（一部約定はしない想定）
        match self.exec_event_execute_order(current_time_ms, order_type, price, size) {
            Ok(mut order_result) => {
                log::debug!("Position add OK");
                //ポジションに追加しする。
                //　ログはupdate_position内で実施（分割があるため）
                self.update_position(&mut order_result);
            }
            Err(e) => {
                log::debug!("position add fail {:?}", e);
                if e == OrderStatus::NoAction {
                    log::debug!("Pass position increment")
                    // Do nothing
                } else {
                    log::debug!("ERROR status {:?} ", e);
                }
            }
        }

        return &self.tick_order_history;
    }

    /// オーダー作りオーダーリストへ追加する。
    /// 最初にオーダー可能かどうか確認する（余力の有無）
    fn make_order(
        &mut self,
        side: OrderType,
        price: f64,
        size: f64,
        duration_ms: i64,
    ) -> Result<(), OrderStatus> {
        // TODO: 発注可能かチェックする

        /*
        if 証拠金不足
            return Err(OrderStatus::NoMoney);
        */

        let order_id = self.generate_id();
        let order = Order::new(
            self.current_time_ms,
            order_id,
            side,
            true,
            self.current_time_ms + duration_ms,
            price,
            size,
            false,
        );

        // TODO: enqueue の段階でログに出力する。

        match side {
            OrderType::Buy => {
                // TODO: Takerになるかどうか確認
                self.long_orders.queue_order(&order);
                return Ok(());
            }
            OrderType::Sell => {
                self.short_orders.queue_order(&order);
                return Ok(());
            }
            _ => {
                log::debug!("Unknown order type {:?} / use B or S", side);
            }
        }

        return Err(OrderStatus::Error);
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

    let sell_close01 = OrderResult::from_order(2, &sell_order01, OrderStatus::ClosePosition);

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

    let sell_close01 = OrderResult::from_order(2, &sell_order01, OrderStatus::InOrder);

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
    let buy_close01 = OrderResult::from_order(2, &buy_order, OrderStatus::InOrder);

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
        assert_eq!(position.size, 200.0 * 4.0 + 100.0);
        assert_eq!(
            position.price,
            (900.0) / (100.0 / 50.0 + 200.0 / 200.0 * 4.0)
        );

        // ポジションのクローズのテスト（小さいオーダーのクローズ
        println!("-- CLOSE ---");
        let r = position.close_position(&mut orders[6]);
        println!("{:?} {:?}", position, orders[6]);
        assert_eq!(position.size, 200.0 * 4.0 + 100.0 - 100.0); // 数は減る
        assert_eq!(
            position.price,
            (900.0) / (100.0 / 50.0 + 200.0 / 200.0 * 4.0)
        ); // 単価は同じ

        //ポジションクローズのテスト（大きいオーダーのクローズではエラーがかえってくる）
        println!("-- CLOSE BIG ---");
        orders[0].size = 10000.0;
        println!("{:?} {:?}", position, orders[0]);
        let r = position.close_position(&mut orders[0]);

        println!("{:?} {:?}", position, orders[0]);
        assert_eq!(r.err(), Some(OrderStatus::OverPosition));

        //オーダーの分割テスト（大きいオーダを分割して処理する。１つがPositionを全クリアする大きさにして、残りを新規ポジションの大きさにする）
        let mut small_order = &mut orders[0];
        println!("{:?}", small_order);

        let remain_order = &mut small_order.split_child(position.size).unwrap();
        println!("{:?}", small_order);
        println!("{:?}", remain_order);
        println!("{:?}", position);

        position.close_position(small_order);
        println!("{:?}", small_order);
        println!("{:?}", position);
        position.open_position(remain_order);
        println!("{:?}", remain_order);
        println!("{:?}", position);
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
    use serde::de::Expected;

    use super::*;
    #[test]
    fn test_new() {
        let session = SessionValue::new();
    }

    #[test]
    fn test_SessionValue() {
        let mut session = SessionValue::new();

        // IDの生成テスト
        // self.generate_id
        let id = session.generate_id();
        println!("{}", id);
        let id = session.generate_id();
        println!("{}", id);

        //
        let current_time = session.get_timestamp_ms();
        println!("{}", current_time);

        // test center price
        session.buy_board_edge_price = 100.0;
        session.sell_board_edge_price = 100.5;
        assert_eq!(session.get_center_price(), 100.25);

        // test center price
        session.buy_board_edge_price = 200.0;
        session.sell_board_edge_price = 200.0;
        assert_eq!(session.get_center_price(), 200.0);
    }

    #[test]
    fn test_avariable_balance() {
        let mut session = SessionValue::new();
        let balnace = session.get_avairable_balance();
        println!("balance = {}", balnace);
    }

    #[test]
    fn test_event_update_time() {
        let mut session = SessionValue::new();
        assert_eq!(session.get_timestamp_ms(), 0); // 最初は０

        session.exec_event_update_time(123, OrderType::Buy, 101.0, 10.0);
        assert_eq!(session.get_timestamp_ms(), 123);
        assert_eq!(session.sell_board_edge_price, 101.0); // Agent側からみるとsell_price
        assert_eq!(session.get_center_price(), 0.0); // 初期化未のときは０

        session.exec_event_update_time(135, OrderType::Sell, 100.0, 10.0);
        assert_eq!(session.get_timestamp_ms(), 135);
        assert_eq!(session.sell_board_edge_price, 101.0);
        assert_eq!(session.buy_board_edge_price, 100.0); // Agent側からみるとbuy_price
        assert_eq!(session.get_center_price(), 100.5); // buyとSellの中間
    }

    #[test]
    fn test_exec_event_execute_order0() {
        let mut session = SessionValue::new();

        session.make_order(OrderType::Buy, 50.0, 10.0, 100);
        println!("{:?}", session.long_orders);

        let r = session.exec_event_execute_order(2, OrderType::Sell, 50.0, 5.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
        let r = session.exec_event_execute_order(2, OrderType::Sell, 49.0, 5.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
        let r = session.exec_event_execute_order(2, OrderType::Sell, 49.0, 5.0);
        println!("{:?}", session.long_orders);        
        println!("{:?}", r);
    }

    #[test]
    fn test_update_position() {
        let mut session = SessionValue::new();

        // 新規にポジション作る（ロング）
        session.make_order(OrderType::Buy, 50.0, 10.0, 100);
        println!("{:?}", session.long_orders);

        let mut result = session.exec_event_execute_order(2, OrderType::Sell, 49.0, 10.0).unwrap();
        println!("{:?}", session.long_orders);        
        println!("{:?}", result);

        session.update_position(&mut result);
        println!("{:?}", session.positions);
        println!("{:?}", result);        

        // 一部クローズ
        session.make_order(OrderType::Sell, 30.0, 8.0, 100);
        println!("{:?}", session.short_orders);

        let mut result = session.exec_event_execute_order(3, OrderType::Buy, 49.0, 10.0).unwrap();
        println!("{:?}", session.short_orders);
        println!("{:?}", result);

        session.update_position(&mut result);
        println!("{:?}", session.positions);
        println!("{:?}", result);        

        // クローズ＋オープン
        session.make_order(OrderType::Sell, 30.0, 3.0, 100);
        println!("{:?}", session.short_orders);

        let mut result = session.exec_event_execute_order(4, OrderType::Buy, 49.0, 10.0).unwrap();
        println!("{:?}", session.short_orders);
        println!("{:?}", result);

        session.update_position(&mut result);
        println!("{:?}", session.positions);
        println!("{:?}", result);  
        
        println!("{:?}", session.order_history);


    }


/*
[OrderResult { timestamp: 0, order_id: "0000-000000000000-001", order_sub_id: 0, order_type: Buy, post_only: true, create_time: 0, status: OpenPosition, open_price: 50.0, close_price: 0.0, size: 10.0, volume: 0.2, profit: -0.005999999999999999, fee: 0.005999999999999999, total_profit: 0.0 },
 OrderResult { timestamp: 0, order_id: "0000-000000000000-002", order_sub_id: 0, order_type: Sell, post_only: true, create_time: 0, status: ClosePosition, open_price: 50.0, close_price: 30.0, size: 8.0, volume: 0.26666666666666666, profit: -5.338133333333333, fee: 0.0048, total_profit: 0.0 }, 
 OrderResult { timestamp: 0, order_id: "0000-000000000000-003", order_sub_id: 0, order_type: Sell, post_only: true, create_time: 0, status: ClosePosition, open_price: 50.0, close_price: 30.0, size: 2.0, volume: 0.06666666666666667, profit: -1.3345333333333333, fee: 0.0012, total_profit: 0.0 }, 
 OrderResult { timestamp: 0, order_id: "0000-000000000000-003", order_sub_id: 1, order_type: Sell, post_only: true, create_time: 0, status: OpenPosition, open_price: 30.0, close_price: 0.0, size: 1.0, volume: 0.03333333333333333, profit: -0.0006, fee: 0.0006, total_profit: 0.0 }]


*/


    #[test]
    fn test_exec_event_execute_order() {
        let mut session = SessionValue::new();
        assert_eq!(session.get_timestamp_ms(), 0); // 最初は０

        // Warm Up
        session.main_exec_event(1, OrderType::Sell, 150.0, 150.0);
        session.main_exec_event(2, OrderType::Buy, 151.0, 151.0);

        println!("--make long order--");

        // TODO: 書庫金不足を確認する必要がある.
        session.make_order(OrderType::Buy, 50.0, 10.0, 100);
        println!("{:?}", session.long_orders);

        // 売りよりも高い金額のオファーにはなにもしない。
        session.main_exec_event(3, OrderType::Sell, 50.0, 150.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定
        session.main_exec_event(4, OrderType::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定.２回目で約定。ポジションに登録
        session.main_exec_event(5, OrderType::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        session.main_exec_event(5, OrderType::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        println!("--make short order--");

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.

        session.make_order(OrderType::Sell, 40.0, 12.0, 100);
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        session.make_order(OrderType::Sell, 41.0, 10.0, 100);
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        session.main_exec_event(5, OrderType::Buy, 49.5, 11.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);


        session.main_exec_event(5, OrderType::Buy, 49.5, 20.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);


        session.main_exec_event(5, OrderType::Buy, 49.5, 100.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);


        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.
        session.make_order(OrderType::Buy, 80.0, 10.0, 100);
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        // 約定
        session.main_exec_event(8, OrderType::Sell, 79.5, 200.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

    }
}


/*
[OrderResult { timestamp: 5, order_id: "0000-000000000000-001", order_sub_id: 0, order_type: Buy, post_only: true, create_time: 2, status: OpenPosition, open_price: 50.0, close_price: 0.0, size: 10.0, volume: 0.2, profit: 0.0, fee: 0.005999999999999999, total_profit: -0.005999999999999999 }, 
OrderResult { timestamp: 5, order_id: "0000-000000000000-002", order_sub_id: 0, order_type: Sell, post_only: true, create_time: 5, status: ClosePosition, open_price: 50.0, close_price: 40.0, size: 10.0, volume: 0.25, profit: 2.5, fee: 0.005999999999999999, total_profit: 2.494 }, 
OrderResult { timestamp: 5, order_id: "0000-000000000000-002", order_sub_id: 1, order_type: Sell, post_only: true, create_time: 5, status: OpenPosition, open_price: 40.0, close_price: 0.0, size: 2.0, volume: 0.05, profit: 0.0, fee: 0.0012, total_profit: -0.0012 }, 
OrderResult { timestamp: 8, order_id: "0000-000000000000-004", order_sub_id: 0, order_type: Buy, post_only: true, create_time: 5, status: ClosePosition, open_price: 40.0, close_price: 80.0, size: 2.0, volume: 0.025, profit: 1.0, fee: 0.0012, total_profit: 0.9988 }, 
OrderResult { timestamp: 8, order_id: "0000-000000000000-004", order_sub_id: 1, order_type: Buy, post_only: true, create_time: 5, status: OpenPosition, open_price: 80.0, close_price: 0.0, size: 8.0, volume: 0.1, profit: 0.0, fee: 0.0048, total_profit: -0.0048 }]
*/



/*
TODO:  ポジションオープンのときにログにクローズと書かれる
TODO:  ポジションクローズができない。


*/
