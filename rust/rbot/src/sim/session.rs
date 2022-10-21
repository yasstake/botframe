
// use crate::common::order::MarketType;
use crate::common::order::Order;
use crate::common::order::OrderResult;
use crate::common::order::OrderSide;
use crate::common::order::OrderStatus;

use crate::sim::market::OrderQueue;

// use crate::sim::market::Position;
use crate::sim::market::Positions;




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

#[derive(Clone, Debug)]
pub struct SessionValue {
    _order_index: i64,
    pub sell_board_edge_price: f64, // best ask price　買う時の価格
    pub buy_board_edge_price: f64,  // best bit price 　売る時の価格
    pub current_time_ms: i64,
    pub long_orders: OrderQueue,
    pub short_orders: OrderQueue,
    pub positions: Positions,
    pub order_history: Vec<OrderResult>,
    pub tick_order_history: Vec<OrderResult>,
    pub wallet_balance: f64, // 入金額
}

impl SessionValue {
    pub fn new() -> Self {
        return SessionValue {
            _order_index: 0,
            sell_board_edge_price: 0.0,
            buy_board_edge_price: 0.0,
            current_time_ms: 0,
            long_orders: OrderQueue::new(true),
            short_orders: OrderQueue::new(false),
            positions: Positions::new(),
            order_history: vec![],
            tick_order_history: vec![],
            wallet_balance: 0.0,
        };
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
        order_type: OrderSide,
        price: f64,
        _size: f64,
    ) {
        self.current_time_ms = current_time_ms;

        //  ２。マーク価格の更新。ログとはエージェント側からみるとエッジが逆になる。
        match order_type {
            OrderSide::Buy => {
                self.sell_board_edge_price = price;
            }
            OrderSide::Sell => {
                self.buy_board_edge_price = price;
            }
            _ => {}
        }

        // 逆転したら補正。　(ほとんど呼ばれない想定)
        // 数値が初期化されていない場合２つの値は0になっているが、マイナスにはならないのでこれでOK.
        if self.sell_board_edge_price < self.buy_board_edge_price {
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
        order_type: OrderSide,
        price: f64,
        size: f64,
    ) -> Result<OrderResult, OrderStatus> {
        match order_type {
            OrderSide::Buy => {
                return self.short_orders.execute(current_time_ms, price, size);
            }
            OrderSide::Sell => {
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
                return Ok(());
            }
            Err(e) => {
                if e == OrderStatus::OverPosition {
                    match self.positions.split_order(order_result) {
                        Ok(mut child_order) => {
                            let _r = self.positions.update_small_position(order_result);
                            self.log_order_result(order_result);
                            let _r = self.positions.update_small_position(&mut child_order);
                            self.log_order_result(&mut child_order);

                            return Ok(());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                } else {
                    return Err(e);
                }
            }
        }
    }

    fn generate_id(&mut self) -> String {
        self._order_index += 1;
        let index = self._order_index;

        let upper = index / 10000;
        let lower: i64 = index % 10000;

        let id = format! {"{:04}-{:04}", upper, lower};

        return id.to_string();
    }

    // order_resultのログを蓄積する（オンメモリ）
    // ログオブジェクトは配列にいれるためClone する。
    // TODO: この中でAgentへコールバックできるか調査
    fn log_order_result(&mut self, order: &OrderResult) {
        let mut order_result = order.clone();
        order_result.update_time = self.current_time_ms;

        self.calc_profit(&mut order_result);

        let tick_result = order_result.clone();
        self.order_history.push(order_result);
        self.tick_order_history.push(tick_result);
    }

    // トータルだけ損益を計算する。
    // log_order_resultの中で計算している。
    // TODO: もっと上位で設定をかえられるようにする。
    // MaketとTakerでも両率を変更する。
    // ProfitはつねにUSD建でOK。
    fn calc_profit(&self, order: &mut OrderResult) {
        if order.status == OrderStatus::OpenPosition || order.status == OrderStatus::ClosePosition {
            let fee_rate = 0.0001;
                order.fee = order.home_size * fee_rate;
            order.total_profit = order.profit - order.fee;
        }
    }

    /* TODO: マージンの計算とFundingRate計算はあとまわし */
    pub fn main_exec_event(
        &mut self,
        current_time_ms: i64,
        order_type: OrderSide,
        price: f64,
        size: f64,
    ) -> &Vec<OrderResult> {
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
                //ポジションに追加する。
                let _r = self.update_position(&mut order_result);
            }
            Err(e) => {
                if e == OrderStatus::NoAction {
                    println!("ERROR status {:?} ", e);
                }
            }
        }

        return &self.tick_order_history;
    }
}

pub trait Session {
    fn get_timestamp_ms(&mut self) -> i64;
    fn make_order(
        &mut self,
        timestamp: i64,
        side: OrderSide,
        price: f64,
        size: f64,
        duration_ms: i64,
        message: String,
    ) -> Result<(), OrderStatus>;

    /*
    fn get_active_orders(&self) -> [Order];
    fn get_posision(&self) -> (Position, Position); // long/short
    fn diposit(&self, balance: f64);
    fn get_balance(&self) -> f64;
    fn get_avairable_balance(&self) -> f64;
    fn set_indicator(&self, key: &str, value: f64); // TODO: implement laterd
    fn result() -> String; // evaluate session result
    fn on_exec(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64) -> SessionEventType;
    */
    // fn ohlcv(&mut self, width_sec: i64, count: i64) -> ndarray::Array2<f64>;
}

impl Session for SessionValue {
    fn get_timestamp_ms(&mut self) -> i64 {
        return self.current_time_ms;
    }
    /// オーダー作りオーダーリストへ追加する。
    /// 最初にオーダー可能かどうか確認する（余力の有無）
    fn make_order(
        &mut self,
        mut timestamp: i64,
        side: OrderSide,
        price: f64,
        size: f64,
        duration_ms: i64,
        message: String,
    ) -> Result<(), OrderStatus> {
        // TODO: 発注可能かチェックする

        /*
        if 証拠金不足
            return Err(OrderStatus::NoMoney);
        */

        if timestamp == 0 {
            timestamp = self.current_time_ms;
        }

        let order_id = self.generate_id();
        let order = Order::new(
            timestamp,
            order_id,
            side,
            true,
            self.current_time_ms + duration_ms,
            price,
            size,
            message,
        );

        // TODO: enqueue の段階でログに出力する。

        match side {
            OrderSide::Buy => {
                // TODO: Takerになるかどうか確認
                self.long_orders.queue_order(&order);
                return Ok(());
            }
            OrderSide::Sell => {
                self.short_orders.queue_order(&order);
                return Ok(());
            }
            _ => {
                println!("Unknown order type {:?} / use B or S", side);
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



#[allow(unused_results)]
#[cfg(test)]
mod test_session_value {
    use super::*;
    #[test]
    fn test_new() {
        let _session = SessionValue::new();
    }

    #[test]
    fn test_session_value() {
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
    fn test_event_update_time() {
        let mut session = SessionValue::new();
        assert_eq!(session.get_timestamp_ms(), 0); // 最初は０

        session.exec_event_update_time(123, OrderSide::Buy, 101.0, 10.0);
        assert_eq!(session.get_timestamp_ms(), 123);
        //assert_eq!(session.sell_board_edge_price, 101.0); // Agent側からみるとsell_price
        assert_eq!(session.get_center_price(), 0.0); // 初期化未のときは０

        session.exec_event_update_time(135, OrderSide::Sell, 100.0, 10.0);
        assert_eq!(session.get_timestamp_ms(), 135);
        //assert_eq!(session.sell_board_edge_price, 101.0);
        // assert_eq!(session.buy_board_edge_price, 100.0); // Agent側からみるとbuy_price
        assert_eq!(session.get_center_price(), 100.5); // buyとSellの中間
    }

    #[test]
    fn test_exec_event_execute_order0() {
        let mut session = SessionValue::new();

        let _r = session.make_order(0, OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        let r = session.exec_event_execute_order(2, OrderSide::Sell, 50.0, 5.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
        let r = session.exec_event_execute_order(2, OrderSide::Sell, 49.0, 5.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
        let r = session.exec_event_execute_order(2, OrderSide::Sell, 49.0, 5.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
    }

    #[test]
    fn test_update_position() {
        let mut session = SessionValue::new();

        // 新規にポジション作る（ロング）
        let _r = session.make_order(0, OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        let mut result = session
            .exec_event_execute_order(2, OrderSide::Sell, 49.0, 10.0)
            .unwrap();
        println!("{:?}", session.long_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut result);
        println!("{:?}", session.positions);
        println!("{:?}", result);

        // 一部クローズ
        let _r = session.make_order(0, OrderSide::Sell, 30.0, 8.0, 100, "".to_string());
        println!("{:?}", session.short_orders);

        let mut result = session
            .exec_event_execute_order(3, OrderSide::Buy, 49.0, 10.0)
            .unwrap();
        println!("{:?}", session.short_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut result);
        println!("{:?}", session.positions);
        println!("{:?}", result);

        // クローズ＋オープン
        let _r = session.make_order(0, OrderSide::Sell, 30.0, 3.0, 100, "".to_string());
        println!("{:?}", session.short_orders);

        let mut result = session
            .exec_event_execute_order(4, OrderSide::Buy, 49.0, 10.0)
            .unwrap();
        println!("{:?}", session.short_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut result);
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
        session.main_exec_event(1, OrderSide::Sell, 150.0, 150.0);
        session.main_exec_event(2, OrderSide::Buy, 151.0, 151.0);

        println!("--make long order--");

        // TODO: 書庫金不足を確認する必要がある.
        let _r = session.make_order(0, OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        // 売りよりも高い金額のオファーにはなにもしない。
        session.main_exec_event(3, OrderSide::Sell, 50.0, 150.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定
        session.main_exec_event(4, OrderSide::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定.２回目で約定。ポジションに登録
        session.main_exec_event(5, OrderSide::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        session.main_exec_event(5, OrderSide::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        println!("--make short order--");

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.

        let _r = session.make_order(0, OrderSide::Sell, 40.0, 12.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        let _r = session.make_order(0, OrderSide::Sell, 41.0, 10.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        session.main_exec_event(5, OrderSide::Buy, 49.5, 11.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        session.main_exec_event(5, OrderSide::Buy, 49.5, 20.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        session.main_exec_event(5, OrderSide::Buy, 49.5, 100.0);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.
        let _r = session.make_order(0, OrderSide::Buy, 80.0, 10.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        // 約定
        session.main_exec_event(8, OrderSide::Sell, 79.5, 200.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", session.positions);
        println!("{:?}", session.order_history);
    }
}
