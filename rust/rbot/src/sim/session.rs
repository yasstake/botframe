// use crate::common::order::MarketType;
use crate::common::order::Order;
use crate::common::order::OrderResult;
use crate::common::order::OrderSide;
use crate::common::order::OrderStatus;

use crate::common::order::Trade;
use crate::exchange::ftx::DbForeach;
use crate::exchange::ftx::FtxMarket;
use crate::sim::market::OrderQueue;

// use crate::sim::market::Position;
use crate::common::time::MicroSec;
use crate::sim::market::Positions;

use crate::db::sqlite::TradeTable;
use numpy::PyArray2;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::pyclass;
use pyo3::prelude::pymethods;
use pyo3::prelude::Py;
use rusqlite::params;

use crate::{MICRO_SECOND, SEC};
use pyo3::*;


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
///         available_balance よりsizeが小さい（余裕がある）
///             oK -> available_balanceからorder_marginへへSize分を移動させてオーダー処理実行
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

#[pyclass(name = "_DummySession")]
#[derive(Clone, Debug)]
pub struct DummySession {
    _order_index: i64,
    #[pyo3(get)]
    pub sell_board_edge_price: f64, // best ask price　買う時の価格
    #[pyo3(get)]
    pub current_timestamp: i64,
    #[pyo3(get)]
    pub buy_board_edge_price: f64, // best bit price 　売る時の価格
    #[pyo3(get)]
    pub exchange_name: String,
    #[pyo3(get)]
    pub market_name: String,
    pub long_orders: OrderQueue,
    pub short_orders: OrderQueue,
    pub positions: Positions,
    pub wallet_balance: f64, // 入金額
}

#[pymethods]
impl DummySession {
    #[new]
    pub fn new(exchange_name: &str, market_name: &str) -> Self {
        return DummySession {
            _order_index: 0,
            sell_board_edge_price: 0.0,
            buy_board_edge_price: 0.0,
            current_timestamp: 0,
            long_orders: OrderQueue::new(true),
            short_orders: OrderQueue::new(false),
            positions: Positions::new(),
            wallet_balance: 0.0,
            exchange_name: exchange_name.to_string().to_ascii_uppercase(),
            market_name: market_name.to_string().to_ascii_uppercase(),
        };
    }

    #[getter]
    pub fn get_center_price(&self) -> f64 {
        if self.buy_board_edge_price == 0.0 || self.sell_board_edge_price == 0.0 {
            return 0.0;
        }

        return (self.buy_board_edge_price + self.sell_board_edge_price) / 2.0;
    }

    #[getter]
    // TODO: 計算する。
    pub fn get_available_balance(&self) -> f64 {
        assert!(false, "not implemented");
        return 0.0;
    }

    /// 未約定でキューに入っているlong orderのサイズ（合計）
    #[getter]
    fn get_long_order_size(&self) -> f64 {
        return self.long_orders.get_size();
    }

    ///　未約定のlong order一覧
    #[getter]
    fn get_long_orders(&self) -> Vec<Order> {
        return self.long_orders.get_q();
    }

    /// 未約定でキューに入っているshort orderのサイズ（合計）
    #[getter]
    fn get_short_order_size(&self) -> f64 {
        return self.short_orders.get_size();
    }

    ///　未約定のshort order一覧
    #[getter]
    fn get_short_orders(&self) -> Vec<Order> {
        return self.short_orders.get_q();
    }

    /// longポジションのサイズ（合計）
    #[getter]
    fn get_long_position_size(&self) -> f64 {
        return self.positions.get_long_position_size();
    }

    /// shortポジションのサイズ（合計）
    #[getter]
    fn get_short_position_size(&self) -> f64 {
        return self.positions.get_short_position_size();
    }


    /// オーダー作りオーダーリストへ追加する。
    /// 最初にオーダー可能かどうか確認する（余力の有無）
    fn make_order(
        &mut self,
        side: &str,
        price: f64,
        size: f64,
        duration_sec: i64,
        message: String,
    ) -> PyResult<OrderStatus> {
        return self._make_order(OrderSide::from_str(side), price, size, duration_sec, message);
    }

    fn _make_order(
        &mut self,
        side: OrderSide,
        price: f64,
        size: f64,
        duration_sec: i64,
        message: String,
    ) -> PyResult<OrderStatus> {
        // TODO: 発注可能かチェックする
        /*
        if 証拠金不足
            return Err(OrderStatus::NoMoney);
        */

        let timestamp = self.current_timestamp;

        let order_id = self.generate_id();
        let order = Order::new(
            timestamp,
            order_id,
            side,
            true,   // TODO: post only 以外のオーダを検討する。
            self.current_timestamp + SEC(duration_sec),
            price,
            size,
            message,
        );

        // TODO: enqueue の段階でログに出力する。
        match side {
            OrderSide::Buy => {
                // TODO: Takerになるかどうか確認
                self.long_orders.queue_order(&order);
                return Ok(OrderStatus::InOrder);
            }
            OrderSide::Sell => {
                self.short_orders.queue_order(&order);
                return Ok(OrderStatus::InOrder);
            }
            _ => {
                println!("Unknown order type {:?} / use B or S", side);
            }
        }

        return Err(PyTypeError::new_err("Order fail"));
    }
}

impl DummySession {
    /// price x sizeのオーダを発行できるか確認する。
    ///   if unrealised_pnl > 0:
    ///      available_balance = wallet_balance - (position_margin + occ_closing_fee + occ_funding_fee + order_margin)
    ///      if unrealised_pnl < 0:
    ///          available_balance = wallet_balance - (position_margin + occ_closing_fee + occ_funding_fee + order_margin) + unrealised_pnl

    /*
    fn check_margin(&self, price: f64, volume: f64) -> bool {
        const LEVERAGE: f64 = 1.0; // TODO: まずはレバレッジ１倍固定から始める。
        let mut order_amount = price * volume;

        order_amount = order_amount / LEVERAGE;

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
    ///
    ///
    fn update_trade_time(
        &mut self,
        trade: &Trade
    ) {
        self.current_timestamp = trade.time;
    }

    fn update_edge_price(&mut self, trade: &Trade) {
            //  ２。マーク価格の更新。ログとはエージェント側からみるとエッジが逆になる。
        match trade.order_side {
            OrderSide::Buy => {
                self.sell_board_edge_price = trade.price;
            }
            OrderSide::Sell => {
                self.buy_board_edge_price = trade.price;
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
    pub fn update_expire_order(
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
                // do nothing
            }
        }
        return Err(OrderStatus::NoAction);
    }

    //　売りのログのときは、買いオーダーを処理
    // 　買いのログの時は、売りオーダーを処理。
    fn update_order_queue(
        &mut self,
        trade: &Trade
    ) -> Result<OrderResult, OrderStatus> {
        return match trade.order_side {
            OrderSide::Buy => {
                self.short_orders.consume(trade)
            }
            OrderSide::Sell => {
                self.long_orders.consume(trade)
            }
            _ => {Err(OrderStatus::Error)}
        }
    }

    fn update_position(
        &mut self,
        mut tick_result: &mut Vec<OrderResult>,
        order_result: &mut OrderResult,
    ) -> Result<(), OrderStatus> {
        //ポジションに追加しする。
        //　結果がOpen,Closeポジションが行われるのでログに実行結果を追加

        match self.positions.update_small_position(order_result) {
            Ok(()) => {
                self.log_order_result(tick_result, order_result);
                Ok(())
            }
            Err(e) => {
                if e == OrderStatus::OverPosition {
                    match self.positions.split_order(order_result) {
                        Ok(mut child_order) => {
                            let _r = self.positions.update_small_position(order_result);
                            self.log_order_result(tick_result, order_result);
                            let _r = self.positions.update_small_position(&mut child_order);
                            self.log_order_result(tick_result, &mut child_order);

                            Ok(())
                        }
                        Err(e) => {
                            Err(e)
                        }
                    }
                } else {
                    Err(e)
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
    fn log_order_result(&mut self, mut tick_log: &mut Vec<OrderResult>, order: &OrderResult) {
        let mut order_result = order.clone();
        order_result.update_time = self.current_timestamp;

        self.calc_profit(&mut order_result);

        //        let tick_result = order_result.clone();
        //        self.order_history.push(order_result);
        tick_log.push(order_result);
    }

    // トータルだけ損益を計算する。
    // log_order_resultの中で計算している。
    // TODO: もっと上位で設定をかえられるようにする。
    // MakerとTakerでも両率を変更する。
    // ProfitはつねにUSD建でOK。
    fn calc_profit(&self, order: &mut OrderResult) {
        if order.status == OrderStatus::OpenPosition || order.status == OrderStatus::ClosePosition {
            let fee_rate = 0.0001;
            order.fee = order.home_size * fee_rate;
            order.total_profit = order.profit - order.fee;
        }
    }

    /* TODO: マージンの計算とFundingRate計算はあとまわし */
    pub fn process_trade(
        &mut self,
        trade: &Trade,
        tick_result: &mut Vec<OrderResult>){
/*
        &mut self,

        current_time_ms: i64,
        order_type: OrderSide,
        price: f64,
        size: f64,
*/
        self.update_trade_time(trade);

        self.update_edge_price(trade);
        // 初期化未のためリターン。次のTickで処理。
        if self.buy_board_edge_price == 0.0 || self.buy_board_edge_price == 0.0 {
            return;
        }

        // 　2. オーダ中のオーダーを更新する。
        // 　　　　　期限切れオーダーを削除する。
        //          毎秒１回実施する（イベントを間引く）
        //      処理継続
        match self.update_expire_order(self.current_timestamp) {
            Ok(result) => {
                self.log_order_result(tick_result, &result);
            }
            _ => {
                // Do nothing
            }
        }

        //現在のオーダーから執行可能な量を _partial_workから引き算し０になったらオーダ成立（一部約定はしない想定）
        match self.update_order_queue(trade){
            Ok(mut order_result) => {
                //ポジションに追加する。
                let _r = self.update_position(tick_result, &mut order_result);
            }
            Err(_e) => {
                // no
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////
// TEST Suite
///////////////////////////////////////////////////////////////////////

// TODO: EXPIRE時にFreeを計算してしまう（キャンセルオーダー扱い）
// TODO: EXPIRE時にポジションか、オーダーキューに残っている。
// TODO:　キャンセルオーダーの実装。

#[allow(unused_results)]
#[cfg(test)]
mod test_session_value {
    use super::*;
    #[test]
    fn test_new() {
        let db = TradeTable::open("BTC-PERP").unwrap();
        let _session = DummySession::new("FTX", "BTC-PERP");
    }

    #[test]
    fn test_session_value() {
        let db = TradeTable::open("BTC-PERP").unwrap();
        let mut session = DummySession::new("FTX", "BTC-PERP");

        // IDの生成テスト
        // self.generate_id
        let id = session.generate_id();
        println!("{}", id);
        let id = session.generate_id();
        println!("{}", id);

        //
        let current_time = session.get_timestamp();
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
        let db = TradeTable::open("BTC-PERP").unwrap();
        let mut session = DummySession::new("FTX", "BTC-PERP");
        assert_eq!(session.get_timestamp(), 0); // 最初は０

        session.exec_event_update_time(123, OrderSide::Buy, 101.0, 10.0);
        assert_eq!(session.get_timestamp(), 123);
        //assert_eq!(session.sell_board_edge_price, 101.0); // Agent側からみるとsell_price
        assert_eq!(session.get_center_price(), 0.0); // 初期化未のときは０

        session.exec_event_update_time(135, OrderSide::Sell, 100.0, 10.0);
        assert_eq!(session.get_timestamp(), 135);
        //assert_eq!(session.sell_board_edge_price, 101.0);
        // assert_eq!(session.buy_board_edge_price, 100.0); // Agent側からみるとbuy_price
        assert_eq!(session.get_center_price(), 100.5); // buyとSellの中間
    }

    #[test]
    fn test_exec_event_execute_order0() {
        let db = TradeTable::open("BTC-PERP").unwrap();
        let mut session = DummySession::new("FTX", "BTC-PERP");

        let _r = session._make_order(OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        let r = session.update_order_queue(&Trade{
            time: 1,
            order_side: OrderSide::Sell,
            price: 50.0,
            size: 5.0,
            liquid: false,
            id: "".to_string()
        });
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
        let r = session.update_order_queue(&Trade{
            time: 2,
            order_side: OrderSide::Sell,
            price: 49.0,
            size: 5.0,
            liquid: false,
            id: "".to_string()
        });
        println!("{:?}", session.long_orders);
        println!("{:?}", r);

        let r = session.update_order_queue(&Trade{
            time: 3,
            order_side: OrderSide::Sell,
            price: 49.0,
            size: 5.0,
            liquid: false,
            id: "".to_string()
        });
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
    }

    #[test]
    fn test_update_position() {
        let db = TradeTable::open("BTC-PERP").unwrap();
        let mut session = DummySession::new("FTX", "BTC-PERP");

        let mut tick_result: Vec<OrderResult> = vec![];

        // 新規にポジション作る（ロング）
        let _r = session._make_order(OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        let mut result = session
            .update_order_queue(&Trade{
                time: 2,
                order_side: OrderSide::Buy,
                price: 49.0,
                size: 10.0,
                liquid: false,
                id: "".to_string()
            })
            .unwrap();
        println!("{:?}", session.long_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut tick_result, &mut result.unwrap());
        println!("{:?}", session.positions);
        println!("{:?}", result);

        // 一部クローズ
        let _r = session._make_order(OrderSide::Sell, 30.0, 8.0, 100, "".to_string());
        println!("{:?}", session.short_orders);

        let mut result = session
            .update_order_queue(&Trade{
                time: 3,
                order_side: OrderSide::Buy,
                price: 49.0,
                size: 10.0,
                liquid: false,
                id: "".to_string()
            })

            .unwrap();
        println!("{:?}", session.short_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut tick_result, &mut result.unwrap());
        println!("{:?}", session.positions);
        println!("{:?}", result);

        // クローズ＋オープン
        let _r = session._make_order(OrderSide::Sell, 30.0, 3.0, 100, "".to_string());
        println!("{:?}", session.short_orders);

        let mut result = session
            .update_order_queue(&Trade{
                time: 4,
                order_side: OrderSide::Buy,
                price: 49.0,
                size: 10.0,
                liquid: false,
                id: "".to_string()
            })
            .unwrap();
        println!("{:?}", session.short_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut tick_result, &mut result.unwrap());
        println!("{:?}", session.positions);
        println!("{:?}", result);

        println!("{:?}", tick_result);
    }


    #[test]
    fn test_exec_event_execute_order() {
        let mut tick_result: Vec<OrderResult> = vec![];

        let db = TradeTable::open("BTC-PERP").unwrap();
        let mut session = DummySession::new("FTX", "BTC-PERP");
        assert_eq!(session.get_timestamp(), 0); // 最初は０

        let mut tick_result: Vec<OrderResult> = vec![];

        // Warm Up
        session.process_trade(&Trade {
            time: 1,
            order_side: OrderSide::Buy,
            price: 150.0,
            size: 150.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);

        session.process_trade(&Trade {
            time: 2,
            order_side: OrderSide::Buy,
            price: 151.0,
            size: 151.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);

        println!("--make long order--");

        // TODO: 書庫金不足を確認する必要がある.
        let _r = session._make_order(OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        // 売りよりも高い金額のオファーにはなにもしない。
        session.process_trade(&Trade{
            time: 3,
            order_side: OrderSide::Sell,
            price: 50.0,
            size: 150.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定
        session.process_trade(&Trade{
            time: 4,
            order_side: OrderSide::Sell,
            price: 49.5,
            size: 5.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定.２回目で約定。ポジションに登録
        session.process_trade(&Trade{
            time: 5,
            order_side: OrderSide::Sell,
            price: 49.5,
            size: 5.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.process_trade(&Trade{
            time: 5,
            order_side: OrderSide::Sell,
            price: 49.5,
            size: 5.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        println!("--make short order--");

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.

        let _r = session._make_order(OrderSide::Sell, 40.0, 12.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        let _r = session._make_order(OrderSide::Sell, 41.0, 10.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        session.process_trade(&Trade{
            time: 5,
            order_side: OrderSide::Sell,
            price: 49.5,
            size: 11.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.process_trade(&Trade{
            time: 5,
            order_side: OrderSide::Sell,
            price: 49.5,
            size: 20.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.process_trade(&Trade{
            time: 5,
            order_side: OrderSide::Sell,
            price: 49.5,
            size: 100.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.
        let _r = session._make_order(OrderSide::Buy, 80.0, 10.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        // 約定
        session.process_trade(&Trade{
            time: 8,
            order_side: OrderSide::Sell,
            price: 79.5,
            size: 200.0,
            liquid: false,
            id: "".to_string()
        }, &mut tick_result);
        println!("{:?}", session.long_orders);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);
    }
}
