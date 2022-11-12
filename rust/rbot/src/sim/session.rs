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

/*
pub trait Session {
    ///　現在のセッション時間をusで取得する。
    fn get_timestamp(&mut self) -> MicroSec;
    fn make_order(
        &mut self,
        timestamp: i64,
        side: OrderSide,
        price: f64,
        size: f64,
        duration_ms: i64,
        message: String,
    ) -> Result<(), OrderStatus>;

    ///　 直近の約定から想定される売り板の最安値（Best Ask価格）を取得する。
    fn get_sell_edge_price(&self) -> f64;

    ///　 直近の約定から想定される買い板の最高値（Best Bit価格）を取得する。
    fn get_buy_edge_price(&self) -> f64;

    /// 未約定でキューに入っているlong orderのサイズ（合計）
    fn get_long_order_size(&self) -> f64;

    ///　未約定のlong order一覧
    fn get_long_orders(&self) -> Vec<Order>;

    /// 未約定でキューに入っているshort orderのサイズ（合計）
    fn get_short_order_size(&self) -> f64;

    ///　未約定のshort order一覧
    fn get_short_orders(&self) -> Vec<Order>;

    /// longポジションのサイズ（合計）
    fn get_long_pos_size(&self) -> f64;

    /// longポジションのサイズ（合計）
    fn get_long_position_size(&self) -> f64;

    /// shortポジションのサイズ（合計）
    fn get_short_position_size(&self) -> f64;

    /*
    /// 現在時刻から width_sec　幅で, count個 OHLCVバーを作る。
    /// Index=0が最新。バーの幅の中にデータが欠落する場合はバーが欠落する（countより少なくなる）
    /// またバーの幅が小さく、バーの数も少ない場合はバーが生成できるエラになる。
    fn ohlcvv(&mut self, width_sec: i64, count: i64) -> Py<PyArray2<f64>>;
    */
}
*/

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

#[pyclass(name = "_DummySession")]
#[derive(Clone, Debug)]
pub struct DummySession {
    _order_index: i64,
    #[pyo3(get)]
    pub sell_board_edge_price: f64, // best ask price　買う時の価格
    #[pyo3(get)]
    pub buy_board_edge_price: f64, // best bit price 　売る時の価格
    #[pyo3(get)]
    pub current_timestamp: i64,
    pub long_orders: OrderQueue,
    pub short_orders: OrderQueue,
    pub positions: Positions,
    // pub order_history: Vec<OrderResult>,
    //pub tick_order_history: Vec<OrderResult>,
    pub wallet_balance: f64, // 入金額
    #[pyo3(get)]
    pub exchange_name: String,
    #[pyo3(get)]
    pub market_name: String,
    //pub agent_on_tick: bool,
    //pub agent_on_clock: bool,
    //pub agent_on_update: bool
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
            // order_history: vec![],
            // tick_order_history: vec![],
            wallet_balance: 0.0,
            exchange_name: exchange_name.to_string().to_ascii_uppercase(),
            market_name: market_name.to_string().to_ascii_uppercase(),
            //agent_on_tick: false,
            //agent_on_clock: false,
            //agent_on_update: false
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
    pub fn get_avairable_balance(&self) -> f64 {
        assert!(false, "not implemnted");
        return 0.0;
    }

    /*
    ///　 直近の約定から想定される売り板の最安値（Best Ask価格）を取得する。
    #[getter]
    fn get_sell_edge_price(&self) -> f64 {
        return self.sell_board_edge_price;
    }

    ///　 直近の約定から想定される買い板の最高値（Best Bit価格）を取得する。
    #[getter]
    fn get_buy_edge_price(&self) -> f64 {
        return self.buy_board_edge_price;
    }
    */

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

    /*
    fn run(&mut self, mut agent: &PyAny, from_time: MicroSec, to_time: MicroSec) {
        log::debug!("session run with agent {:?}", agent);
        self.agent_on_tick = self.has_function(agent, "on_tick");
        log::debug!("want on tick  {:?}", self.agent_on_tick);
        self.agent_on_clock = self.has_function(agent, "on_clock");
        log::debug!("want on clock {:?}", self.agent_on_clock);
        self.agent_on_update = self.has_function(agent, "on_update");
        log::debug!("want on event {:?}", self.agent_on_update);

        let clock_interval  = self.clock_interval(agent);
        log::debug!("clock interval {:?}", clock_interval);

        /*
        let exchange_name: String  = py_session.getattr("exchange_name").unwrap().extract::<String>().unwrap();
        let market_name: String  = py_session.getattr("market_name").unwrap().extract::<String>().unwrap();

        log::debug!("exchange name= {}", exchange_name);
        log::debug!("market   name= {}", market_name);
        */
        // TODO: FTxに依存している！！・名前でよびだせるようにする。
        let mut ftx = FtxMarket::new("BTC-PERP", true);
        log::debug!("FtxMarket created {:?}", &ftx);


        Python::with_gil(|py|{


            let mut statement = ftx.select_all_statement();

            let iter = statement.query_map(params![], |row|{
                let bs_str: String = row.get_unwrap(1);
                let bs = OrderSide::from_str(bs_str.as_str());

                Ok(Trade {
                    time: row.get_unwrap(0),
                    price: row.get_unwrap(2),
                    size: row.get_unwrap(3),
                    order_side: bs,
                    liquid: row.get_unwrap(4),
                    id: row.get_unwrap(5),
                })
            }).unwrap();

            let mut session = DummySession::new();
            let mut s = Py::new(py, session).unwrap();

            for trade in iter {
                match trade {
                    Ok(t) => {
                        log::debug!("{:?}", t);

                        self.tick(&s, &t, agent);
                        // session = s.extract::<DummySession>(py).unwrap();
                    },
                    Err(e) => {
                        log::warn!("err {}", e);
                    }
                }
            }
        });

    }
    */

    /// 現在時刻から width_sec　幅で, count個 OHLCVバーを作る。
    /// Index=0が最新。バーの幅の中にデータが欠落する場合はバーが欠落する（countより少なくなる）
    /// またバーの幅が小さく、バーの数も少ない場合はバーが生成できるエラになる。
    ///
    /*
    fn ohlcvv(&mut self, width_sec: i64, count: i64) -> Py<PyArray2<f64>> {
        return self.db.ohlcvv(width_sec, count);
    }
    */

    /// オーダー作りオーダーリストへ追加する。
    /// 最初にオーダー可能かどうか確認する（余力の有無）
    fn make_order2(
        &mut self,
        side: &str,
        price: f64,
        size: f64,
        duration_sec: i64,
        message: String,
    ) -> PyResult<OrderStatus> {
        return self.make_order(OrderSide::from_str(side), price, size, duration_sec, message);
    }

    fn make_order(
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
            true,
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
    /*
        fn tick(&mut self, session: &Py<DummySession>, trade: &Trade, agent:&PyAny) {
                if self.agent_on_tick {
                let args = (trade.time, trade.order_side.to_string(), trade.price, trade.size);
                let result = agent.call_method1("on_tick", args).unwrap();

                /*
                match result {
                    Ok(_oK) => {
                        //
                    },
                    Err(e) => {
                        log::warn!("Call on_tick Error {:?}", e);
                    }
                }
                */
            }

            if self.agent_on_clock {
                // check first tick after o'clock
            }

            let mut tick_result: Vec<OrderResult> = vec![];

            let exec_results = self.main_exec_event(&tick_result, trade.time, trade.order_side, trade.price, trade.size);
    //        if self.agent_on_update {
                //for exec in exec_results {
                    //let py_result =
    //                let result = agent.call_method1("on_update", args)
                //}

        }

        fn has_function(&self, agent: &PyAny, event_function_name: &str) -> bool {
            match agent.dir().contains(event_function_name) {
                Ok(r) => {
                    return true;
                }
                Err(e) => {
                    return false;
                }
            }
        }

        fn clock_interval(&self, agent: &PyAny) -> i64 {
            if self.has_function(agent, "clock_interval") {
                let interval = agent.call_method0("clock_interval");
                if interval.is_ok(){
                    let ret = interval.unwrap().extract::<i64>();
                    if ret.is_ok() {
                        return SEC(ret.unwrap());
                    }
                }
            }

            log::warn!("Agent has no clock_interval function, use 60[sec] as default");
            return SEC(60); // default
        }
    */

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
    pub fn exec_event_update_time(
        &mut self,
        current_time_ms: i64,
        order_type: OrderSide,
        price: f64,
        _size: f64,
    ) {
        self.current_timestamp = current_time_ms;

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
    pub fn exec_event_expire_order(
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
                return Ok(());
            }
            Err(e) => {
                if e == OrderStatus::OverPosition {
                    match self.positions.split_order(order_result) {
                        Ok(mut child_order) => {
                            let _r = self.positions.update_small_position(order_result);
                            self.log_order_result(tick_result, order_result);
                            let _r = self.positions.update_small_position(&mut child_order);
                            self.log_order_result(tick_result, &mut child_order);

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
        tick_result: &mut Vec<OrderResult>,
        current_time_ms: i64,
        order_type: OrderSide,
        price: f64,
        size: f64,
    ) {
        self.exec_event_update_time(current_time_ms, order_type, price, size);
        //  0. AgentへTick更新イベントを発生させる。
        //  1. 時刻のUpdate

        //　売り買いの約定が発生していないときは未初期化のためリターン
        // （なにもしない）
        if self.buy_board_edge_price == 0.0 || self.buy_board_edge_price == 0.0 {
            return;
        }

        // 　2. オーダ中のオーダーを更新する。
        // 　　　　　期限切れオーダーを削除する。
        //          毎秒１回実施する（イベントを間引く）
        //      処理継続
        match self.exec_event_expire_order(current_time_ms) {
            Ok(result) => {
                self.log_order_result(tick_result, &result);
            }
            _ => {
                // Do nothing
            }
        }

        //現在のオーダーから執行可能な量を _partial_workから引き算し０になったらオーダ成立（一部約定はしない想定）
        match self.exec_event_execute_order(current_time_ms, order_type, price, size) {
            Ok(mut order_result) => {
                //ポジションに追加する。
                let _r = self.update_position(tick_result, &mut order_result);
            }
            Err(_e) => {
                // no
            }
        }
    }

    fn get_timestamp(&mut self) -> MicroSec {
        return self.current_timestamp;
    }
}

///////////////////////////////////////////////////////////////////////
// TEST Suite
///////////////////////////////////////////////////////////////////////

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

        let _r = session.make_order(OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        let r = session.exec_event_execute_order(1, OrderSide::Sell, 50.0, 5.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
        let r = session.exec_event_execute_order(2, OrderSide::Sell, 49.0, 5.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
        let r = session.exec_event_execute_order(3, OrderSide::Sell, 49.0, 5.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
    }

    #[test]
    fn test_update_position() {
        let db = TradeTable::open("BTC-PERP").unwrap();
        let mut session = DummySession::new("FTX", "BTC-PERP");

        let mut tick_result: Vec<OrderResult> = vec![];

        // 新規にポジション作る（ロング）
        let _r = session.make_order(OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        let mut result = session
            .exec_event_execute_order(2, OrderSide::Sell, 49.0, 10.0)
            .unwrap();
        println!("{:?}", session.long_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut tick_result, &mut result);
        println!("{:?}", session.positions);
        println!("{:?}", result);

        // 一部クローズ
        let _r = session.make_order(OrderSide::Sell, 30.0, 8.0, 100, "".to_string());
        println!("{:?}", session.short_orders);

        let mut result = session
            .exec_event_execute_order(3, OrderSide::Buy, 49.0, 10.0)
            .unwrap();
        println!("{:?}", session.short_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut tick_result, &mut result);
        println!("{:?}", session.positions);
        println!("{:?}", result);

        // クローズ＋オープン
        let _r = session.make_order(OrderSide::Sell, 30.0, 3.0, 100, "".to_string());
        println!("{:?}", session.short_orders);

        let mut result = session
            .exec_event_execute_order(4, OrderSide::Buy, 49.0, 10.0)
            .unwrap();
        println!("{:?}", session.short_orders);
        println!("{:?}", result);

        let _r = session.update_position(&mut tick_result, &mut result);
        println!("{:?}", session.positions);
        println!("{:?}", result);

        println!("{:?}", tick_result);
    }

    /*
    [OrderResult { timestamp: 0, order_id: "0000-000000000000-001", order_sub_id: 0, order_type: Buy, post_only: true, create_time: 0, status: OpenPosition, open_price: 50.0, close_price: 0.0, size: 10.0, volume: 0.2, profit: -0.005999999999999999, fee: 0.005999999999999999, total_profit: 0.0 },
     OrderResult { timestamp: 0, order_id: "0000-000000000000-002", order_sub_id: 0, order_type: Sell, post_only: true, create_time: 0, status: ClosePosition, open_price: 50.0, close_price: 30.0, size: 8.0, volume: 0.26666666666666666, profit: -5.338133333333333, fee: 0.0048, total_profit: 0.0 },
     OrderResult { timestamp: 0, order_id: "0000-000000000000-003", order_sub_id: 0, order_type: Sell, post_only: true, create_time: 0, status: ClosePosition, open_price: 50.0, close_price: 30.0, size: 2.0, volume: 0.06666666666666667, profit: -1.3345333333333333, fee: 0.0012, total_profit: 0.0 },
     OrderResult { timestamp: 0, order_id: "0000-000000000000-003", order_sub_id: 1, order_type: Sell, post_only: true, create_time: 0, status: OpenPosition, open_price: 30.0, close_price: 0.0, size: 1.0, volume: 0.03333333333333333, profit: -0.0006, fee: 0.0006, total_profit: 0.0 }]


    */

    #[test]
    fn test_exec_event_execute_order() {
        let db = TradeTable::open("BTC-PERP").unwrap();
        let mut session = DummySession::new("FTX", "BTC-PERP");
        assert_eq!(session.get_timestamp(), 0); // 最初は０

        let mut tick_result: Vec<OrderResult> = vec![];

        // Warm Up
        session.main_exec_event(&mut tick_result, 1, OrderSide::Sell, 150.0, 150.0);
        session.main_exec_event(&mut tick_result, 2, OrderSide::Buy, 151.0, 151.0);

        println!("--make long order--");

        // TODO: 書庫金不足を確認する必要がある.
        let _r = session.make_order(OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        // 売りよりも高い金額のオファーにはなにもしない。
        session.main_exec_event(&mut tick_result, 3, OrderSide::Sell, 50.0, 150.0);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定
        session.main_exec_event(&mut tick_result, 4, OrderSide::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定.２回目で約定。ポジションに登録
        session.main_exec_event(&mut tick_result, 5, OrderSide::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.main_exec_event(&mut tick_result, 5, OrderSide::Sell, 49.5, 5.0);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        println!("--make short order--");

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.

        let _r = session.make_order(OrderSide::Sell, 40.0, 12.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        let _r = session.make_order(OrderSide::Sell, 41.0, 10.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        session.main_exec_event(&mut tick_result, 5, OrderSide::Buy, 49.5, 11.0);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.main_exec_event(&mut tick_result, 5, OrderSide::Buy, 49.5, 20.0);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.main_exec_event(&mut tick_result, 5, OrderSide::Buy, 49.5, 100.0);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.
        let _r = session.make_order(OrderSide::Buy, 80.0, 10.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        // 約定
        session.main_exec_event(&mut tick_result, 8, OrderSide::Sell, 79.5, 200.0);
        println!("{:?}", session.long_orders);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);
    }
}
