use std::sync::Arc;

use exchange::ohlcv_df_from_ohlc;
use exchange::round_down_tick;
use exchange::MarketInfo;
use pyo3::ffi::PyTuple_GetSlice;
use pyo3::ffi::Py_DebugFlag;
use pyo3::ffi::Py_SetRecursionLimit;
use pyo3::prelude::*;

use ::polars::prelude::DataFrame;

#[macro_use]
extern crate anyhow;
extern crate directories;
extern crate time;

pub mod bb;
pub mod exchange;
pub mod pyutil;

use chrono::NaiveDateTime;
use polars::prelude::Series;
use polars_lazy::prelude::*;

/*
Python からよびださされるモジュール

想定利用方法イメージ：

--- Agent

class Agent:
    def on_tick(self, time_ms)
    def on_update(self, time_ms, id, sub_id, status, price, volume) // 後で実装


---- Market(Session) API
    market.start_offset(from_h);

    market.run(agent, interval_sec)

    market.timestamp_ms
    market.make_order(side, price, volume, duration_s)

    market.history
    market.ohlcv

    market.balance
    market.indicator(key, value)
    market.position

    market.result  // あとで実装。


--- Market (history API)
    market.log_start_ms()
    market.log_end_ms()
    market.log_ohlcv



---- Main
    import rbot

    exchange = rbot.DummyBb()
    exchange.load_data(ndays)

    agent = Agent()

    exchange.register_agent(agent)

    exchange.run(10)

    print(exchange.history)





*/

use crate::bb::market::Bb;
use chrono::{DateTime, Utc};

use numpy::IntoPyArray;
use numpy::PyArray2;

use crate::exchange::order::OrderType;
use async_std::task;
use log::debug;
use polars::datatypes::TimeUnit;
use polars_core::datatypes::AnyValue::Float64;

use crate::exchange::session::SessionValue;
use pyo3::types::PyList;

use crate::exchange::ohlcv_from_df;

#[pyclass(module = "rbot")]
struct DummyBb {
    market: Bb,
    _sim_start_ms: i64,
    _sim_end_ms: i64,
    _debug_loop_count: i64,
    order_history: Vec<OrderResult>,
}

struct MainSession {
    df: DataFrame,
    session: SessionValue,
}

impl MainSession {
    pub fn from(df: DataFrame) -> Self {
        return MainSession {
            df: df,
            session: SessionValue::new(),
        };
    }

    pub fn copy_child(&self, ohlcv_df: &DataFrame, ohlcv_window_sec: i64) -> CopySession {
        return CopySession::from(&self, ohlcv_df,ohlcv_window_sec);
    }

    pub fn main_exec_event(&mut self, current_time_ms: i64, order_type: OrderType, price: f64, size: f64) -> &Vec<OrderResult> {
        return self.session.main_exec_event(current_time_ms, order_type, price, size);
    }    
}

impl Session for MainSession {
    fn get_timestamp_ms(&mut self) -> i64 {
        return self.session.get_timestamp_ms();
    }

    fn make_order(
        &mut self,
        side: OrderType,
        price: f64,
        size: f64,
        duration_ms: i64,        
        message: String,
    ) -> Result<(), OrderStatus> {
        return self
            .session
            .make_order(side, price, size, duration_ms, message);
    }
}

use crate::exchange::order::Orders;
use crate::exchange::session::Positions;

#[pyclass]
#[derive(Clone)]
struct CopySession {
    df: DataFrame,
    df_ohlcv: DataFrame,
    sell_board_edge_price: f64,
    buy_board_edge_price: f64,
    current_time_ms: i64,
    long_orders: Orders,
    short_orders: Orders,
    positions: Positions,
    wallet_balance: f64, // 入金額
    _ohlcv_window: i64,
}

impl CopySession {
    fn from(s: &MainSession, ohlcv_df: &DataFrame, ohlcv_window_sec: i64) -> Self {
        return CopySession {
            df: s.df.clone(),
            df_ohlcv: ohlcv_df.clone(),
            sell_board_edge_price: s.session.sell_board_edge_price,
            buy_board_edge_price: s.session.buy_board_edge_price,
            current_time_ms: s.session.current_time_ms,
            long_orders: s.session.long_orders.clone(),
            short_orders: s.session.short_orders.clone(),
            positions: s.session.positions.clone(),
            wallet_balance: s.session.wallet_balance,
            _ohlcv_window: ohlcv_window_sec * 1_000,
        };
    }
}

use crate::exchange::ohlcv_df_from_raw;
use crate::exchange::order::Order;
use polars::prelude::Float64Type;

#[pymethods]
impl CopySession {
    #[getter]
    ///　現在のセッション時間をmsで取得する。
    fn get_current_time(&self) -> i64 {
        return self.current_time_ms;
    }

    #[getter]
    ///　 直近の約定から想定される売り板の最安値（Best Ask価格）を取得する。
    fn get_sell_edge_price(&self) -> f64 {
        return self.sell_board_edge_price;
    }

    #[getter]
    ///　 直近の約定から想定される買い板の最高値（Best Bit価格）を取得する。    
    fn get_buy_edge_price(&self) -> f64 {
        return self.buy_board_edge_price;
    }

    #[getter]
    /// 未約定でキューに入っているlong orderのサイズ（合計）
    fn get_long_order_size(&self) -> f64 {
        return self.long_orders.get_size();
    }

    #[getter]
    ///　未約定のlong order一覧
    fn get_long_orders(&self) -> Vec<Order> {
        return self.long_orders.get_q();
    }

    #[getter]
    /// 未約定でキューに入っているshort orderのサイズ（合計）
    fn get_short_order_size(&self) -> f64 {
        return self.short_orders.get_size();
    }

    #[getter]
    ///　未約定のshort order一覧    
    fn get_short_orders(&self) -> Vec<Order> {
        return self.short_orders.get_q();
    }

    #[getter]
    /// long/short合計のポジション損益（手数料込み）
    fn get_pos_balance(&self) -> f64 {
        return 0.0;
    }

    #[getter]
    /// longポジションのサイズ（合計）
    fn get_long_pos_size(&self) -> f64 {
        return self.positions.get_long_position_size();
    }

    #[getter]
    ///　long ポジションの平均購入単価
    fn get_long_pos_avrage_price(&self) -> f64 {
        return self.positions.get_long_position_price();
    }

    #[getter]
    /// shortポジションのサイズ（合計）
    fn get_short_pos_size(&self) -> f64 {
        return self.positions.get_short_position_size();
    }

    #[getter]
    /// Shortポジションの平均購入単価
    fn get_short_pos_avarage_price(&self) -> f64 {
        return self.positions.get_short_position_price();
    }

    /// 現在時刻から width_sec　幅で, count個 OHLCVバーを作る。
    /// Index=0が最新。バーの幅の中にデータが欠落する場合はバーが欠落する（countより少なくなる）
    /// またバーの幅が小さく、バーの数も少ない場合はバーが生成できるエラになる。
    /// TODO: きちんとしたエラーコードを返す。
    fn ohlcv(&mut self, width_sec: i64, count: i64) -> Py<PyArray2<f64>> {
        if width_sec * 1_000 < self._ohlcv_window {
            println!("ohlcv width is shorter than tick, consider use ohlcv_raw() instead");
        }

        let current_time_ms = self.get_current_time();
        // OHLCVの最新のwindowには将来値がふくまれるのカットする。
        let current_time_ms = round_down_tick(current_time_ms, self._ohlcv_window);

        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        // println!("OHLC currenttime{:?}/{:?}  widh={:?} count={:?}", current_time_ms, PrintTime(current_time_ms), width_sec, count);
        let df = ohlcv_df_from_ohlc(&self.df_ohlcv, current_time_ms, width_sec, count);

        let array: ndarray::Array2<f64> = df
            .select(&["timestamp", "open", "high", "low", "close", "vol"])
            .unwrap()
            .to_ndarray::<Float64Type>()
            .unwrap();

        let py_array2: &PyArray2<f64> = array.into_pyarray(py);

        return py_array2.to_owned();
    }

    fn ohlcv_raw(&mut self, width_sec: i64, count: i64) -> Py<PyArray2<f64>> {
        let current_time_ms = self.get_current_time();
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let df = &self.df;

        let df = ohlcv_df_from_raw(df, current_time_ms, width_sec, count);

        let array: ndarray::Array2<f64> = df
            .select(&["timestamp", "open", "high", "low", "close", "vol"])
            .unwrap()
            .to_ndarray::<Float64Type>()
            .unwrap();

        let py_array2: &PyArray2<f64> = array.into_pyarray(py);

        return py_array2.to_owned();
    }
}

use crate::exchange::order::OrderResult;

impl DummyBb {
    fn on_event(
        dummyBb: &mut DummyBb,
        py: &Python,
        agent: &mut PyAny,
        results: &Vec<OrderResult>,
    ) -> PyResult<()> {
        //call back event update
        for r in results {
            dummyBb.order_history.push(r.clone());
            let result = PyOrderResult::from(r);

            let py_result = Py::new(*py, result)?;
            let obj = py_result.to_object(*py);

            let args = PyTuple::new(*py, [&obj]);
            agent.call_method1("on_update", args)?;
        }
        Ok(())
    }

    fn on_clock(
        py: &Python,
        agent: &mut PyAny,
        session: &MainSession,
        ohlcv_df: &DataFrame,
        interval_sec: i64,
        clock_time: i64,
    ) -> PyResult<()> {
        // let copy_session = CopySession::from(&py_session, &ohlcv_df, interval_sec);
        let copy_session = CopySession::from(&session, &ohlcv_df, interval_sec);
        let py_session2 = Py::new(*py, copy_session)?;

        let result = agent.call_method1("on_tick", (clock_time, py_session2))?;

        Ok(())
    }

    fn make_single_order(&self, session: &mut MainSession, order: &PyOrder) -> PyResult<()> {
        &session.make_order(
            order.side,
            order.price,
            order.size,
            order.duration_ms,            
            order.message.clone(),
        );

        Ok(())
    }

    fn make_order(&self, session: &mut MainSession, result: &PyAny) -> PyResult<()> {
        if result.is_none() {
            return Ok(());
        }

        match result.extract::<PyOrder>() {
            Ok(order) => {
                self.make_single_order(session, &order)?;
            }
            Err(e) => {
                // マルチオーダーの処理
                match result.downcast::<PyList>() {
                    Ok(list) => {
                        for order_item in list.iter() {
                            match result.extract::<PyOrder>() {
                                Ok(order_item) => {
                                    self.make_single_order(session, &order_item)?;
                                }
                                Err(e) => {}
                            }
                        }
                    }
                    Err(e) => {println!("unknown order type {:?}", result);}
                }
            }
        }
        Ok(())
    }
}

#[pymethods]
impl DummyBb {
    #[new]
    fn new() -> Self {
        return DummyBb {
            market: Bb::new(),
            _sim_start_ms: 0,
            _sim_end_ms: 0,
            _debug_loop_count: 0,
            order_history: vec![],
        };
    }

    fn __str__(&mut self) -> String {
        return format!("DummyBB: from:{:?}({:?}) to:{:?}/({:?}) rec_no:{}",
            self.get_log_start_ms().unwrap(), PrintTime(self.get_log_start_ms().unwrap()),
            self.get_log_end_ms().unwrap(), PrintTime(self.get_log_end_ms().unwrap()),
            self.get_number_of_records()
    );
    }

    //--------------------------------------------------------------------------------------------
    // Market (Session) API

    fn run(&mut self, agent: &PyAny, interval_sec: i64) -> PyResult<()> {
        let methods_list = agent.dir();

        let mut want_clock = false;
        if methods_list.contains("on_tick").unwrap() {
            println!("call back tick by {}[sec]", interval_sec);
            want_clock = true;
        }

        let mut want_update = false;
        if methods_list.contains("on_update").unwrap() {
            println!("call back by update");
            want_update = true;
        }

        let mut want_tick = false;
        if methods_list.contains("on_tick").unwrap() {
            println!("call back by all log events");
            want_tick = true;
        }

        if (want_clock == false) && (want_update == false) {
            println!("on_tick() OR on_update() must be implementd")
        }

        // warm up run: １分
        // データ保持期間　カットしない？

        let mut start_time_ms = self.market.start_time();
        let end_time_ms = self.market.end_time();

        if self.get_sim_start_ms() == 0 {
            self.set_sim_start_ms(start_time_ms + 60 * 1_000); // warm up 60 sec
        }

        if self.get_sim_end_ms() == 0 {
            self.set_sim_end_ms(end_time_ms);
        }

        let df = self.market.market.select_df(start_time_ms, end_time_ms);

        let time_s = &df["timestamp"];
        let price_s = &df["price"];
        let size_s = &df["size"];
        let bs_s = &df["bs"];

        let time = &time_s.datetime().unwrap();
        let price = price_s.f64().unwrap();
        let size = size_s.f64().unwrap();
        let bs = bs_s.utf8().unwrap();

        let ohlcv_df = ohlcv_df_from_raw(&df, 0, interval_sec, 0);

        Python::with_gil(|py| {
            let mut py_session = MainSession::from(self.market._df());

            let skip_until = self.get_sim_start_ms();

            for (((t, p), s), b) in time.into_iter().zip(price).zip(size).zip(bs) {
                let time = t.unwrap();
                let price = p.unwrap();
                let size = s.unwrap();
                let bs = b.unwrap();

                if self.get_sim_end_ms() < time {
                    return Ok(());
                }

                log::debug!("{:?} {:?} {:?} {:?}", time, price, size, bs);

                let warm_up_ok_flag = if skip_until < time { true } else { false };

                // すべてのイベントを呼び出し
                if want_tick {
                    let bs = OrderType::from_str(bs).to_long_string();

                    let args = (time, bs, price, size);
                    let result = agent.call_method1("on_tick", args)?;

                    self.make_order(&mut py_session, &result)?;
                }

                // TODO: May skip wam up time
                // 最初のインターバル毎の時刻で呼び出し。
                let current_time_ms = py_session.get_timestamp_ms();
                let clock_time = (time / 1_000 / interval_sec) * 1_000 * interval_sec;

                if want_clock && (current_time_ms < clock_time) && warm_up_ok_flag {
                    if self._debug_loop_count != 0 {
                        self._debug_loop_count -= 1;
                        if self._debug_loop_count == 0 {
                            return Ok(());
                        }
                    }

                    let copy_session = CopySession::from(&py_session, &ohlcv_df, interval_sec);
                    let py_session2 = Py::new(py, copy_session)?;

                    let result = agent.call_method1("on_clock", (clock_time, py_session2))?;

                    self.make_order(&mut py_session, &result)?;

                    /*
                    match result.extract::<PyOrder>() {
                        Ok(order) => {
                            &py_session.make_order(
                                order.side,
                                order.price,
                                order.size,
                                order.duration_ms,                                
                                order.message.clone(),
                            );
                            // println!("Make ORDER {:?}", order);
                        }
                        Err(e) => {}
                    }

                    match result.downcast::<PyList>() {
                        Ok(list) => {
                            for item in list.iter() {
                                println!("{:?}", item);
                            }
                        }
                        Err(e) => {}
                    }
                    */
                }


                let results =
                    py_session
                        // .session
                        .main_exec_event(time, OrderType::from_str(bs), price, size);


                //call back event update
                if want_update && results.len() != 0 {
                    /*


                    //let copy_session = CopySession::from(&py_session, &ohlcv_df, interval_sec);
                    let py_session2 = Py::new(py, copy_session2)?;
                    */

                    for r in results {
                        self.order_history.push(r.clone());
                        let result = PyOrderResult::from(r);

                        let py_result = Py::new(py, result)?;
                        let obj = py_result.to_object(py);

                        let args = PyTuple::new(py, [&obj]);
                        let result = agent.call_method1("on_update", args)?;

                        // self.make_order(&mut py_session, &result)?;
                    }
                }

            }
            Ok(())
        })
    }


    //--------------------------------------------------------------------------------------------
    // Market History API
    // 過去ndays分のログをダウンロードしてロードする。
    fn log_load(&mut self, ndays: usize) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(self.market.download_exec_log_ndays(ndays as i32));
    }

    /// 開始時間と終了時間、間隔を指定してohlcvをつくる。
    /// 出力は時間順。
    fn ohlcv(&mut self, start_time_ms: i64, end_time_ms: i64, width_sec: i64) -> Py<PyArray2<f64>> {
        let df = &self.market._df();

        let df = ohlcv_from_df(df, start_time_ms, end_time_ms, width_sec);

        let array: ndarray::Array2<f64> = df
            .select(&["timestamp", "open", "high", "low", "close", "vol"])
            .unwrap()
            .to_ndarray::<Float64Type>()
            .unwrap();

        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let py_array2: &PyArray2<f64> = array.into_pyarray(py);

        return py_array2.to_owned();
    }

    #[getter]
    fn get_transactions(&self) -> PyResult<PyObject> {
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let list = PyList::empty(py);

        for item in &self.order_history {
            let result = PyOrderResult::from(item);
            let py_result = Py::new(py, result)?;
            let obj = py_result.to_object(py);

            list.append(obj)?;
        }

        return Ok(list.to_object(py));
    }

    fn reset_transaction(&mut self) {
        &self.order_history.clear();
    }

    #[getter]
    fn get_log_start_ms(&self) -> PyResult<i64> {
        return Ok(self.market.start_time());
    }

    #[getter]
    fn get_log_end_ms(&self) -> PyResult<i64> {
        return Ok(self.market.end_time());
    }

    #[setter]
    fn set_sim_start_ms(&mut self, start_ms: i64) {
        self._sim_start_ms = start_ms;
    }

    #[getter]
    fn get_sim_start_ms(&self) -> i64 {
        return self._sim_start_ms;
    }

    #[setter]
    fn set_sim_end_ms(&mut self, end_ms: i64) {
        self._sim_end_ms = end_ms;
    }

    #[getter]
    fn get_sim_end_ms(&self) -> i64 {
        return self._sim_end_ms;
    }

    #[setter]
    fn set_debug_loop_count(&mut self, count: i64) {
        // トリッキーではあるが、カウントダウン側とのバランスをとって＋１
        self._debug_loop_count = count + 1;
    }

    #[getter]
    fn get_number_of_records(&mut self) -> i64 {
        return self.market.market.history_size();
    }
}





use crate::exchange::order::OrderStatus;

#[pyclass]
pub struct PyOrderResult {
    #[pyo3(get)]
    pub timestamp: i64,
    #[pyo3(get)]
    pub order_id: String,
    #[pyo3(get)]
    pub order_sub_id: String, // 分割された場合に利用
    #[pyo3(get)]
    pub order_type: String,
    #[pyo3(get)]
    pub post_only: bool,
    #[pyo3(get)]
    pub create_time: i64,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub open_price: f64,
    #[pyo3(get)]
    pub close_price: f64,
    #[pyo3(get)]
    pub size: f64, // in usd
    #[pyo3(get)]
    pub volume: f64, //in BTC
    #[pyo3(get)]
    pub profit: f64,
    #[pyo3(get)]
    pub fee: f64,
    #[pyo3(get)]
    pub total_profit: f64,
    #[pyo3(get)]
    pub position_change: f64,    
    #[pyo3(get)]
    pub message: String,
}

impl PyOrderResult {
    fn from(result: &OrderResult) -> Self {
        let mut position_change = 0.0;
        match result.status {
            OrderStatus::OpenPosition => {
                if result.order_type == OrderType::Buy {
                    position_change = result.size;
                }
                else if result.order_type == OrderType::Sell {
                    position_change = - result.size;
                }
            }
            OrderStatus::ClosePosition => {
                if result.order_type == OrderType::Buy {
                    position_change = - result.size;
                }
                else if result.order_type == OrderType::Sell {
                    position_change = result.size;                    
                }
            }
            _ => {
                // just ignore (no position change)
            }

        }
        return PyOrderResult {
            timestamp: result.timestamp,
            order_id: result.order_id.clone(),
            order_sub_id: result.order_sub_id.to_string(),
            order_type: result.order_type.to_long_string(),
            post_only: result.post_only,
            create_time: result.create_time,
            status: result.status.to_string(),
            open_price: result.open_price,
            close_price: result.close_price,
            size: result.size,
            volume: result.volume,
            profit: result.profit,
            fee: result.fee,
            total_profit: result.total_profit,
            position_change: position_change,
            message: result.message.clone(),
        };
    }
}

#[pyclass(name = "Order", module = "rbot")]
#[derive(Debug, Clone)]
struct PyOrder {
    side: OrderType,
    price: f64,
    size: f64,
    duration_ms: i64,
    message: String,
}

#[pymethods]

impl PyOrder {
    #[new]
    fn new(side: String, price: f64, size: f64, valid_sec: i64, message: String) -> Self {
        return PyOrder {
            side: OrderType::from_str(side.as_str()),
            price: price,
            size: size,
            duration_ms: valid_sec * 1_000,
            message: message,
        };
    }

    fn __str__(&self) -> PyResult<String> {
        return Ok(format!(
            "side: {}, price: {}, size: {}, duration_ms: {}, {}",
            self.side.to_long_string(),
            self.price,
            self.size,
            self.duration_ms,
            self.message,
        ));
    }
}

use crate::pyutil::PrintTime;
use crate::pyutil::HHMM;
use crate::pyutil::YYYYMMDD;

/// A Python module implemented in Rust.
#[pymodule]
fn rbot(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DummyBb>()?;
    m.add_class::<PyOrderResult>()?;
    m.add_class::<PyOrder>()?;
    // m.add_function(wrap_pyfunction!(sim_run, m)?)?;
    m.add_function(wrap_pyfunction!(HHMM, m)?)?;
    m.add_function(wrap_pyfunction!(YYYYMMDD, m)?)?;
    m.add_function(wrap_pyfunction!(PrintTime, m)?)?;

    Ok(())
}

use crate::exchange::session::Session;

use crate::exchange::Market;

///------------------------------------------------------------------------
/// TEST SECION
///------------------------------------------------------------------------

#[test]
fn test_plugin_all() {
    let mut bb = DummyBb::new();

    /*
    bb.get_balance();
    bb.timestamp();
    bb.load_data(3);
    bb.make_order("BUY", 100.0, 10.0, 100);
    bb.get_history();
    bb.ohlcv(100);
    bb.get_balance();
    bb.set_balance(100.0);
    bb.get_position();
    // bb.run();
    bb.reslut();

    */
}

use numpy::array;
use numpy::PyArray1;

#[test]
fn test_convert_pynumpy() {
    let gil = pyo3::Python::acquire_gil();

    let py_array: &PyArray1<f64> = vec![1.0, 2.0, 3.0].into_pyarray(gil.python());
    //  assert_eq!(py_array.as_slice().unwrap(), &[1, 2, 3]);
    assert!(py_array.resize(100).is_err()); // You can't resize owned-by-rust array.

    let gil = pyo3::Python::acquire_gil();
    let py_array2: &PyArray2<i32> = array![[1, 2, 3], [4, 5, 6]].into_pyarray(gil.python());
    //  assert_eq!(py_array.as_slice().unwrap(), &[1, 2, 3]);
    py_array2.to_owned();
    assert!(py_array.resize(100).is_err()); // You can't resize owned-by-rust array.
}

use pyo3::types::PyTuple;

#[test]
fn test_market_call() {
    let code: &str = r#"class Agent:
    def __init__(self):
        pass
            
    def on_message(self, market):
        print("market")
        print(market.start_time)
"#;

    Python::with_gil(|py| {
        let pymodule = PyModule::from_code(py, code, "", "").unwrap();

        let rbot = py.import("rbot").unwrap();

        let bb = rbot.call_method0("DummyBb").unwrap();

        let agent = pymodule.call_method0("Agent").unwrap();

        let args = PyTuple::new(py, &[bb]);
        let result = agent.call_method1("on_message", args).unwrap();
    })
}

#[test]
fn test_python_call() {
    Python::with_gil(|py| {
        let polars = py.import("polars").unwrap();

        let df_any = polars.call_method0("DataFrame").unwrap();
        // let df_any = polars.call_method1("DataFrame", ([1,1])).unwrap();

        println!("{}", df_any);
        println!("{}", df_any.get_type().name().unwrap());

        let shape_any = df_any.getattr("shape");
        println!("shape{:?}", shape_any);

        let dirs = df_any.dir();
        println!("dir {}", dirs);

        let methods = df_any.get_type().get_item("shape");
        // let  = df_any.get_item("shape");
        println!("{:?}", methods);

        let r = df_any.call_method0("max").unwrap();
        println!("max={}", r);

        let r = df_any.get_type().call_method0("max").unwrap();
        println!("{}", r);

        //let c: PyDataFrame = df_any.extract().unwrap();

        let b = df_any.get_item("").unwrap();
        println!("{}", b);

        let b = df_any.getattr("df").unwrap();
        println!("{}", b);

        let a = df_any.get_item("df").unwrap();
        println!("{}", a);

        let df_ptr = df_any.into_ptr();

        /*
                unsafe {
                    let s = df_ptr.cast::<PyDataFrame>();

                    let shape = (*s).df.shape();

                    println!("Data Shape={} {}", shape.0, shape.1);
                }


                let b = df_any.is_instance_of::<PyDataFrame>().unwrap();
                println!("Instance is PyDataFrame {}", b);

                //let df: &PyDataFrame = df_any.try_into_exact().unwrap();

                //let b = df_any.is_instance_of::<DataFrame>().unwrap();
                //println!("Instance is DataFrame {}", b);

                //df_any.cast_as::<PyDataFrame>();

                //let d = df_any.downcast::<PyDataFrame>().unwrap();
                // let d: &DataFrame = df_any.try_into().unwrap();

                //        println!("{}", d);

                // let my_df: Py<PyDataFrame> = pyo3::PyTryInto::try_into(&df_any, py);

                let df_py: PyRef<PyDataFrame> = df_any.extract().unwrap();

                //        println!("{}", df_py.);
        */
    })
}

#[test]
fn test_python_method_search() {
    let pyscript = r#"
        class Agent:
            def __init__():
                pass
                
            def on_exec(time_ns, buy_or_sell, price, volume):
                print(time_ns, buy_or_sell, price, volume)

            def on_tick(time_ns):
                print(time_ns)

            def other_func():
                print("other")
        "#;

    Python::with_gil(|py| {
        let result = PyModule::from_code(py, pyscript, "test.py", "test");

        /*
        let polars = py.import("polars").unwrap();

        let df_any = polars.call_method0("DataFrame").unwrap();
        // let df_any = polars.call_method1("DataFrame", ([1,1])).unwrap();

        println!("{}", df_any);
        println!("{}", df_any.get_type().name().unwrap());

        let r = df_any.call_method0("max").unwrap();
        println!("max={}", r);

        let r = df_any.get_type().call_method0("max").unwrap();
        println!("{}", r);
        */
    });
}

#[cfg(test)]
mod CopySessionTest {
    use super::*;
    use crate::exchange::{Market, MarketInfo};
    use crate::MainSession;
    use crate::{exchange::session::SessionValue, CopySession};

    fn make_session() -> CopySession {
        let mut market = Market::new();
        let main_session = MainSession::from(market._df());
        let copy_session = CopySession::from(&main_session, &market._df(), 1);

        return copy_session;
    }

    fn make_long_order(timestamp: i64, price: f64, size: f64) -> Order {
        return Order::new(
            timestamp,
            "1".to_string(),
            OrderType::Buy,
            true,
            1000,
            price,
            size,
            "first".to_string(),
            false,
        );
    }

    fn make_short_order(timestamp: i64, price: f64, size: f64) -> Order {
        return Order::new(
            timestamp,
            "1".to_string(),
            OrderType::Sell,
            true,
            1000,
            price,
            size,
            "first".to_string(),
            false,
        );
    }

    #[test]
    ///　現在のセッション時間をmsで取得する。
    fn test_get_current_time() {
        let mut session = make_session();

        assert_eq!(session.get_current_time(), 0);

        session.current_time_ms = 10;
        assert_eq!(session.get_current_time(), 10);
    }

    #[test]
    ///　 直近の約定から想定される売り板の最安値（Best Ask価格）を取得する。
    ///

    fn test_get_sell_buy_edge_price() {
        let mut session = make_session();

        assert_eq!(session.get_sell_edge_price(), 0.0);

        session.sell_board_edge_price = 12.0;
        assert_eq!(session.get_sell_edge_price(), 12.0);
    }

    #[test]
    ///　 直近の約定から想定される買い板の最高値（Best Bit価格）を取得する。    
    fn test_get_buy_edge_price() {
        let mut session = make_session();

        assert_eq!(session.get_buy_edge_price(), 0.0);

        session.buy_board_edge_price = 13.0;
        assert_eq!(session.get_buy_edge_price(), 13.0);
    }

    #[test]
    /// 未約定でキューに入っているlong orderのサイズ（合計）
    fn test_get_long_order_size() {
        let mut session = make_session();

        session
            .long_orders
            .queue_order(&make_long_order(1, 10.0, 12.0));
        assert_eq!(session.get_long_order_size(), 12.0);

        session
            .long_orders
            .queue_order(&make_long_order(1, 10.0, 20.5));
        assert_eq!(session.get_long_order_size(), 32.5);
    }

    #[test]
    ///　未約定のlong order一覧
    fn get_long_orders() {
        let mut session = make_session();
        session.get_long_orders();
        assert!(false, "not tested");
    }

    #[test]
    /// 未約定でキューに入っているshort orderのサイズ（合計）
    fn get_short_order_size() {
        let mut session = make_session();

        session
            .short_orders
            .queue_order(&make_short_order(1, 100.0, 50.5));
        assert_eq!(session.get_short_order_size(), 50.5);

        session
            .short_orders
            .queue_order(&make_short_order(1, 1.1, 2.5));
        assert_eq!(session.get_short_order_size(), 101.1);
    }

    #[test]
    ///　未約定のshort order一覧    
    fn get_short_orders() {
        let mut session = make_session();
        session.get_short_orders();
        assert!(false, "not tested");
    }

    #[test]
    /// long/short合計のポジション損益（手数料込み）
    fn get_pos_balance() {
        let mut session = make_session();
        assert!(false, "not tested");
    }
    #[test]
    /// longポジションのサイズ（合計）
    fn get_long_pos_size_price() {
        let mut session = make_session();

        let order = make_long_order(1, 100.0, 10.0);
        let mut order_result = OrderResult::from_order(1, &order, OrderStatus::InOrder);

        session
            .positions
            .long_position
            .open_position(&mut order_result);

        assert_eq!(session.get_long_pos_size(), 10.0);
        assert_eq!(session.get_long_pos_avrage_price(), 100.0);
    }

    #[test]
    /// shortポジションのサイズ（合計）
    fn get_short_pos_price_size() {
        let mut session = make_session();

        let order = make_short_order(1, 99.0, 9.9);
        let mut order_result = OrderResult::from_order(1, &order, OrderStatus::InOrder);

        session
            .positions
            .short_position
            .open_position(&mut order_result);

        assert_eq!(session.get_short_pos_size(), 9.9);
        assert_eq!(session.get_short_pos_avarage_price(), 99.0);
    }
}
