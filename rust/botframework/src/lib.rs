use std::sync::Arc;

use exchange::ohlcv_df_from_ohlc;
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
    fn from(bb: &mut DummyBb) -> Self {
        return MainSession {
            df: bb.market.market._df(),
            session: SessionValue::new(),
        };
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
    ) -> Result<(), OrderStatus> {
        return self.session.make_order(side, price, size, duration_ms);
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
    _ohlcv_width: i64,
}

impl CopySession {
    fn from(s: &MainSession, ohlcv_df: &DataFrame, ohlcv_width: i64) -> Self {
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
            _ohlcv_width: ohlcv_width,
        };
    }
}

use crate::exchange::ohlcv_df_from_raw;
use crate::exchange::order::Order;
use polars::prelude::Float64Type;

#[pymethods]
impl CopySession {
    #[getter]
    fn get_current_time(&self) -> i64 {
        return self.current_time_ms;
    }

    #[getter]
    fn get_sell_board_edge_price(&self) -> f64 {
        return self.sell_board_edge_price;
    }

    #[getter]
    fn get_buy_board_edge_price(&self) -> f64 {
        return self.buy_board_edge_price;
    }

    #[getter]
    fn get_long_orders(&self) -> Vec<Order> {
        return self.long_orders.get_q();
    }

    #[getter]
    fn get_short_orders(&self) -> Vec<Order> {
        return self.short_orders.get_q();
    }

    #[getter]
    fn get_long_position(&self) -> (f64, f64) {
        return self.positions.get_long_position();
    }

    #[getter]
    fn get_short_position(&self) -> (f64, f64) {
        return self.positions.get_short_position();
    }

    fn ohlcv(&mut self, width_sec: i64, count: i64) -> Py<PyArray2<f64>> {
        if width_sec < self._ohlcv_width {
            println!("ohlcv width is shorter than tick, consider use ohlcv_raw() instead");
        }

        let current_time_ms = self.get_current_time();
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let df = &self.df_ohlcv;

        let df = ohlcv_df_from_ohlc(df, current_time_ms, width_sec, count);

        let array: ndarray::Array2<f64> = df
            .select(&["time", "open", "high", "low", "close", "vol"])
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
            .select(&["time", "open", "high", "low", "close", "vol"])
            .unwrap()
            .to_ndarray::<Float64Type>()
            .unwrap();

        let py_array2: &PyArray2<f64> = array.into_pyarray(py);

        return py_array2.to_owned();
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
    //--------------------------------------------------------------------------------------------
    // Market (Session) API

    fn run(&mut self, agent: &PyAny, interval_sec: i64) -> PyResult<()> {
        let methods_list = agent.dir();

        let mut want_tick = false;
        if methods_list.contains("on_tick").unwrap() {
            println!("call back tick by {}[sec]", interval_sec);
            want_tick = true;
        }

        let mut want_update = false;
        if methods_list.contains("on_update").unwrap() {
            println!("call back by update");
            want_update = true;
        }

        let mut want_event = false;
        if methods_list.contains("on_event").unwrap() {
            println!("call back by all log events");
            want_event = true;
        }

        if (want_tick == false) && (want_update == false) {
            println!("on_tick() OR on_update() must be implementd")
        }

        // warm up run: １０分
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

        let time_s = &df["time"];
        let price_s = &df["price"];
        let size_s = &df["size"];
        let bs_s = &df["bs"];

        let time = &time_s.datetime().unwrap();
        let price = price_s.f64().unwrap();
        let size = size_s.f64().unwrap();
        let bs = bs_s.utf8().unwrap();

        let ohlcv_df = ohlcv_df_from_raw(&df, 0, interval_sec, 0);

        Python::with_gil(|py| {
            let mut py_session = MainSession::from(self);

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
                // TODO: もしTrueを返したら、つぎのtickを即時呼び出し
                if want_event {
                    let bs = OrderType::from_str(bs).to_long_string();

                    let args = (time, bs, price, size);
                    agent.call_method1("on_event", args)?;
                }

                // TODO: May skip wam up time
                // 最初のインターバル毎の時刻で呼び出し。
                let current_time_ms = py_session.get_timestamp_ms();
                let clock_time = (time / 1_000 / interval_sec) * 1_000 * interval_sec;

                if want_tick && (current_time_ms < clock_time) && warm_up_ok_flag {
                    if self._debug_loop_count != 0 {
                        self._debug_loop_count -= 1;
                        if self._debug_loop_count == 0 {
                            return Ok(());
                        }
                    }

                    let copy_session = CopySession::from(&py_session, &ohlcv_df, interval_sec);
                    let py_session2 = Py::new(py, copy_session)?;

                    let result = agent.call_method1("on_tick", (clock_time, py_session2))?;

                    match result.extract::<PyOrder>() {
                        Ok(order) => {
                            &py_session.make_order(
                                order.side,
                                order.price,
                                order.size,
                                order.duration_ms,
                            );
                            println!("Make ORDER {:?}", order);
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
                }

                let results = py_session.session.main_exec_event(
                    time,
                    OrderType::from_str(bs),
                    price,
                    size,
                );

                //call back event update
                if want_update && results.len() != 0 {
                    for r in results {
                        self.order_history.push(r.clone());                        
                        let result = PyOrderResult::from(r);

                        let py_result = Py::new(py, result)?;
                        let obj = py_result.to_object(py);

                        let args = PyTuple::new(py, [&obj]);
                        agent.call_method1("on_update", args)?;
                    }


                }
            }
            // self.order_history = py_session.session.order_history;

            // println!("--");


            //println!("{:?}", self.order_history);

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

    fn log_ohlcv(&mut self, current_time_ms: i64, width_sec: i64, count: i64) -> Py<PyArray2<f64>> {
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let array = self.market._ohlcv(current_time_ms, width_sec, count);

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
}

#[pyfunction]
fn sim_run(market: &PyAny, agent: &PyAny, interval_sec: i64) -> PyResult<()> {
    let methods_list = agent.dir();

    let mut want_tick = false;
    if methods_list.contains("on_tick").unwrap() {
        println!("call back tick by {}[sec]", interval_sec);
        want_tick = true;
    }

    let mut want_update = false;
    if methods_list.contains("on_update").unwrap() {
        println!("call back by update");
        want_update = true;
    }

    let mut want_event = false;
    if methods_list.contains("on_event").unwrap() {
        println!("call back by all log events");
        want_event = true;
    }

    if (want_tick == false) && (want_update == false) {
        println!("on_tick() OR on_update() must be implementd")
    }

    Python::with_gil(|py| {
        /*
            let py_bb = Py::new(py, market)?;
            let obj = py_result.to_object(py);

        */
        let mut bb_cell: &PyCell<DummyBb> = market.downcast()?;
        let mut bb = &mut bb_cell.borrow_mut();

        let mut start_time_ms = bb.market.start_time();
        let end_time_ms = bb.market.end_time();

        let df = bb.market.market.select_df(start_time_ms, end_time_ms);

        let time_s = &df["time"];
        let price_s = &df["price"];
        let size_s = &df["size"];
        let bs_s = &df["bs"];

        let time = &time_s.datetime().unwrap();
        let price = price_s.f64().unwrap();
        let size = size_s.f64().unwrap();
        let bs = bs_s.utf8().unwrap();

        let mut session = bb.market.market.get_session();

        for (((t, p), s), b) in time.into_iter().zip(price).zip(size).zip(bs) {
            let time = t.unwrap();
            let price = p.unwrap();
            let size = s.unwrap();
            let bs = b.unwrap();
            log::debug!("{:?} {:?} {:?} {:?}", time, price, size, bs);

            // TODO: May skip wam up time

            // 最初のインターバル毎の時刻で呼び出し。
            let current_time_ms = session.get_timestamp_ms();
            let clock_time = (time / 1_000 / interval_sec) * 1_000 * interval_sec;

            if want_tick && (current_time_ms < clock_time) {
                // let market = Py::new(py, bb)?.to_object(py);

                //                let args = PyTuple::new(py, [bb, clock_time]);

                let result = agent.call_method1("on_tick", (market, clock_time))?;
                // call back tick
            }

            // すべてのイベントを呼び出し
            // TODO: もしTrueを返したら、つぎのtickを即時呼び出し
            if want_event {
                let bs = OrderType::from_str(bs).to_long_string();
                //let on_event: &PyAny = agent.get_item("on_event")?.into();
                let args = (time, bs, price, size);
                agent.call_method1("on_event", args)?;
                //on_event.call_method1(py, args)?;

                // agent.call_method(name, args, kwargs)
            }

            let results = session.main_exec_event(time, OrderType::from_str(bs), price, size);

            //call back event update
            if want_update && results.len() != 0 {
                for r in results {
                    let result = PyOrderResult::from(r);

                    let py_result = Py::new(py, result)?;
                    let obj = py_result.to_object(py);

                    let args = PyTuple::new(py, [&obj]);
                    agent.call_method1("on_update", args)?;
                }
            }
        }

        Ok(())
    })
}

use crate::exchange::order::OrderResult;
use crate::exchange::order::OrderStatus;

#[pyclass]
pub struct PyOrderResult {
    #[pyo3(get, set)]
    pub timestamp: i64,
    #[pyo3(get, set)]
    pub order_id: String,
    #[pyo3(get, set)]
    pub order_sub_id: String, // 分割された場合に利用
    #[pyo3(get, set)]
    pub order_type: String,
    #[pyo3(get, set)]
    pub post_only: bool,
    #[pyo3(get, set)]
    pub create_time: i64,
    #[pyo3(get, set)]
    pub status: String,
    #[pyo3(get, set)]
    pub open_price: f64,
    #[pyo3(get, set)]
    pub close_price: f64,
    #[pyo3(get, set)]
    pub size: f64, // in usd
    #[pyo3(get, set)]
    pub volume: f64, //in BTC
    #[pyo3(get, set)]
    pub profit: f64,
    #[pyo3(get, set)]
    pub fee: f64,
    #[pyo3(get, set)]
    pub total_profit: f64,
}

impl PyOrderResult {
    fn from(result: &OrderResult) -> Self {
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
}

#[pymethods]

impl PyOrder {
    #[new]
    fn new(side: String, price: f64, size: f64, valid_sec: i64) -> Self {
        return PyOrder {
            side: OrderType::from_str(side.as_str()),
            price: price,
            size: size,
            duration_ms: valid_sec * 1_000,
        };
    }

    fn __str__(&self) -> PyResult<String> {
        return Ok(format!(
            "side: {}, price: {}, size: {}, duration_ms: {}",
            self.side.to_long_string(),
            self.price,
            self.size,
            self.duration_ms
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

/*
use arrow::{array::ArrayRef, ffi};
use polars::prelude::*;
use polars_arrow::export::arrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{ffi::Py_uintptr_t, PyAny, PyObject, PyResult};


pub(crate) fn to_py_array(py: Python, pyarrow: &PyModule, array: ArrayRef) -> PyResult<PyObject> {
    let array_ptr = Box::new(ffi::ArrowArray::empty());
    let schema_ptr = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = Box::into_raw(array_ptr);
    let schema_ptr = Box::into_raw(schema_ptr);

    unsafe {
        ffi::export_field_to_c(
            &ArrowField::new("", array.data_type().clone(), true),
            schema_ptr,
        );
        ffi::export_array_to_c(array, array_ptr);
    };

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        Box::from_raw(array_ptr);
        Box::from_raw(schema_ptr);
    };

    Ok(array.to_object(py))
}

pub fn rust_series_to_py_series(series: &Series) -> PyResult<PyObject> {
    // ensure we have a single chunk
    let series = series.rechunk();
    let array = series.to_arrow(0);

    // acquire the gil
    let gil = Python::acquire_gil();
    let py = gil.python();
    // import pyarrow
    let pyarrow = py.import("pyarrow")?;

    // pyarrow array
    let pyarrow_array = to_py_array(py, pyarrow, array)?;

    // import polars
    let polars = py.import("polars")?;
    let out = polars.call_method1("from_arrow", (pyarrow_array,))?;
    Ok(out.to_object(py))
}


*/
