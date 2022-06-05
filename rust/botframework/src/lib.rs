use pyo3::prelude::*;

#[macro_use]
extern crate anyhow;
extern crate directories;

extern crate time;

pub mod bb;
pub mod exchange;



/*
Python からよびださされるモジュール

想定利用方法イメージ：

    import rbot

    exchange = rbot.DummyBb()

    exchange.load_data(ndays)

    // 取引所想定パラメータの設定 初めは不要。
    exchange.exec_delay         //　執行時間ディレイ in sec
    

    create agent
        register call back
            tick()
            time(day, hour, min, sec)

        register call back
            order

        exchange.make_order(side, price, volume, duration)

        exchange.history
        exchagne.ohlcv
        exchange.balance
        exchange.position

    exchange.set_balance

    // get ready?
    exchange.run(agent)         // may be async method?
    exchange.get_result()





class Agent:
    on_tick():
    on_time(day, hour, min, sec):
    on_order(status, side, price, volume, id)
*/

use crate::bb::market::Bb;
use chrono::{Datelike, Utc, Duration, DateTime};



#[pyclass(module = "rbot")]
struct DummyBb {
    market: Bb,
    balance: f32,
    now: DateTime<Utc>
}

#[pymethods]
impl DummyBb {
    #[new]
    fn new() -> Self {
        println!("DummyBb is created{}", "");
        return DummyBb{
            market: Bb::new(),
            balance: 0.0,
            now: DateTime::now()
        };
    }

    fn now(&self) -> PyResult<String> {
        return Ok(self.now.to_str());
    }

    fn load_data(&self, ndays: usize) {
        //TODO:
        println!("Loading log file for past ndays");

        self.market.download_exec_log_ndays(ndays as i32);
    }

    fn make_order(&self, side: &str, price: f32, volume: f32, duration: i32) -> PyResult<String> {
        // TODO:
        println!("make order of side{} price={} vol={} duration={}",
                side, price, volume, duration);

        return Ok("order id string(maybe guid)".to_string());
    }

    #[getter]
    fn get_history(&self) -> PyResult<String> {
        // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }

    fn ohlcv(&self, width_sec: i32) -> PyResult<String> {
        // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }

    #[getter]
    fn get_balance(&self) -> PyResult<f32> {
        // TODO: retum must by numpy object

        return Ok(self.balance)
    }

    #[setter]
    fn set_balance(&mut self, balance: f32) {
        self.balance = balance;
    }

    #[getter]
    fn get_position(&self) -> PyResult<String> {
        // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }

    fn run(&self) -> PyResult<String> {
        // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }

    #[getter]
    fn reslut(&self) -> PyResult<String> {
    // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }
}



/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}


/// A Python module implemented in Rust.
#[pymodule]
fn rbot(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<DummyBb>()?;

    Ok(())
}
