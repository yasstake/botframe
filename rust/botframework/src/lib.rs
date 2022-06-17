use std::borrow::Borrow;
use std::borrow::BorrowMut;

use exchange::MarketInfo;
use pyo3::ffi::PyTuple_GetSlice;
use pyo3::prelude::*;
// use pyo3::types::PyDateTime;
// use pyo3::types::PyInt;

//use crate::polars::PyDataFrame;
use ::polars::prelude::DataFrame;
// use tungstenite::protocol::frame::coding::Data;

#[macro_use]
extern crate anyhow;
extern crate directories;
extern crate time;

pub mod bb;
pub mod exchange;

use polars_lazy::prelude::*;
use polars::prelude::Series;

use chrono::NaiveDateTime;


// use pyo3::PyTryInto::try_into;

/*
Python からよびださされるモジュール

想定利用方法イメージ：

--- Agent

class Agent:
    def on_tick(self, session, time_ms)
    def on_exec(self, session, time_ms, side, price, volume) // 後で実装


---- Session API
    session.run(agent, from, time_s)

    session.timestamp_ms
    session.make_order(side, price, volume, duration)

    // session.history あとで実装
    session.ohlcv
    session.balance
    session.indicator(key, value)
    // session.position あとで実装。まずは約定は重ねない。

    session.result


---- Main
    import rbot

    exchange = rbot.DummyBb()
    exchange.load_data(ndays)

    agent = Agent()

    result = exchange.run(agent 10)

    print(result)

*/

use crate::bb::market::Bb;
use chrono::{DateTime, Utc};

use numpy::IntoPyArray;
use numpy::PyArray2;


#[pyclass(module = "rbot")]
struct DummyBb {
    market: Bb,
}

use async_std::task;

#[pymethods]
impl DummyBb{
    #[new]
    fn new() -> Self {
        return DummyBb {
            market: Bb::new()
        };
    }

    /*
    fn new_session(&mut self) -> PyResult<Session> {
        let session = Session{
            balance: 0.0,
            market: self.market.market.borrow(),
            timestamp_ms:0
        };

        return Ok(session);
    }
    */

    // 過去ndays分のログをダウンロードしてロードする。
    fn load_data(&mut self, ndays: usize) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(
                self.market.download_exec_log_ndays(ndays as i32)
            );
    }

    /*
    fn create_session(&self) -> PyResult<Session> {
        return Ok(Session::new());
    }
    */

    fn ohlcv(&mut self, current_time_ms: i64, width_sec: i64, count: i64) -> Py<PyArray2<f64>> {
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let array = self.market._ohlcv(current_time_ms, width_sec, count);

        println!("s:{}", current_time_ms);
        println!("{:?}", array);

        let py_array2: &PyArray2<f64> = array.into_pyarray(py);
        
        return py_array2.to_owned();
    }

    #[getter]
    fn get_start_time(&self) -> PyResult<i64> {
        return Ok(self.market.start_time());
    }

    #[getter]
    fn get_end_time(&self) -> PyResult<i64> {
        return Ok(self.market.end_time());
    }
}

use crate::exchange::session::Session;


use crate::exchange::Market;



#[pyclass(module = "rbot")]
struct PySession {

}




/*
#[pyclass(module = "rbot")]
struct Session {
    timestamp_ms: i64,
    balance: f64,
    market: &'py Market
}

fn find_agent_method(pyObj: PyAny, method_name: &str) -> bool{
   let methods = pyObj.dir();

    match methods.contains(method_name) {
        Ok(r) => {
            return r;
        }
        Err(err)       => {
            println!("{:?}", err);
            return false;
        }
    };
}

#[pymethods]
impl Session {
    // now is a time stamp of U64
    // TODO: implement DateTime return value method
    fn timestamp(&self) -> PyResult<i64> {
       return Ok(self.timestamp_ms);
    }

    fn make_order(&self, side: &str, price: f32, volume: f32, duration: i32) -> PyResult<String> {
        println!(
            "make order of side{} price={} vol={} duration={}",
            side, price, volume, duration
        );

        return Ok("order id string(maybe guid)".to_string());
    }

    /*
    #[getter]
    fn get_history(&mut self) -> PyResult<PyDataFrame> {
        // TODO: retum must by numpy object
        let data_frame = self.market.df();

        let py_frame = PyDataFrame::new(data_frame);

        return Ok(py_frame);
    }
    */

    #[getter]
    fn get_balance(&self) -> PyResult<f64> {
        return Ok(self.balance);
    }

    #[setter]
    fn set_balance(&mut self, balance: f64) {
        self.balance = balance;
    }

    /*
    #[getter]
    fn get_position(&self) -> PyResult<String> {
        // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }
    */

    fn run(&self, m: &PyModule) -> PyResult<String> {
        // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }

    #[getter]
    fn reslut(&self) -> PyResult<String> {
        // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }
}
*/


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
    m.add_class::<PyTest>()?;

    Ok(())
}

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

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
struct PyTest {
    moji: String,
}

#[pymethods]
impl PyTest {
    #[new]
    pub fn new() -> Self {
        return PyTest {
            moji: "abdedfg".to_string(),
        };
    }
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

#[test]
fn test_downcast() {
    Python::with_gil(|py| {
        let rbot = py.import("rbot").unwrap();

        let df_any = rbot.call_method0("PyTest").unwrap();
        // let df_any = polars.call_method1("DataFrame", ([1,1])).unwrap();

        println!("{}", df_any);
        println!("{}", df_any.get_type().name().unwrap());

        let item: PyTest = df_any.extract().unwrap();

        println!("{}", item.moji);

        // let t: PyTest = df_any.extract().unwrap();
    });
}

use pyo3::types::PyTuple;


#[test]
fn test_market_call() {
    let code: &str = 
r#"class Agent:
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

        let shape_any  = df_any.getattr("shape");
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
    }
    );



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
