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

// use pyo3::PyTryInto::try_into;

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
use chrono::{DateTime, Utc};

use numpy::IntoPyArray;
use numpy::PyArray2;

#[pyclass(module = "rbot")]
struct DummyBb {
    market: Bb,
    balance: f32,
    now: DateTime<Utc>,
}

//use polars::prelude::DataFrame;

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyDataFrame {
    pub df: DataFrame,
}

impl PyDataFrame {
    pub(crate) fn new(df: DataFrame) -> Self {
        PyDataFrame { df }
    }

    pub(crate) fn try_into(&self) -> PyResult<&DataFrame> {
        Ok(&self.df)
    }
}

#[pymethods]
impl DummyBb {
    #[new]
    fn new() -> Self {
        println!("DummyBb is created{}", "");
        return DummyBb {
            market: Bb::new(),
            balance: 0.0,
            now: Utc::now(),
        };
    }

    // now is a time stamp of U64
    // TODO: implement DateTime return value method
    fn timestamp(&self) -> PyResult<i64> {
        let t = self.now.timestamp();

        return Ok(t);
    }

    fn load_data(&mut self, ndays: usize) {
        //TODO:
        println!("Loading log file for past ndays");

        self.market.download_exec_log_ndays(ndays as i32);
    }

    fn make_order(&self, side: &str, price: f32, volume: f32, duration: i32) -> PyResult<String> {
        // TODO:
        println!(
            "make order of side{} price={} vol={} duration={}",
            side, price, volume, duration
        );

        return Ok("order id string(maybe guid)".to_string());
    }

    #[getter]
    fn get_history(&mut self) -> PyResult<PyDataFrame> {
        // TODO: retum must by numpy object
        let data_frame = self.market.df();

        let py_frame = PyDataFrame::new(data_frame);

        return Ok(py_frame);
    }

    fn ohlcv(&self, width_sec: i32) -> PyResult<String> {
        // TODO: retum must by numpy object

        return Ok("numpy object ".to_string());
    }

    fn _ohlcv(&self, _width: i32, _count: i32) -> Py<PyArray2<f64>> {
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();
        let py_array2: &PyArray2<f64> = array![[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]].into_pyarray(py);
        //  assert_eq!(py_array.as_slice().unwrap(), &[1, 2, 3]);
        // &py_array2.to_owned();
        
        return py_array2.to_owned();
    }

    #[getter]
    fn get_balance(&self) -> PyResult<f32> {
        // TODO: retum must by numpy object

        return Ok(self.balance);
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

#[test]
fn test_python_call() {
    Python::with_gil(|py| {
        let polars = py.import("polars").unwrap();

        let df_any = polars.call_method0("DataFrame").unwrap();
        // let df_any = polars.call_method1("DataFrame", ([1,1])).unwrap();

        println!("{}", df_any);
        println!("{}", df_any.get_type().name().unwrap());

        let r = df_any.call_method0("max").unwrap();
        println!("max={}", r);

        let r = df_any.get_type().call_method0("max").unwrap();
        println!("{}", r);

        let c: PyDataFrame = df_any.extract().unwrap();

        let b = df_any.get_item("").unwrap();
        println!("{}", b);

        let b = df_any.getattr("df").unwrap();
        println!("{}", b);

        let a = df_any.get_item("df").unwrap();
        println!("{}", a);

        let df_ptr = df_any.into_ptr();

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
    })
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
