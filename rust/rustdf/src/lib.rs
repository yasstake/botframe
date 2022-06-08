
use polars::export::arrow::array::new_empty_array;
use pyo3::prelude::*;

pub mod dataframe;

/*
// Rust と Pythonのデータ連携
//　ためしてみること）
//  1) Numpy連携
//       https://qiita.com/kngwyu/items/5e5fe2e2fbf19ce3fe38
         ここで紹介されている IntoPyArray
         https://docs.rs/numpy/0.7.0/numpy/convert/trait.IntoPyArray.html
         これはVec!をゼロコピーでArrayにするらしい。
//　2) Series連携
        https://github.com/pola-rs/polars/tree/master/examples/python_rust_compiled_function


//　3) DataFrame連携
         ここを利用してうまくいかないか？
          https://github.com/datafusion-contrib/datafusion-python/blob/main/src/dataframe.rs
*/

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn rustdf(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<Df>()?;
    Ok(())
}







use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::prelude::Series;


#[pyclass(module="rustdf")]
struct Df {
    price: Vec<f32>,
    time: Vec<i64>, 

    trade_history: DataFrame
}

#[pymethods]
impl Df {
    #[new]
    fn init() -> Df {
        let mut t: Vec<i64> = vec![];
        for i in 0..3000000 {
            t.push(i);
        }

        let mut p: Vec<f32> = Vec::new();
        for i in 0..3000000 {
            p.push((i as f32)/10.0);
        }
   
        let mut ts = Series::new("time_ns", &t);
        let mut ps = Series::new("price", &p);
    
        let mut trade_history = DataFrame::new(vec![ts, ps]).unwrap();

        return  Df {
            price: p,
            time: t,
            trade_history: trade_history
        };
    }

    fn len(&self) -> usize {
        return self.trade_history.shape().0;
    }

    fn rowdf(&self) -> PyResult<DataFrame> {
        return Ok(self.trade_history);
    }

}



#[test]
fn test_all() {
    let df = Df::init();

    let np = df;
}

use arrow::{array::ArrayRef, ffi};

use arrow::ffi::ArrowSchema;
use ArrowArray;

use polars::prelude::*;
use polars_arrow::export::arrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{ffi::Py_uintptr_t, PyAny, PyObject, PyResult};


// Arrow array to Python.
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
