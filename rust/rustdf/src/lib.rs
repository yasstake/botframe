
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
//       https://github.com/datafusion-contrib/datafusion-python
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
    Ok(())
}

#[test]
fn test_all() {
    
}