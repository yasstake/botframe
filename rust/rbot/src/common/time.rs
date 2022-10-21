


use chrono::NaiveDateTime;
use pyo3::prelude::*;

// Timestamp scale for system wide.(Nano Sec is default)
pub type NanoSec = i64;

#[pyfunction]
pub fn time_string(t: NanoSec) -> String {
    let sec = t / 1_000_000;
    let nano = (t % 1_000_000) as u32;
    let datetime = NaiveDateTime::from_timestamp(sec, nano);

    return datetime.format("%Y-%m-%d-%H:%M:%S.").to_string() + format!("{:06}", nano).as_str();
}

#[cfg(test)]
mod time_test {
    use super::*;
    #[test]
    fn test_to_str() {
        assert_eq!(time_string(0), "1970-01-01-00:00:00.000000");
        assert_eq!(time_string(1), "1970-01-01-00:00:00.000001");
        assert_eq!(time_string(1_000_001), "1970-01-01-00:00:01.000001");
    }
}
