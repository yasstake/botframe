


use chrono::{Utc, DateTime, NaiveDateTime};
use pyo3::prelude::*;

// Timestamp scale for system wide.(Nano Sec is default)
pub type MicroSec = i64;

pub fn to_seconds(microsecond: MicroSec) -> f64 {
    return (microsecond as f64) / 1_000_000.0;
}


#[pyfunction]
pub fn time_string(t: MicroSec) -> String {
    let sec = t / 1_000_000;
    let nano = ((t % 1_000_000) * 1_000) as u32;
    let datetime = NaiveDateTime::from_timestamp(sec, nano);

    // return datetime.format("%Y-%m-%d-%H:%M:%S.").to_string() + format!("{:06}", nano).as_str();
    return datetime.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
}

#[pyfunction]
pub fn parse_time(t: &str) -> MicroSec {
    let datetime = DateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.6f%z");

    return datetime.unwrap().timestamp_micros();
}

#[pyfunction]
pub fn DAYS(days: i64) -> MicroSec {
    return (24 * 60 * 60 * 1_000_000 * days) as MicroSec;
}

#[pyfunction]
pub fn HHMM(hh:i64, mm: i64) -> MicroSec {
    return   (( (hh * 60 * 60) + (mm * 60)) * 1_000_000) as MicroSec;
}

///
/// 現在時刻を返す(Microsec)
/// ```
/// println!("{:?}", NOW());
/// ```
#[pyfunction]
pub fn NOW() -> MicroSec {
    return Utc::now().timestamp_micros();
}

#[cfg(test)]
mod time_test {
    use super::*;
    #[test]
    fn test_to_str() {
        assert_eq!(time_string(0), "1970-01-01T00:00:00.000000");
        assert_eq!(time_string(1), "1970-01-01T00:00:00.000001");
        assert_eq!(time_string(1_000_001), "1970-01-01T00:00:01.000001");
    }

    // https://rust-lang-nursery.github.io/rust-cookbook/datetime/parse.html
    #[test]
    fn test_parse_time() {
        const TIME1: &str = "2022-10-22T14:22:43.407735+00:00";
        let r = parse_time(TIME1);
        println!("{:?}", r);

        assert_eq!(1_000_001, parse_time("1970-01-01T00:00:01.000001+00:00"));
    }

    #[test]
    fn test_days() {
        assert_eq!(DAYS(1), parse_time("1970-01-02T00:00:00.000000+00:00"));
        assert_eq!(HHMM(1, 1), parse_time("1970-01-01T01:01:00.000000+00:00"));
    }

    #[test]
    fn test_print_now() {
        let now = NOW();
        println!("{:?} {:?}", now, time_string(now));
    }
}
