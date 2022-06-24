

use std::fmt::Binary;

use chrono::NaiveDateTime;
use chrono::NaiveDate;

use pyo3::pyfunction;

/// YYYYまたはYY mm ddをいれるとmsでのシリアル値を返す
#[pyfunction]
pub fn YYYYMMDD(yy: i64, mm: i64, dd: i64) -> i64 {
    let date = NaiveDate::from_ymd(yy as i32, mm as u32, dd as u32);
    let datetime: NaiveDateTime  = date.and_hms(0, 0, 0);

    return datetime.timestamp_millis();
}

/// 時刻HH:MMのシリアル値を返す。
#[pyfunction]
pub fn HHMM(hh: i64, mm:i64) -> i64{
    let date = NaiveDate::from_ymd(0, 0, 0);
    let datetime: NaiveDateTime  = date.and_hms(hh as u32, mm as u32, 0);

    return datetime.timestamp_millis();
}

/// 時刻のシリアル値の時刻をPrintableな文字列で返す（UTC)
#[pyfunction]
pub fn PrintTime(time_ms: i64) -> String {
    let datetime = NaiveDateTime::from_timestamp(time_ms/1_000, ((time_ms % 1_000)*1_000_000) as u32);
    let ms = time_ms % 1_000;
    
    return datetime.format("%Y-%m-%d-%H:%M:%S.").to_string() + format!("{:03}", ms).as_str();
}



#[cfg(test)]
mod UtilTest{
    use crate::pyutil::*;

    #[test]
    fn testYYMMDD() {
        assert_eq!(YYYYMMDD(1970,1,1), 0);
        assert_eq!(YYYYMMDD(1970,1,2), 24 * 60 * 60 * 1_000);        
    }

    #[test]
    fn testHHSS() {
        assert_eq!(HHMM(0,0), 0);
        assert_eq!(HHMM(0,1), 1 * 60 * 1_000);        
        assert_eq!(HHMM(1,0), 60 * 60 * 1_000);                
    }

    #[test]
    fn testPrintDate(){
        assert_eq!(PrintTime(0), "1970-01-01-00:00:00.000");
        assert_eq!(PrintTime(1), "1970-01-01-00:00:00.001");        
        assert_eq!(PrintTime(1_000 * 60), "1970-01-01-00:01:00.000");                
    }


}