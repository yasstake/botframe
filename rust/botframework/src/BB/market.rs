use crate::exchange::Market;
use crate::exchange::Trade;

use crate::bb::log::load_log_file;

use chrono::{Datelike, Utc, Duration};



pub struct Bb {
    market: Market
}

impl Bb {
    pub fn new() -> Bb {
        return Bb {
            market: Market::new()
        }                 
    }    

    pub fn download_exec_log(&mut self, yyyy: i32, mm: i32, dd: i32) {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
    
        rt.block_on(
            async {
                fn insert_callback(m: &mut Market, t: Trade) {
                    m.add_trade(t);
                    // println!("{} {} {} {}",t.time_ns, t.bs, t.price, t.size)
                }
                // then load log
                load_log_file(2022, 6, 1, insert_callback, &mut self.market).await;
            }
        )
    }

    // 過去N日分のログをダウンロードし、Maketクラスへロードする。
    pub fn download_exec_log_ndays(&mut self, ndays: i32) {
        let last_day = Utc::now() - Duration::days(1);

        for i in (0..ndays).rev(){
            let log_date = last_day - Duration::days(i as i64);
            let year = log_date.year();
            let month = log_date.month() as i32;
            let day = log_date.day() as i32;

            self.download_exec_log(year, month, day);
            println!("load complete {}/{}/{}", year, month, day);
        }
    }
}



#[tokio::main]
#[test]
async fn test_download_log_for_five_days() {
    // make instance of market

    let bb = Bb::new();

    bb.download_exec_log_ndays(5);
}


#[tokio::main]
#[test]
async fn test_download_log() {
    // make instance of market
    let mut market = Market::new();

    fn insert_callback(m: &mut Market, t: Trade) {
        m.add_trade(t);
        // println!("{} {} {} {}",t.time_ns, t.bs, t.price, t.size)
    }

    // then load log
    load_log_file(2022, 6, 1, insert_callback, &mut market).await;
    load_log_file(2022, 6, 2, insert_callback, &mut market).await;
    load_log_file(2022, 6, 3, insert_callback, &mut market).await;
}

#[tokio::main]
#[test]
pub async fn make_market() {
    // make instance of market
    let mut market = Market::new();

    fn insert_callback(m: &mut Market, t: Trade) {
        m.add_trade(t);
        // println!("{} {} {} {}",t.time_ns, t.bs, t.price, t.size)
    }

    // then load log
    load_log_file(2022, 6, 1, insert_callback, &mut market).await;
    load_log_file(2022, 6, 2, insert_callback, &mut market).await;
    load_log_file(2022, 6, 3, insert_callback, &mut market).await;

    // insert log to market
}
