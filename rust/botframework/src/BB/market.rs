use crate::exchange::Market;
use crate::exchange::Trade;

use crate::bb::log::load_log_file;



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
}


#[tokio::main]
async fn download_log() {
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
