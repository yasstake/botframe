use crate::exchange::Market;
use crate::exchange::Trade;

use crate::bb::log::load_log_file;

#[tokio::main]
#[test]
async fn make_market() {
    // make instance of market
    let mut market = Market::new();

    fn nothing_callback(m: &mut Market, t: Trade) {
        m.add_trade(t);
        // println!("{} {} {} {}",t.time_ns, t.bs, t.price, t.size)
    }

    // then load log
    load_log_file(2022, 6, 1, nothing_callback, &mut market).await;
    load_log_file(2022, 6, 2, nothing_callback, &mut market).await;
    load_log_file(2022, 6, 3, nothing_callback, &mut market).await;

    // insert log to market
}
