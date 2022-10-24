use std::thread::sleep;
use std::time::Duration;
use crate::common::time::{DAYS, HHMM, MicroSec, NOW, to_seconds, time_string};
use crate::common::order::Trade;
use crate::exchange::ftx::message::{FtxTrade, FtxTradeMessage};
use log;

const FTX_REST_ENDPOINT: &str = "https://ftx.com/api";

const BTCMARKET: &str = "BTC-PERP";


async fn download_trade_ndays(market_name: &str, ndays: i64, callback: fn(&Trade)) {
    log::debug!("download_trade_ndays {}", ndays);
    let start_time = NOW() - DAYS(ndays);
    let mut end_time = NOW() + HHMM(0, 10);   // 10分後を指定し最新を取得。


    loop {
        log::debug!("download trade to: {}", time_string(end_time));

        let trades = download_trade(market_name, start_time, end_time).await;
        let trade_len = trades.len();
        log::debug!("{}", trade_len);

        for t in trades {
            callback(&t);
            if t.time < end_time {
                end_time = t.time;
            }
        }

        if trade_len  <= 100 || start_time == end_time {
            break;
        }

        sleep(Duration::from_millis(10));
    }
}


// TODO: エラーハンドリング（JSONエラー/503エラーの場合、現在はPanicしてしまう）
//
async fn download_trade(market_name: &str, from_microsec: MicroSec, to_microsec: MicroSec ) -> Vec<Trade> {
    let start_sec = to_seconds(from_microsec) as i64;
    let end_sec = to_seconds(to_microsec) as i64;

    let url = format!("{}/markets/{}/trades?start_time={}&end_time={}", FTX_REST_ENDPOINT, market_name, start_sec, end_sec);
    log::debug!("{}", url);

    let response = reqwest::get(url).await;

    return match response {
        Ok(response) => {
            match response.text().await {
                Ok(res) => {
                    match serde_json::from_str::<FtxTradeMessage>(res.as_str()) {
                        Ok(mut message) => message.get_trades(),
                        Err(e) => {
                            log::warn!("log history format(json) error = {}/{}", e, res);
                            vec![]
                        }
                    }
                }
                Err(e) => {
                    log::warn!("log history format(json) error = {}", e);
                    vec![]
                }
            }
        },
        Err(e) => {
            log::warn!("download history error = {:?}", e);
            vec![]
        }
    }
}


#[cfg(test)]
mod test_ftx_client {
    use super::*;
    use std::io::Cursor;
    use crate::common::init_log;
    use crate::common::time::{DAYS, NOW};
    use crate::exchange::ftx::message::FtxTradeMessage;
    use crate::time_string;

    #[tokio::test]
    async fn test_download_trade() {
        init_log();
        let to_time = NOW();
        let from_time = to_time - DAYS(10);

        let trades = download_trade(BTCMARKET,from_time, to_time).await;

        log::debug!("FROM: {:?} {:?}", trades[0].time, time_string(trades[0].time));
        log::debug!("TO:   {:?} {:?}", trades[trades.len() -1].time, time_string(trades[trades.len() -1].time));
        log::debug!("Trade len = {:?}", trades.len());
    }



    #[tokio::test]
    async fn test_download_ndays () {
        init_log();

        log::debug!("begin test");
        let callback = |t:&Trade| {/*println!("{:?}", t);*/};

        download_trade_ndays(BTCMARKET, 1, callback).await;
        log::debug!("end test");
    }
}
