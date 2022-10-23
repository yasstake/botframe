mod message;

use reqwest;
use crate::common::time::{MicroSec, to_seconds};
use crate::exchange::ftx::message::{FtxTrade, FtxTradeMessage};

const FTX_API_ENDPOINT: &str = "https:://ftx.com/api/";


// TODO: マーケット種別に対応
fn download_trade(from_microsec: MicroSec, to_microsec: MicroSec ) -> Vec<FtxTrade> {
    let from_sec = to_seconds(from_microsec) as i64;
    let to_sec = to_seconds(to_microsec) as i64;

    const FTX_API_ENDPOINT_HISTORY: &str = "https://ftx.com/api/markets/BTC-PERP/trades";
    let client = reqwest::Client::new();
    let response = client.get(FTX_API_ENDPOINT_HISTORY)
        .query(&[("from", from_sec.to_string()), ("to", to_sec.to_string())]).send().await.unwrap();

    if response.status().is_success() {
        let res = response.text().await.unwrap();
        let message: FtxTradeMessage = serde_json::from_str(res.as_str()).unwrap();

        return message.result;
    }

    vec![]
}




#[cfg(test)]
mod test_ftx_client {
    use std::io::Cursor;
    use crate::exchange::ftx::FTX_API_ENDPOINT;
    use crate::exchange::ftx::message::FtxTradeMessage;
    use crate::time_string;

//     #[derive[Deserialize]]



    #[tokio::test]
    async fn test_download_log(){
        println!("{:?}", time_string(1559881711 * 1_000 * 1_000));

        const FTX_API_ENDPOINT_HISTORY: &str = "https://ftx.com/api/markets/BTC-PERP/trades";
        let client = reqwest::Client::new();
        let response = client.get(FTX_API_ENDPOINT_HISTORY)
            .query(&[("from", "1000"), ("to", "1000000000")]).send().await.unwrap();

        if response.status().is_success() {
            let res = response.text().await.unwrap();
            let message: FtxTradeMessage = serde_json::from_str(res.as_str()).unwrap();

            for rec in message.result {
                println!("{:?}", rec);
            }
        }
    }
}
