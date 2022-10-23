

// https://docs.rs/serde_json/latest/serde_json/
// serde_json = {version = "1.0.87"}
// serde_derive = {version = "1.0.147}

use chrono::DateTime;
use serde_derive::{Deserialize, Serialize};
use crate::common::order::Trade;
use crate::OrderSide;

#[derive(Debug, Serialize, Deserialize)]
pub struct FtxTradeMessage {
    success: bool,
    pub(crate) result: Vec<FtxTrade>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FtxTrade {
    id: i64,   // "id":5196537114
    price: f64,   // "price":19226.0
    size: f64,    // "size":0.0147
    side: String, // "side":"sell"
    liquidation: bool, // "liquidation":false
    time: String       // "time":"2022-10-22T14:22:43.407735+00:00"
}

impl FtxTrade {
    pub fn to_trade(&self) -> Trade {
        return Trade {
            time: 0,
            price: self.price,
            size: self.size,
            bs: OrderSide::from_str(&self.side),
            liquid: self.liquidation,
            id: self.id.to_string()
        }
    }
}

#[cfg(test)]
mod test_ftx_message {
    use crate::common::time::parse_time;
    use crate::exchange::ftx::message::FtxTradeMessage;

    const MESSAGE: &str = r#"
     {"success":true,
            "result":[
               {"id":5196537114,"price":19226.0,"size":0.0147,"side":"sell","liquidation":false,"time":"2022-10-22T14:22:43.407735+00:00"},
               {"id":5196537109,"price":19226.0,"size":0.0823,"side":"sell","liquidation":false,"time":"2022-10-22T14:22:43.325945+00:00"},
               {"id":5196537075,"price":19226.0,"size":0.0992,"side":"sell","liquidation":false,"time":"2022-10-22T14:22:42.465804+00:00"}
               ]
     }
    "#;


    #[test]
    fn test_ftx_trade_message() {
        let message: FtxTradeMessage = serde_json::from_str(MESSAGE).unwrap();

        println!("{:?}", message);
        assert_eq!(message.success, true);
        assert_eq!(message.result.len(), 3);
        println!("{:?}", parse_time(message.result[0].time.as_str() ));
    }

}






