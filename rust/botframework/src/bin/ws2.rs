use failure::Error;
use serde_derive::{Deserialize, Serialize};

const json_string: &str = r#"
{"topic":"ParseTradeMessage.BTCUSD",
 "data":[
       {"trade_time_ms":1619398389868,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":2000,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"8241a632-9f07-5fa0-a63d-06cefd570d75","cross_seq":6169452432},
       {"trade_time_ms":1619398389947,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":200,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"ff87be41-8014-5a33-b4b1-3252a6422a41","cross_seq":6169452432}]}
"#;

/*
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op")]
enum Operation {
    Add {
        #[serde(flatten)]
        task_type: TaskType,
        #[serde(flatten)]
        task: Task,
    },
    Take(TaskType),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum TaskType {
    Technical,
    Miscellaneous,
}
*/

#[test]
fn test_bybit_message() {
    const M: &str = r#"
    {"topic":"ParseTradeMessage.BTCUSD",
     "data":[
           {"trade_time_ms":1619398389868,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":2000,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"8241a632-9f07-5fa0-a63d-06cefd570d75","cross_seq":6169452432},
           {"trade_time_ms":1619398389947,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":200,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"ff87be41-8014-5a33-b4b1-3252a6422a41","cross_seq":6169452432}]}
    "#;

    let ws_message: BybitWsMessage = serde_json::from_str(M).unwrap();

    match ws_message {
        BybitWsMessage::TradeMessage { data } => println!("{}", data[0].timestamp),
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "topic")]
enum BybitWsMessage {
    #[serde(rename = "ParseTradeMessage.BTCUSD")]
    TradeMessage { data: Vec<Trade> },
}

#[test]
fn test_parse_trade_message() {
    const m: &str = r#"[{"trade_time_ms":1619398389947,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":200,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"ff87be41-8014-5a33-b4b1-3252a6422a41","cross_seq":6169452432}]"#;

    let message: Vec<Trade> = serde_json::from_str(&m).unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
struct Trade {
    trade_time_ms: u64,     // {"trade_time_ms":1619398389947,
    timestamp: String,      // "timestamp":"2021-04-26T00:53:09.000Z",
    symbol: String,         // "symbol":"BTCUSD",
    side: String,           // "side":"Sell",
    size: u64,              // "size":200,
    price: f64,             // "price":50703.5,
    tick_direction: String, // "tick_direction":"ZeroMinusTick",
    trade_id: String,       // "trade_id":"ff87be41-8014-5a33-b4b1-3252a6422a41",
    cross_seq: u64,         // "cross_seq":6169452432}
}

fn main() {}

/*

#[test]
pub fn run() -> Result<(), Error> {
    println!("\n======== Extract fields to structs ========\n");

    let string = r#"[
        {"op": "Add",  tasks: ["type": "Miscellaneous", "name": "clean up room"]},
        {"op": "Add",  tasks: ["type": "Technical",     "name": "fix Wi-Fi"]},
        {"op": "Take", tasks: ["type": "Technical"]},
        {"op": "Take", tasks: ["type": "Miscellaneous"]}
    ]"#;

    let operations: Vec<Operation> = serde_json::from_str(&string)?;

    println!("String: {}", string);
    println!("Decoded: {:?}", operations);

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op")]
enum Operation {
    Add {
        #[serde(flatten)]
        task_type: TaskType,
        #[serde(flatten)]
        task: Task,
    },
    Take(TaskType),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum TaskType {
    Technical,
    Miscellaneous,
}

#[derive(Debug, Serialize, Deserialize)]
struct Task {
    name: String,
}

*/
