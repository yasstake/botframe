
// Based on https://raw.githubusercontent.com/haxpor/bybit-shiprekt/master/src/main.rs


use anyhow::Result;

use tungstenite::Message;
use tungstenite::error::Error as TungsError;

use tokio::net::TcpStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;
use url::Url;

use futures_util::stream::StreamExt;
use futures_util::sink::SinkExt;

use std::time::Duration;


#[macro_export]
macro_rules! ret_err {
    ($err:expr) => {{
        return Err($err(None));
    }};

    ($err:expr, $($args:expr),+) => {{
        let str_formed = std::fmt::format(format_args!($($args),+));
        return Err($err(Some(str_formed)));
    }}
}

const BB_WS_ENDPOINT: &str = "wss://stream.bybit.com/realtime";
const BB_SUBSCRIBE_EXEC: &str = r#"{"op":"subscribe","args": ["trade.BTCUSD"]}"#;


#[tokio::main]
#[test]
async fn test_connect_async_to_wss() -> Result<()> {
    let connection = connect_to_wss(BB_WS_ENDPOINT).await?;


    println!("connected ");

    Ok(())
}


async fn connect_to_wss(wss_url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let url = Url::parse(wss_url)?;

    let (stream, _response) = connect_async(url).await?;

    Ok(stream)
}


#[tokio::main]
#[test]
async fn test_ws_loop() -> Result<()> {
    ws_loop(BB_WS_ENDPOINT);

    Ok(())
}


async fn ws_loop(url_str: &str) -> Result<()> {
    // Connect
    println!("ws loop");

    let stream: WebSocketStream<MaybeTlsStream<TcpStream>> = connect_to_wss(url_str).await?;
    let (mut ws_sender, mut ws_receiver) = stream.split();

    println!("connected");

    // Send Opcode

    match ws_sender.send(Message::Text(BB_SUBSCRIBE_EXEC.into())).await {
        Ok(_) => println!("subscribed to liquidation topic"),
        Err(e) => println!("Error"),
    }

    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));

    // receive Loop\\\\\
    'receive: loop {
        tokio::select! {
            msg_item = ws_receiver.next() => {
                match msg_item {
                    Some(msg) => {
                        match msg {
                            Ok(Message::Text(json_str)) => {
                                println!("{}", json_str);
                            },
                            Ok(Message::Ping(msg)) => println!("Received ping message; msg={:#?}", msg),
                            Ok(Message::Pong(msg)) => println!("Received pong message; msg={:#?}", msg),
                            Ok(Message::Binary(bins)) => println!("Received Binbary message, content={}", std::str::from_utf8(&bins).unwrap_or("unknown")),
                            Ok(Message::Frame(frame)) => println!("Received Frame message, content={:?}", frame),
                            Ok(Message::Close(optional_cf)) => match optional_cf {
                               _ => {
                                   println!("(websocket closed)");
                                   break 'receive;                                   
                               }
                            },

                            // from now they are error cases that we need to
                            // reconnect to websocket if occur
                            //
                            // by break into the outer loop
                            Err(TungsError::ConnectionClosed) => {
                                eprintln!("Error: connection closed");
                                break 'receive;
                            },
                            Err(TungsError::AlreadyClosed) => {
                                eprintln!("Error: already closed");
                                break 'receive;                                                                   
                            },
                            Err(TungsError::Io(e)) => {
                                eprintln!("Error: IO; err={}", e);
                                break 'receive;                                                                                                   
                            },
                            Err(TungsError::Tls(e)) => {
                                eprintln!("Error:: Tls error; err={}", e);
                                break 'receive;                                                                                                                                   
                            },
                            Err(TungsError::Capacity(e)) => {
                                type CError = tungstenite::error::CapacityError;
                                match e {
                                    CError::TooManyHeaders => eprintln!("Error: CapacityError, too many headers"),
                                    CError::MessageTooLong{ size, max_size } => eprintln!("Error: CapacityError, message too long with size={}, max_size={}", size, max_size),
                                }
                                break 'receive;                                                                                                                                                                   
                            },
                            Err(TungsError::Protocol(e)) => {
                                eprintln!("Error: Protocol, err={}", e);
                                break 'receive;                                                                                                                                                                   
                            },
                            Err(TungsError::SendQueueFull(e)) => {
                                type PMsg = tungstenite::protocol::Message;
                                match e {
                                    PMsg::Text(text) => eprintln!("Error: SendQueueFull for Text message, content={}", text),
                                    PMsg::Binary(bins) => eprintln!("Error: SendQueueFull for Binary message, content={}", std::str::from_utf8(&bins).unwrap_or("unknown")),
                                    PMsg::Ping(bins) => eprintln!("Error: SendQueueFull for Ping message, content={}", std::str::from_utf8(&bins).unwrap_or("unknown")),
                                    PMsg::Pong(bins) => eprintln!("Error: SendQueueFull for Pong message, content={}", std::str::from_utf8(&bins).unwrap_or("unknown")),
                                    PMsg::Close(close_frame_optional) => {
                                        match close_frame_optional {
                                            Some(close_frame) => eprintln!("Error: SendQueueFull for Close message, content={:?}", close_frame),
                                            None => eprintln!("Error: SendQueueFull for Close message, no close-frame content")
                                        }
                                    },
                                    PMsg::Frame(frame) => eprintln!("Error: SendQueueFull for Frame messasge, content={:?}", frame)
                                }
                                break 'receive;                                                                                                                                                                                                   
                            },
                            Err(TungsError::Utf8) => {
                                eprintln!("Error: Utf8 coding error");
                                continue 'receive;
                            },
                            Err(TungsError::Url(e)) => {
                                eprintln!("Error: Invalid Url; err={:?}", e);
                                continue 'receive;                                
                            },
                            Err(TungsError::Http(e)) => {
                                eprintln!("Error: Http error; err={:?}", e);
                                continue 'receive;                                
                            },
                            Err(TungsError::HttpFormat(e)) => {
                                eprintln!("Error: Http format error; err{:?}", e);
                                continue 'receive;                                
                            },
                        }
                    },
                    None => (),
                }
            }
            // NOTE: even heartbeat won't save us from arbitrary connection
            // closing down (around ~12-14 hours of long running process from
            // testing).
            _ = heartbeat_interval.tick() => {
                match ws_sender.send(Message::Text(r#"{"op":"ping"}"#.into())).await {
                    Ok(_) => println!("send ping message"),
                    Err(e) => eprintln!("error sending ping message; err={}", e),
                }
            }
        }
    }
    
    
    Ok(())
}




#[tokio::main]
async fn main() {
    println!("start");
    ws_loop(BB_WS_ENDPOINT).await;
}




use serde_json::json;

const TRADE_RECORD: &str =
r#"
{"topic":"ParseTradeMessage.BTCUSD",
 "data":[
       {"trade_time_ms":1619398389868,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":2000,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"8241a632-9f07-5fa0-a63d-06cefd570d75","cross_seq":6169452432},
       {"trade_time_ms":1619398389947,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":200,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"ff87be41-8014-5a33-b4b1-3252a6422a41","cross_seq":6169452432}]}
"#;

use serde_derive::Serialize;
use serde_derive::Deserialize;

#[derive(Debug, Serialize)]
#[serde(tag = "topic")]
enum BbMessage {
    #[serde(rename="ParseTradeMessage.BTCUSD")]
    TradeMessage {
        trade_time_ms: u64,
        timestamp: String,
        symbol: String,
        side: String,
        size: u32,
        price: f32,
        tickdirection: String,
        trade_id: String,
        cross_seq: u64,
    }    
}

/* 
#[test]
fn test_parse_trade_record() {
//    let m: BbMessage = serde_json::from_str(TRADE_RECORD)?;
    let m = serde_json::from_str(TRADE_RECORD);
}

*/

struct TradeMessage {

}

struct TradeRecord {
    time: u64,      // time in ms
    price: f32,
    size:  u32,
    id:    u128
}



/*
#[tokio::main]
async fn main() {
    /*
    // create bot instance for telegram
    let telegram_bot_instance = create_instance(
        &match std::env::var("HX_BYBIT_SHIPREKT_TELEGRAM_BOT_TOKEN") {
            Ok(res) => res,
            Err(e) => errprint_exit1!(OperationError::ErrorMissingRequiredEnvVar, "HX_BYBIT_SHIPREKT_TELEGRAM_BOT_TOKEN not defined; err={}", e),
        },
        &match std::env::var("HX_BYBIT_SHIPREKT_TELEGRAM_CHANNEL_CHAT_ID") {
            Ok(res) => res,
            Err(e) => errprint_exit1!(OperationError::ErrorMissingRequiredEnvVar, "HX_BYBIT_SHIPREKT_TELEGRAM_CHANNEL_CHAT_ID not defined; err={}", e),
        });
    */

    'main_reconnect_loop: loop {
        println!("Connecting to ByBit websocket...");
        // connect to wss
        let (ws_stream, _response) = match utils::connect_async_to_wss("wss://stream.bybit.com/realtime").await {
            Ok(res) => res,
            Err(e) => errprint_exit1!(e),
        };
        println!("(connected)");

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));

        // TODO: provide filtering options through cli e.g. BTCUSD, XRPUSD, etc
        match ws_sender.send(Message::Text(r#"{"op": "subscribe", "args": ["liquidation"]}"#.into())).await {
            Ok(_) => println!("subscribed to liquidation topic"),
            Err(e) => errprint_exit1!(OperationError::ErrorWssTopicSubscription, "error subscribing to liquidation topic; err={}", e),
        }

        loop {
            tokio::select! {
                msg_item = ws_receiver.next() => {
                    match msg_item {
                        Some(msg) => {
                            match msg {
                                Ok(Message::Text(json_str)) => {
                                    match serde_json::from_str::<'_, VariantResponse>(&json_str) {
                                        Ok(VariantResponse::Response(json_obj)) => {
                                            // TODO: provide option flag at CLI to avoid printing the
                                            // following. Fixed set to false for now.
                                            if false {
                                                // check 'op' field to differentiate type
                                                // of response
                                                match json_obj.request.op.to_lowercase().as_str() {
                                                    "ping" => println!("recieved pong msg"),
                                                    "subscribe" => println!("received subscribe msg"),
                                                    _ => (),
                                                }
                                            }
                                        },
                                        Ok(VariantResponse::Liquidation(json_obj)) => {
                                            let inner_json_obj = match json_obj.data {
                                                GenericData::Liquidation(json_obj) => json_obj,
                                            };

                                            let base_currency = utils::get_base_currency(&inner_json_obj.symbol).unwrap_or("UNKNOWN");
                                            let is_linear = utils::is_linear_perpetual(&inner_json_obj.symbol);
                                            let side = if inner_json_obj.side == "Buy" { "Long" } else { "Short" };

                                            let (ms, ns) = utils::get_ms_and_ns_pair(inner_json_obj.time);
                                            // FIXME: dang, NaiveDateTime::from_timestamp requires i64, this means
                                            // timestamp supports for 132 years further until 2102 since epoch 1970
                                            let datetime: DateTime<Utc> = DateTime::from_utc(NaiveDateTime::from_timestamp(ms as i64, ns), Utc);
                                            let bankruptcy_worth_str = ((inner_json_obj.price * inner_json_obj.qty as f64 * 1000.0_f64).round() / 1000.0_f64).separated_string();
                                            let qty_str = inner_json_obj.qty.separated_string();
                                            let price_str = inner_json_obj.price.separated_string();
                                            let base_or_quote_currency_str = if is_linear { "USDT" } else { base_currency };

                                            let message = format!("Bybit shiprekt a {side} position of {qty} {base_or_quote_currency} (worth ${bankruptcy_value}) on the {symbol} {perpetual_or_not} contract at ${price} - {datetime_str}",
                                                side=side,
                                                qty=qty_str,
                                                base_or_quote_currency=base_or_quote_currency_str,
                                                bankruptcy_value=bankruptcy_worth_str,
                                                symbol=inner_json_obj.symbol,
                                                perpetual_or_not=if utils::is_non_perpetual_contract(&inner_json_obj.symbol) { "Futures" } else { "Perpetual futures" },
                                                price=price_str,
                                                datetime_str=datetime.to_string());

                                            match send_message(&telegram_bot_instance, &message) {
                                                Ok(_) => println!("Notified event: {side} position of {symbol} worth ${bankruptcy_value} with {qty} {base_or_quote_currency} at ${price}",
                                                                  symbol=inner_json_obj.symbol,
                                                                  side=side,
                                                                  bankruptcy_value=bankruptcy_worth_str,
                                                                  qty=qty_str,
                                                                  base_or_quote_currency=base_or_quote_currency_str,
                                                                  price=price_str),
                                                // FIXME: upstream fix for rustelebot for `Display` of `ErrorResult`
                                                Err(e) => eprintln!("{}", e.msg)
                                            }
                                        },
                                        Err(e) => eprintln!("-- error parsing JSON response: {} --", e),
                                    }
                                },
                                Ok(Message::Ping(msg)) => println!("Received ping message; msg={:#?}", msg),
                                Ok(Message::Pong(msg)) => println!("Received pong message; msg={:#?}", msg),
                                Ok(Message::Binary(bins)) => println!("Received Binbary message, content={}", std::str::from_utf8(&bins).unwrap_or("unknown")),
                                Ok(Message::Frame(frame)) => println!("Received Frame message, content={:?}", frame),
                                Ok(Message::Close(optional_cf)) => match optional_cf {
                                   _ => {
                                       println!("(websocket closed)");
                                       continue 'main_reconnect_loop;      // reconnect to websocket again
                                   }
                                },

                                // from now they are error cases that we need to
                                // reconnect to websocket if occur
                                //
                                // by break into the outer loop
                                Err(TungsError::ConnectionClosed) => {
                                    eprintln!("Error: connection closed");
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::AlreadyClosed) => {
                                    eprintln!("Error: already closed");
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::Io(e)) => {
                                    eprintln!("Error: IO; err={}", e);
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::Tls(e)) => {
                                    eprintln!("Error:: Tls error; err={}", e);
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::Capacity(e)) => {
                                    type CError = tungstenite::error::CapacityError;
                                    match e {
                                        CError::TooManyHeaders => eprintln!("Error: CapacityError, too many headers"),
                                        CError::MessageTooLong{ size, max_size } => eprintln!("Error: CapacityError, message too long with size={}, max_size={}", size, max_size),
                                    }
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::Protocol(e)) => {
                                    eprintln!("Error: Protocol, err={}", e);
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::SendQueueFull(e)) => {
                                    type PMsg = tungstenite::protocol::Message;

                                    match e {
                                        PMsg::Text(text) => eprintln!("Error: SendQueueFull for Text message, content={}", text),
                                        PMsg::Binary(bins) => eprintln!("Error: SendQueueFull for Binary message, content={}", std::str::from_utf8(&bins).unwrap_or("unknown")),
                                        PMsg::Ping(bins) => eprintln!("Error: SendQueueFull for Ping message, content={}", std::str::from_utf8(&bins).unwrap_or("unknown")),
                                        PMsg::Pong(bins) => eprintln!("Error: SendQueueFull for Pong message, content={}", std::str::from_utf8(&bins).unwrap_or("unknown")),
                                        PMsg::Close(close_frame_optional) => {
                                            match close_frame_optional {
                                                Some(close_frame) => eprintln!("Error: SendQueueFull for Close message, content={:?}", close_frame),
                                                None => eprintln!("Error: SendQueueFull for Close message, no close-frame content")
                                            }
                                        },
                                        PMsg::Frame(frame) => eprintln!("Error: SendQueueFull for Frame messasge, content={:?}", frame)
                                    }
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::Utf8) => {
                                    eprintln!("Error: Utf8 coding error");
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::Url(e)) => {
                                    eprintln!("Error: Invalid Url; err={:?}", e);
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::Http(e)) => {
                                    eprintln!("Error: Http error; err={:?}", e);
                                    continue 'main_reconnect_loop;
                                },
                                Err(TungsError::HttpFormat(e)) => {
                                    eprintln!("Error: Http format error; err{:?}", e);
                                    continue 'main_reconnect_loop;
                                },
                            }
                        },
                        None => (),
                    }
                }
                // NOTE: even heartbeat won't save us from arbitrary connection
                // closing down (around ~12-14 hours of long running process from
                // testing).
                _ = heartbeat_interval.tick() => {
                    match ws_sender.send(Message::Text(r#"{"op":"ping"}"#.into())).await {
                        Ok(_) => println!("send ping message"),
                        Err(e) => eprintln!("error sending ping message; err={}", e),
                    }
                }
            }
        }
    }
}

*/
