



/* 
sample data
https://bybit-exchange.github.io/docs/inverse/?console#t-websockettrade

{
    "topic": "trade.BTCUSD",
    "data": [
        {
            "timestamp": "2020-01-12T16:59:59.000Z",
            "trade_time_ms": 1582793344685,
            "symbol": "BTCUSD",
            "side": "Sell",
            "size": 328,
            "price": 8098,
            "tick_direction": "MinusTick",
            "trade_id": "00c706e1-ba52-5bb0-98d0-bf694bdc69f7",
            "cross_seq": 1052816407
        }
    ]
}
*/



struct TradId {
    id: u128
}

impl TradeId {
    fn from_str() {

    }

}


struct Trade {

}
