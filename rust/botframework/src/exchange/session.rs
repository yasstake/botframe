


use crate::exchange::MarketInfo;
use crate::exchange::Market;

use std::rc::Rc;


// Status life cycle
//   "CREATED" -> "CLOSE" or "CANCEL"


struct Order {
    order_id: String,
    create_time: i64,
    valid_until: i64,
    price: f64,
    sell_volume: f64,
    buy_volume: f64,    
    _partial_work: f64
}

struct ClosedOrder {
    order_id: String,
    create_time: f64,
    status: String,
    price: f64,
    sell_volume: f64,
    buy_volume: f64,    
    sell_volume_filled: f64,
    buy_volume_filled: f64,    
    fee: f64,
    total: f64
}

struct Indicator {
    time: i64,
    key: String,
    value: f64
}

struct SessionValue {
    current_time: i64,
    current_orders: Vec<Order>,
    order_history: Vec<ClosedOrder>,
    position: Vec<Order>,
    balance: f64,
}

impl SessionValue {
    fn exec_event(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64){

    }    
   
    fn insert_order(&self, side: &str, price: f64, volume: f64, duration_ms: i64) -> bool {
        return false
    }    


}



pub trait Agent {
    fn on_tick(&self, session: &Market, time_ms: i64); 
    fn on_exec(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64);
    fn on_order(&self, session: &Market, time_ms: i64, action: &str, price: f64, size: f64);    
}

pub trait Session {
    fn reset_session(&self);
    fn get_timestamp_ms(&self) -> i64;
    fn make_order(&self, side: &str, price: f64, volume: f64, duration_ms: i64) -> bool;
    fn get_orders(&self);
    fn get_posision(&self);
    fn get_posision_list(&self);
    fn ohlcv(&mut self, width_sec: i64, count: i64) -> ndarray::Array2<f64>;
    fn get_balance(&self) -> f64;
    fn set_indicator(&self, key: &str, value: f64);
    fn result(key: &str) -> String;
    fn run(&self, agent: &dyn Agent, from_time_ms: i64, time_interval_ms: i64) -> bool;
}


