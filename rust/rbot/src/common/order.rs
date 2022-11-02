use super::time::MicroSec;
use pyo3::pyclass;
use pyo3::pymethods;

use strum_macros::Display;

#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Display)]
pub enum OrderSide {
    Buy,
    Sell,
    Unknown,
}

impl OrderSide {
    pub fn from_str(order_type: &str) -> Self {
        match order_type.to_uppercase().as_str() {
            "B" | "BUY" => {
                return OrderSide::Buy;
            }
            "S" | "SELL" | "SEL" => {
                return OrderSide::Sell;
            }
            _ => {
                return OrderSide::Unknown;
            }
        }
    }

    pub fn from_buy_side(buyside: bool) -> Self {
        match buyside {
            true => {
                return OrderSide::Buy;
            },
            _ => {
                return OrderSide::Sell;
            }
        }
    }

    pub fn is_buy_side(&self) -> bool {
        match &self {
            OrderSide::Buy => {
                return true;
            }
            _ => {
                return false;
            }
        }
    }



}

// Represent one Trade execution.
#[pyclass]
#[derive(Debug)]
pub struct Trade {
    pub time: MicroSec,
    pub price: f64,
    pub size: f64,
    pub order_side: OrderSide,
    pub liquid: bool,
    pub id: String,
}


impl Trade {
    pub fn new(time_microsec: MicroSec, price: f64, size: f64, order_side: OrderSide, liquid: bool, id: String) -> Self{
        return Trade {
            time: time_microsec,
            price,
            size,
            order_side,
            liquid,
            id
        }
    }

    pub fn to_csv(&self) -> String {
        format!("{}, {}, {}, {}, {}, {}\n", self.time, self.price, self.size, self.order_side, self.liquid, self.id)
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct Order {
    _order_index: i64,
    pub create_time: MicroSec, // in ns
    pub order_id: String,     // YYYY-MM-DD-SEQ
    pub order_side: OrderSide,
    pub post_only: bool,
    pub valid_until: MicroSec, // in ns
    pub price: f64,           // in
    pub size: f64,            // in forein
    pub message: String,
    pub remain_size: f64, // ログから想定した未約定数。０になったら全部約定。
}

impl Order {
    pub fn new(
        create_time: MicroSec, // in ns
        order_id: String,     // YYYY-MM-DD-SEQ
        order_side: OrderSide,
        post_only: bool,
        valid_until: MicroSec, // in ns
        price: f64,
        size: f64,            // in forreign currency.
        message: String,
    ) -> Self {
        return Order {
            _order_index: 0,
            create_time,
            order_id,
            order_side,
            post_only,
            valid_until,
            price,
            size,
            message,
            remain_size: size,
        };
    }
}

#[pyclass]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OrderStatus {
    NoAction,
    Wait,          // 処理中
    InOrder,       // オーダー中
    OrderComplete, // tempolary status.
    OpenPosition,  // ポジションオープン
    ClosePosition, // ポジションクローズ（このときだけ、損益計算する）
    OverPosition,  // ポジション以上の反対売買。別途分割して処理する。
    ExpireOrder,   // 期限切れ
    Liquidation,   // 精算
    PostOnlyError, // 指値不成立。
    NoMoney,       //　証拠金不足（オーダできず）
    Error,         // その他エラー（基本的には発生させない）
}

#[test]
fn test_print_order_status() {
    let s = OrderStatus::Wait;

    println!("{:#?}", s);

    println!("{}", s.to_string());
}

impl OrderStatus {
    pub fn to_string(&self) -> String {
        return format!("{:#?}", self);
    }
}

// 約定結果
#[pyclass]
#[derive(Debug, Clone)]
pub struct OrderResult {
    pub update_time: MicroSec,
    pub order_id: String,
    pub order_sub_id: i32, // 分割された場合に利用
    pub order_side: OrderSide,
    pub post_only: bool,
    pub create_time: MicroSec,
    pub status: OrderStatus,
    pub open_price: f64,
    pub close_price: f64,
    pub price: f64,
    pub home_size: f64,
    pub foreign_size: f64,
    pub profit: f64,
    pub fee: f64,
    pub total_profit: f64,
    pub message: String,
}

impl OrderResult {
    pub fn from_order(
        timestamp: MicroSec,
        order: &Order,
        status: OrderStatus,
    ) -> Self {
        return OrderResult {
            update_time: timestamp,
            order_id: order.order_id.clone(),
            order_sub_id: 0,
            order_side: order.order_side,
            post_only: order.post_only,
            create_time: order.create_time,
            status,
            open_price: order.price,
            close_price: 0.0,
            price: order.price,
            home_size: order.size,
            foreign_size: order.size / order.price,
            profit: 0.0,
            fee: 0.0,
            total_profit: 0.0,
            message: order.message.clone(),
        };
    }

    fn update_foreign_size(&mut self) {
        self.foreign_size = self.home_size / self.open_price; // まだ約定されていないはずなのでOpenPrice採用
    }

    /// オーダーを指定された大きさで２つに分ける。
    /// 一つはSelf, もう一つはCloneされたChild Order
    /// 子供のオーダについては、sub_idが１インクリメントする。
    /// 分けられない場合(境界が大きすぎる） NoActionが返る。
    pub fn split_child(&mut self, size: f64) -> Result<OrderResult, OrderStatus> {
        if self.home_size < size {
            // do nothing.
            return Err(OrderStatus::NoAction);
        }

        let mut child = self.clone();

        child.order_sub_id = self.order_sub_id + 1;
        child.home_size = self.home_size - size;
        child.update_foreign_size();

        self.home_size = size;
        self.update_foreign_size();

        return Ok(child);
    }
}


///////////////////////////////////////////////////////////////////////////////
//     Unit TEST
///////////////////////////////////////////////////////////////////////////////


#[cfg(test)]
mod order_side_test {
    use super::*;
    #[test]
    fn test_from_str() {
        assert_eq!(OrderSide::from_str("B"), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("Buy"), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("BUY"), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("S"), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("Sell"), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("SELL"), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("BS"), OrderSide::Unknown);
    }

    fn test_from_buy_side() {
        assert_eq!(OrderSide::from_buy_side(true), OrderSide::Buy);
        assert_eq!(OrderSide::from_buy_side(false), OrderSide::Sell);
    }

    fn test_is_buy_side() {
        assert_eq!(OrderSide::Buy.is_buy_side(), true);
        assert_eq!(OrderSide::Sell.is_buy_side(), false);        
    }

}

#[cfg(test)]
mod order_test {
    use super::*;
    #[test]
    fn test_new() {
        let _order_index = 0;
        let create_time = 1;
        let order_id = "id".to_string();
        let order_side = OrderSide::Buy;
        let post_only = false;
        let valid_until = 10;
        let price = 10.0;
        let size = 12.0;
        let message = "message".to_string();

        let order = Order::new(create_time, order_id.clone(), order_side, post_only, valid_until, price, size, message.clone());

        println!("{:?}", order);
        assert_eq!(0, order._order_index);
        assert_eq!(create_time, order.create_time);
        assert_eq!(order_id, order.order_id);
        assert_eq!(order_side, order.order_side);
        assert_eq!(post_only, order.post_only);
        assert_eq!(valid_until, order.valid_until);
        assert_eq!(price, order.price);
        assert_eq!(message, order.message);
        assert_eq!(size, order.remain_size);
    }
}

#[cfg(test)]
#[allow(unused_results)]
#[allow(unused_variables)]
mod order_result_test {
    use super::*;
    
    #[test]
    fn test_from_order() {
        let _order_index = 0;
        let create_time = 1;
        let order_id = "id".to_string();
        let order_side = OrderSide::Buy;
        let post_only = false;
        let valid_until = 10;
        let price = 10.0;
        let size = 12.0;
        let message = "message".to_string();

        let order = Order::new(create_time, order_id.clone(), order_side, post_only, valid_until, price, size, message.clone());

        let current_time: MicroSec = 100;
        let order_result = OrderResult::from_order(current_time, &order, OrderStatus::OpenPosition);

        assert_eq!(order_result.update_time, current_time);
    }


    #[test]
    fn test_split_order_small() {
        let order = Order::new(1, "close".to_string(), OrderSide::Buy, true, 100, 10.0, 100.0, "msg".to_string());

        let mut closed_order = OrderResult::from_order(2, &order, OrderStatus::OrderComplete);
        assert_eq!(closed_order.home_size, 100.0);
        assert_eq!(closed_order.foreign_size, 100.0/10.0);

        let result =  &closed_order.split_child(10.0);

        match result {
            Ok(child) => {
                assert_eq!(closed_order.home_size, 10.0);
                assert_eq!(closed_order.foreign_size, 10.0/10.0);
                assert_eq!(child.home_size, 90.0);
                assert_eq!(child.foreign_size, 90.0/10.0);

                println!("{:?}", child);
            }
            Err(_) => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_split_order_eq() {
        let order = Order::new(1, "close".to_string(), OrderSide::Buy, true, 100, 10.0, 100.0, "msg".to_string());

        let mut closed_order = OrderResult::from_order(2, &order, OrderStatus::OrderComplete);

        let result =  &closed_order.split_child(100.0);

        match result {
            Ok(child) => {
                println!("{:?}", closed_order);
                assert_eq!(closed_order.home_size, 100.0);
                assert_eq!(closed_order.foreign_size, 100.0/10.0);
                assert_eq!(child.home_size, 0.0);
                assert_eq!(child.foreign_size, 0.0);

                println!("{:?}", child);
            }
            Err(_) => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_split_order_big() {
        let order = Order::new(1, "close".to_string(), OrderSide::Buy, true, 100, 10.0, 100.0, "msg".to_string());

        let mut closed_order = OrderResult::from_order(2, &order, OrderStatus::OrderComplete);

        let result =  &closed_order.split_child(100.1);

        match result {
            Ok(_) => {
                assert!(false);
            }
            Err(_) => {
                assert!(true);
            }
        }
    }

}

#[derive(Debug)]
pub struct TimeChunk {
    pub start: MicroSec,
    pub end: MicroSec
}
