#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OrderType {
    Buy,
    Sell,
    Unknown,
}

impl OrderType {
    pub fn from_str(order_type: &str) -> Self {
        match order_type.to_uppercase().as_str() {
            "B" | "BUY" => {
                return OrderType::Buy;
            }
            "S" | "SELL" | "SEL" => {
                return OrderType::Sell;
            }
            _ => {
                println!("Error Unknown order type {}", order_type);
                return OrderType::Unknown;
            }
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            OrderType::Buy => return &"B",
            OrderType::Sell => return &"S",
            OrderType::Unknown => {
                println!("ERROR unknown order type");
                return &"UNKNOWN";
            }
        }
    }
}


pub enum OrderStatus {
    NoAction,
    Enqueue,
    OrderComplete,
    OrderExpire,
    Liquidation,
    NoMoney,
}

impl OrderStatus {
    fn to_str(&self) -> &str {
        match self {
            FailOrder => return &"Fail",
            Enqueue => return &"Enqueue",
            OrderComplete => return &"Complete",
            OrderExpire => return &"Expire",
            Liquidation => return &"Liquidation",
            NoMoney => return &"NoMoney",
            _ => return "Unknown",
        }
    }
}

pub struct ClosedOrder {
    timestamp: i64,
    order_id: String,
    order_type: OrderType,
    create_time: i64,
    status: OrderStatus,
    price: f64,
    sell_size: f64, // in usd
    buy_size: f64,  // in usd
    profit: f64,
    fee: f64,
    total_profit: f64,
}

impl ClosedOrder {
    pub fn from_order(
        timestamp: i64,
        order: &Order,
        status: OrderStatus,
        profit: f64,
        fee: f64,
        total_profit: f64,
    ) -> Self {
        let mut sell_size = 0.0;
        let mut buy_size = 0.0;
        match order.order_type {
            OrderType::Buy => {
                buy_size = order.size;
            }
            OrderType::Sell => {
                sell_size = order.size;
            }
            OrderType::Unknown => {}
        }

        return ClosedOrder {
            timestamp: timestamp,
            order_id: order.order_id.clone(),
            order_type: order.order_type,
            create_time: order.create_time,
            status: status,
            price: order.price,
            sell_size: sell_size,
            buy_size: buy_size,
            profit: profit,
            fee: fee,
            total_profit: total_profit,
        };
    }
}

// Status life cycle
//   "CREATED" -> "CLOSE" or "CANCEL"

#[derive(Debug)]
pub struct Order {
    pub create_time: i64, // in ms
    pub order_id: String, // YYYY-MM-DD-SEQ
    pub order_type: OrderType,
    pub valid_until: i64, // in ms
    pub price: f64,
    pub size: f64,        // in USD
    pub taker: bool,      // takerの場合true, falseの場合はmakerとなる。
    pub remain_size: f64, // ログから想定した未約定数。０になったら全部約定。本来は分割で約定するが、０となの全部約定時のみ発生。
}

impl Order {
    pub fn new(
        create_time: i64, // in ms
        order_id: String, // YYYY-MM-DD-SEQ
        order_type: OrderType,
        valid_until: i64, // in ms
        price: f64,
        size: f64, // in USD
        taker: bool,
    ) -> Self {
        return Order {
            create_time: create_time,
            order_id: order_id,
            order_type: order_type,
            valid_until: valid_until,
            price: price,
            size: size,
            taker: taker,
            remain_size: size,
        };
    }
}

/// 未実現オーダーリストを整理する。
/// ・　オーダーの追加
/// ・　オーダの削除（あとで実装）
/// ・　オーダー中のマージン計算
/// ・　オーダーのExpire
/// ・　オーダーの約定
pub struct Orders {
    buy_queue: bool,
    q: Vec<Order>,
}

use std::cmp::Ordering;

impl Orders {
    pub fn new(buy_order: bool) -> Self {
        return Orders {
            buy_queue: buy_order,
            q: vec![],
        };
    }

    /// オーダーをキューに入れる。
    pub fn queue_order(&mut self, order: Order) {
        self.q.push(order);
        self.sort();
    }

    pub fn margin(&mut self) -> f64 {
        let mut m: f64 = 0.0;

        for order in &self.q {
            m += order.size;
        }

        return m;
    }

    // Sellオーダーを約定しやすい順番に整列させる
    //   *やすい順番
    //   早い順番
    fn sell_comp(a: &Order, b: &Order) -> Ordering {
        if a.price < b.price {
            return Ordering::Less;
        } else if a.price > b.price {
            return Ordering::Greater;
        } else if a.create_time < b.create_time {
            return Ordering::Less;
        }
        return Ordering::Equal;
    }

    // buyオーダーを約定しやすい順番に整列させる
    //   *高い順番
    //   早い順番
    fn buy_comp(a: &Order, b: &Order) -> Ordering {
        if a.price < b.price {
            return Ordering::Greater;
        } else if a.price > b.price {
            return Ordering::Less;
        } else if a.create_time < b.create_time {
            return Ordering::Less;
        }
        return Ordering::Equal;
    }

    fn sort(&mut self) {
        if self.buy_queue {
            // 高い方・古い方から並べる
            self.q.sort_by(Orders::buy_comp);
        } else {
            // 安い方・古い方から並べる
            self.q.sort_by(Orders::sell_comp);
        }
    }

    /// Queueの中にオーダがはいっているかを確認する。
    pub fn has_q(&self) -> bool {
        return self.q.is_empty() == false;
    }

    ///　全件なめる処理になるので数秒ごとに１回でOKとする。
    /// 先頭の１つしかExpireしないが、何回も呼ばれるのでOKとする（多少の誤差を許容）
    fn expire(&mut self, current_time_ms: i64) -> Result<ClosedOrder, OrderStatus> {
        let l = self.q.len();

        for i in 0..l {
            if self.q[i].valid_until < current_time_ms {
                let order = self.q.remove(i);

                let close_order = ClosedOrder::from_order(
                    current_time_ms,
                    &order,
                    OrderStatus::OrderExpire,
                    0.0,
                    0.0,
                    0.0,
                );
                return Ok(close_order);
            }
        }

        return Err(OrderStatus::NoAction);
    }

    /// 約定履歴からオーダーを処理する。
    /// 優先度の高いほうから１つづつ処理することとし、先頭のオーダ一つが約定したらリターンする。
    /// うまくいった場合はClosedOrderを返す（ほとんどの場合はErrを返す(前回から変化が小さいのでなにもしていない）
    /// 約定は、一つ下の刻みのログが発生したらカウントする。
    /// 超巨大オーダがきた場合でも複数約定はさせず、次回に回す。
    pub fn execute(
        &mut self,
        current_time_ms: i64,
        price: f64,
        size: f64,
    ) -> Result<ClosedOrder, OrderStatus> {
        if self.has_q() == false {
            return Err(OrderStatus::NoAction);
        }

        let l = self.q.len();
        let mut size_remain = size;

        // 順番に価格条件をみたしたものから約定したこととし、remain_sizeをへらしていく。
        for i in 0..l {
            if (self.buy_queue && (self.q[i].price < price))
                || (!self.buy_queue && (price < self.q[i].price))
            {
                if self.q[i].remain_size < size_remain {
                    self.q[i].remain_size = 0.0;
                    size_remain -= self.q[i].remain_size;
                } else {
                    self.q[i].remain_size -= size_remain;
                    size_remain = 0.0;
                    return Err(OrderStatus::NoAction);
                }
            }
        }

        for i in 0..l {
            if self.q[i].remain_size <= 0.0 {}
        }

        return Err(OrderStatus::NoAction);
    }

    /// キューの中に処理できるオーダーがあれば、size_remainをへらしていく。
    /// size_remainが０になったらオーダ完了の印。
    /// 実際の取り出しは pop_order_historyで実施する。
    fn execute_remain_size(&mut self, current_time_ms: i64, price: f64, size: f64) -> bool {
        if self.has_q() == false {
            return false;
        }

        let l = self.q.len();
        let mut size_remain = size;
        let mut update = false;

        // 順番に価格条件をみたしたものから約定したこととし、remain_sizeをへらしていく。
        for i in 0..l {
            if (self.buy_queue && (self.q[i].price < price))
                || (!self.buy_queue && (price < self.q[i].price))
            {
                update = true;
                if self.q[i].remain_size < size_remain {
                    self.q[i].remain_size = 0.0;
                    size_remain -= self.q[i].remain_size;
                } else {
                    self.q[i].remain_size -= size_remain;
                    size_remain = 0.0;

                    break;
                }
            }
        }

        return update;
    }

    ///　全額処理されたオーダをキューから取り出し ClosedOrderオブジェクトを作る。
    /// 損益データはここでは入れない。取り出したあとで別途設定する。
    fn pop_order_history(
        &mut self,
        current_time_ms: i64,
        price: i64,
    ) -> Result<ClosedOrder, OrderStatus> {
        let l = self.q.len();

        for i in 0..l {
            // 約定完了のオーダーバックログを発見。処理は１度に１回のみ。本来は巨大オーダで複数処理されることがあるけど実装しない。
            if self.q[i].remain_size <= 0.0 {
                let order = &self.q.remove(i);

                // TODO: 取引手数料の計算
                let close_order = ClosedOrder::from_order(
                    current_time_ms,
                    &order,
                    OrderStatus::OrderComplete,
                    0.0,
                    0.0,
                    0.0,
                );

                return Ok(close_order);
            }
        }

        return Err(OrderStatus::NoAction);
    }
}


/////////////////////////////////////////////////////////////////////////////////
// TEST
///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod OrderTypeTest {
    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(OrderType::from_str("buy"), OrderType::Buy);
        assert_eq!(OrderType::from_str("Buy"), OrderType::Buy);
        assert_eq!(OrderType::from_str("B"), OrderType::Buy);
        assert_eq!(OrderType::from_str("BUY"), OrderType::Buy);

        assert_eq!(OrderType::Buy.to_str(), "B");

        assert_eq!(OrderType::from_str("Sell"), OrderType::Sell);
        assert_eq!(OrderType::from_str("S"), OrderType::Sell);
        assert_eq!(OrderType::from_str("SELL"), OrderType::Sell);
        assert_eq!(OrderType::from_str("sell"), OrderType::Sell);

        assert_eq!(OrderType::Sell.to_str(), "S");
    }
}


#[cfg(test)]
mod TestOrders {
    use super::*;
    #[test]
    fn test_orders() {
        let mut orders = Orders::new(true);
        assert_eq!(orders.has_q(), false);

        let o1 = Order::new(
            1,
            "low price".to_string(),
            OrderType::Buy,
            2,
            100.0,
            100.0,
            false,
        );
        let o2 = Order::new(
            3,
            "low price but later".to_string(),
            OrderType::Buy,
            2,
            100.0,
            50.0,
            false,
        );
        let o3 = Order::new(
            2,
            "high price".to_string(),
            OrderType::Buy,
            2,
            200.0,
            200.0,
            false,
        );
        let o4 = Order::new(
            1,
            "high price but first".to_string(),
            OrderType::Buy,
            2,
            200.0,
            50.0,
            false,
        );

        orders.queue_order(o1);
        assert_eq!(orders.has_q(), true);
        orders.queue_order(o2);
        orders.queue_order(o3);
        orders.queue_order(o4);

        assert_eq!(orders.margin(), 400.0);

        assert_eq!(orders.q[0].price, 200.0);
        assert_eq!(orders.q[0].size, 50.0);
        assert_eq!(orders.q[1].price, 200.0);
        assert_eq!(orders.q[1].size, 200.0);
        assert_eq!(orders.q[2].price, 100.0);
        assert_eq!(orders.q[2].size, 100.0);
        assert_eq!(orders.q[3].price, 100.0);
        assert_eq!(orders.q[3].size, 50.0);
        println!("{:?}", orders.q);

        // Sell Order
        let mut orders = Orders::new(false);

        let o1 = Order::new(
            1,
            "low price".to_string(),
            OrderType::Sell,
            2,
            100.0,
            100.0,
            false,
        );
        let o2 = Order::new(
            3,
            "low price but later".to_string(),
            OrderType::Sell,
            2,
            100.0,
            50.0,
            false,
        );
        let o3 = Order::new(
            2,
            "high price".to_string(),
            OrderType::Sell,
            2,
            200.0,
            200.0,
            false,
        );
        let o4 = Order::new(
            1,
            "high price but first".to_string(),
            OrderType::Sell,
            2,
            200.0,
            50.0,
            false,
        );

        orders.queue_order(o1);
        orders.queue_order(o2);
        orders.queue_order(o3);
        orders.queue_order(o4);

        assert_eq!(orders.margin(), 400.0);

        assert_eq!(orders.q[2].price, 200.0);
        assert_eq!(orders.q[2].size, 50.0);
        assert_eq!(orders.q[3].price, 200.0);
        assert_eq!(orders.q[3].size, 200.0);
        assert_eq!(orders.q[0].price, 100.0);
        assert_eq!(orders.q[0].size, 100.0);
        assert_eq!(orders.q[1].price, 100.0);
        assert_eq!(orders.q[1].size, 50.0);
        println!("{:?}", orders.q);
    }
}

