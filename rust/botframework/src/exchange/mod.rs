

enum OrderType {
    Buy,
    Sell
}


struct Trade {
    time_ns: i64,
    price: f32,
    size:  f32,
    bs:  OrderType,
    id: String
}


struct Market {
    time_ns: Vec<i64>,
    price: Vec<f32>,
    size:  Vec<f32>,
    bs:  Vec<OrderType>,
    id: Vec<String>
}

impl Market {
    fn new() -> Market {
        return Market { 
            time_ns: Vec::new(), 
            price: Vec::new(), 
            size: Vec::new(), 
            bs: Vec::new(), 
            id: Vec::new() 
        }
    }

    fn add_trade(mut self, time_ns: i64, price: f32, size:  f32, bs:  OrderType,id: String) -> Market{
        self.time_ns.push(time_ns);
        self.price.push(price);
        self.size.push(size);
        self.bs.push(bs);
        self.id.push(id);

        return self;
    }
}

#[test]
fn test_add_trade() {
    let mut m = Market::new();

    for i in 0..300000*30 {
        m = m.add_trade(1000, 10.0, 10.0, OrderType::Buy, "asdfasdfasdfasdfasdf".to_string());
    }

    println!("{}", m.id.len());
}
