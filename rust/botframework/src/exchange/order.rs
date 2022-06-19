#[derive(Debug, PartialEq)]
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
