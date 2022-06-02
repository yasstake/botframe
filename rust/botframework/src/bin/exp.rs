







struct TransactionId {
    id: u128
}

impl TransactionId {
    fn from_str(id: &str) -> TransactionId {

        fn hex_to_char(c: char) -> i32 {
            match c {
                '0' => return 0,
                '1' => return 1,
                '2' => return 2,
                '3' => return 3,
                '4' => return 4,
                '5' => return 5,
                '6' => return 6,
                '7' => return 7,
                '8' => return 8,
                '9' => return 9,
                'a' => return 10,
                'b' => return 11,
                'c' => return 12,
                'd' => return 13,
                'e' => return 14,
                'f' => return 15,                                                                                                                        
                'A' => return 10,
                'B' => return 11,
                'C' => return 12,
                'D' => return 13,
                'E' => return 14,
                'F' => return 15,                                                                                                                        
                _ => {
                    println!("error ->{}",c); 
                    return 0xff
                }
            }
        }
        
        fn parse_hex_string(h: &str) -> u128 {
            let mut num: u128 = 0;
        
            for c in h.chars() {
                num <<= 4;
                let hex = hex_to_char(c) as u128;
                num += hex;
            }
        
            return num;
        }


        //　　　　　　　"00c706e1-ba52-5bb0-98d0-bf694bdc69f7";
        //            |   |   ||   ||   ||   ||   |   |
        // hyphens  : |   |   8|  13|  18|  23|   |   |
        // positions: 0   4    9   14   19   24  28  32   36
        let mut hex_id: u128 = 0;
        
        let id1_str: &str =  &id[0..8];
        let n = parse_hex_string(id1_str);
        hex_id += n;
        
        let id2_str: &str =  &id[9..13];
        hex_id <<= 4*4;    
        let n = parse_hex_string(id2_str);
        hex_id += n;
        
        let id3_str: &str =  &id[14..18];
        hex_id <<= 4*4;    
        let n = parse_hex_string(id3_str);
        hex_id += n;
        
        let id4_str: &str =  &id[19..23];
        hex_id <<= 4*4;    
        let n = parse_hex_string(id4_str);
        hex_id += n;
        
        let id5_str: &str =  &id[24..];                
        hex_id <<= 4*12;        
        let n = parse_hex_string(id5_str);
        hex_id += n;
        
        TransactionId { id: hex_id}
    }

    fn to_str(self) -> String {
        format!("{:032x}", self.id)
    }
}


#[test]
fn test_transaction_id() {
    const ID: &str = "00c706e1-ba52-5bb0-98d0-bf694bdc69f7";    
    const ID2: &str = "00c706e1ba525bb098d0bf694bdc69f7";        
    let id = TransactionId::from_str(ID);
    let id_str = id.to_str();
    println!("{}-{}", ID, id_str);

    assert_eq!(ID2, id_str);
}


fn main() {
    println!("hello");
}
