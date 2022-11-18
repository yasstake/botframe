pub mod ftx;


#[cfg(test)]
mod test_exchange {

    // reqwest https://docs.rs/reqwest/latest/reqwest/struct.Response.html
    #[tokio::test]
    async fn download_yahoo() {
        use futures::StreamExt;

        match reqwest::get("https://www.yahoo.co.jp").await {
            Ok(response) => {
                let mut stream = response.bytes_stream();

                while let Some(item) = stream.next().await {
                    print!("{:?}", item.unwrap());
                }
            },
            Err(e) => {
                panic!("Open URL Error {:?}", e);
            }
        }
    }

    /*
    use std::io::BufReader;
    // client builder https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html#method.gzip
    #[test]
    fn download_log() {
        let client_builder = reqwest::blocking::Client::builder();
        // let client_builder = reqwest::Client::builder();

        let client = client_builder
            .build().unwrap();

        match client.get("https://public.bybit.com/trading/BTCUSD/BTCUSD2022-11-17.csv.gz").send() {
            Ok(response) => {
                let result= response.bytes();

                if let Ok(mut item) = result {
                    BufReader::new((GzDecoder::new(item)));
                    print!("{:?}", item);
                }
            },
            Err(e) => {
                panic!("Open URL Error {:?}", e);
            }

        }
    }
    */

    use std::error;

    /*
    #[tokio::main]
    async fn main() {
        use async_compression::futures::bufread::GzipDecoder;
        use std::io::BufReader;

        let response = reqwest::get("http://localhost:8000/test.txt.gz").await?;
        let reader = response.bytes_stream();


        let mut decoder = GzipDecoder::new(BufReader::new(reader.into_stream()));
        let mut data = String::new();
        decoder.read_to_string(&mut data).await?;
        println!("{data:?}");
        Ok(())
    }    
    */

    /*
    #[tokio::test]
    async fn test_get() {
        get();
    }

    */

    #[tokio::test]
    async fn main() -> Result<(), Box<dyn error::Error>> {
        use async_compression::futures::bufread::GzipDecoder;
        use futures::{
            io::{self, BufReader, ErrorKind},
            prelude::*,
        };
        let response = reqwest::get("https://public.bybit.com/trading/BTCUSD/BTCUSD2022-11-17.csv.gz").await?;
        let reader = response
            .bytes_stream()
            .map_err(|e| io::Error::new(ErrorKind::Other, e))
            .into_async_read();
        let mut lines  = BufReader::new(GzipDecoder::new(BufReader::new(reader)));

        loop {
            let mut buf = String::new();

            match lines.read_line(&mut buf).await {
                Ok(read_size) => {
                    if read_size == 0 {
                        break;
                    }
                    print!("{}", buf);
                },
                Err(_e) => {
                    // EOF
                    break;
                }
            } 
        }

        Ok(())
    }


    #[tokio::test]
    async fn stream_ungzip_get() -> Result<(), String> {
        use async_compression::futures::bufread::GzipDecoder;
        use futures::{
            io::{self, BufReader, ErrorKind},
            prelude::*,
        };
        use reqwest::Response;

        // async で GET
        let result = reqwest::get("https://public.bybit.com/trading/BTCUSD/BTCUSD2022-11-17.csv.gz").await;

        let response: Response;
        match result {
            Ok(r) => {
                response = r;
            },
            Err(e) => {
                return Err(e.to_string());
            }
        }

        // 順次解凍しながら表示
        let reader = response
            .bytes_stream()
            .map_err(|e| io::Error::new(ErrorKind::Other, e))
            .into_async_read();
        let mut lines  = BufReader::new(GzipDecoder::new(BufReader::new(reader)));

        loop {
            let mut buf = String::new();

            match lines.read_line(&mut buf).await {
                Ok(read_size) => {
                    if read_size == 0 {
                        break;
                    }
                    print!("{}", buf);  // 表示
                },
                Err(_e) => {
                    // EOF
                    break;
                }
            } 
        }

        Ok(())
    }


    // timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
    // 1668728606,BTCUSD,Buy,611,16661.00,ZeroMinusTick,00fcc5e9-d36f-51ae-9906-11f83ed505a7,3.6672468639337374e+06,611,0.036672468639337374    

    #[test]
    fn test_parse() {
        parse_log_rec("1668728606,BTCUSD,Buy,611,16661.00,ZeroMinusTick,00fcc5e9-d36f-51ae-9906-11f83ed505a7,3.6672468639337374e+06,611,0.036672468639337374");
    }

    pub fn parse_log_rec(rec: &str) {
        let rec_trim = rec.trim();
        let row = rec_trim.split(",");  // カラムに分割
    
        let mut time_us: i64 = 0;
        let mut price: f64 = 0.0;
        let mut size: f64 = 0.0;
        let mut order_side: String = String::new();
        let mut id: String = String::new();
    
        // カラム毎の処理
        for (i, col) in row.enumerate() {
            match i {
                0 => {
                    /*timestamp*/
                    time_us = col.parse::<i64>().unwrap();
                    time_us *= 1_000;
                }
                1 => { /* symbol IGNORE */ }
                2 => {
                    /* side */
                    order_side = col.to_string();
                }
                3 => {
                    /* size */
                    size = col.parse::<f64>().unwrap();
                }
                4 => {
                    /* price */
                    price = col.parse::<f64>().unwrap();
                }
                5 => { /* tickDirection IGNORE */ }
                6 => {
                    /* trdMatchID */
                    id = col.to_string();
                }
                7 => { /* grossValue IGNORE */ }
                8 => { /* homeNotional IGNORE */ }
                9 => { /* foreignNotional IGNORE */ }
                _ => {
                    /* ERROR */
                    panic!("unknon record format");
                }
            }
        }

        println!("time_us: {}, order_side: {}, price: {}, size: {}, id: {}", time_us, order_side, size, price, id);
    }


}




