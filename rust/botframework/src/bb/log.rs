// Copyright(c) yasstake 2022. All rights reserved. (no warranty)

use std::fs;

use flate2::bufread::GzDecoder;

use directories::ProjectDirs;
use std::fs::File;
use std::io::prelude::*;

pub type BbError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type BbResult<T> = Result<T, BbError>;

#[derive(Clone, Copy, PartialEq)]
pub enum MarketType {
    BTCUSD,
    BTCUSDT,
    ETHUSD,
    ETHUSDT,
    XRPUSD,
    XRPUSDT,
    SOLUSDT,
    ERROR,
}

use std::io::Error;


impl MarketType {
    pub fn from_str(symbol: &str) -> MarketType {
        match symbol {
            "BTCUSD" => {
                return MarketType::BTCUSD;
            }
            "BTCUSDT" => {
                return MarketType::BTCUSDT;
            }
            "ETHUSD" => {
                return MarketType::ETHUSD;
            }
            "ETHUSDT" => {
                return MarketType::ETHUSDT;
            }
            "XRPUSD" => {
                return MarketType::XRPUSD;
            }
            "XRPUSDT" => {
                return MarketType::XRPUSDT;
            }
            "SOLUSDT" => {
                return MarketType::SOLUSDT;
            }
            _ => {
                println!("unknown type");
                return MarketType::ERROR;
            }
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            &MarketType::BTCUSD => {
                return &"BTCUSD";
            }
            &MarketType::BTCUSDT => {
                return &"BTCUSDT";
            }
            &MarketType::ETHUSD => {
                return &"ETHUSD";
            }
            &MarketType::ETHUSDT => {
                return &"ETHUSDT";
            }
            &MarketType::XRPUSD => {
                return &"XRPUSD";
            }
            &MarketType::XRPUSDT => {
                return &"XRPUSDT";
            }
            &MarketType::SOLUSDT => {
                return &"SOLUSDT";
            }
            &MarketType::ERROR => {
                println!("MarketType ERROR");
                return &"ERROR";
            }
        }
    }

    pub fn size_in_btc(&self) -> bool {
        match self {
            &MarketType::BTCUSD | &MarketType::ETHUSD | &MarketType::XRPUSD => {
                return false;
            }
            &MarketType::BTCUSDT
            | &MarketType::ETHUSDT
            | &MarketType::XRPUSDT
            | &MarketType::SOLUSDT => {
                return true;
            }
            &MarketType::ERROR => {
                println!("MarketType ERROR");
                return false;
            }
        }
    }
}



#[test]
fn test_market_type() {
    assert_eq!(MarketType::BTCUSD.to_str(), "BTCUSD");
    assert_eq!(MarketType::BTCUSDT.to_str(), "BTCUSDT");
}

pub fn log_file_dir() -> Option<ProjectDirs> {
    ProjectDirs::from("net", "takibi", "rusty-exchange")
}

// Create or return log directory path
// default "~/BBLOG/" will be used.
// TODO: if environment variable "BB_LOG_DIR" set, that will be used.

pub fn log_file_path(market_type: &MarketType, yyyy: i32, mm: i32, dd: i32) -> String {
    if let Some(base_path) = log_file_dir() {
        let data_dir = base_path
            .data_dir()
            .join("BBLOG")
            .join(market_type.to_str());
        let full_path = data_dir.join(bb_log_file_name(market_type, yyyy, mm, dd));

        fs::create_dir_all(data_dir).unwrap(); // TODO: need error handling?

        match full_path.to_str() {
            None => {
                return "".to_string();
            }
            Some(p) => return p.to_string(),
        }
    }

    return "".to_string();
}

fn log_download_url(market_type: &MarketType, yyyy: i32, mm: i32, dd: i32) -> String {
    let file_name = bb_log_file_name(market_type, yyyy, mm, dd);

    return format!(
        "https://public.bybit.com/trading/{}/{}",
        market_type.to_str(),
        file_name
    );
}

fn bb_log_file_name(market_type: &MarketType, yyyy: i32, mm: i32, dd: i32) -> String {
    return format!(
        "{}{:04}-{:02}-{:02}.csv.gz",
        market_type.to_str(),
        yyyy,
        mm,
        dd
    );
}

// Download log file
// Download Log file from bybit archive specified date(YYYY, MM, DD)
//
async fn download_exec_logfile(
    market_type: &MarketType,
    yyyy: i32,
    mm: i32,
    dd: i32,
) -> BbResult<()> {
    let dest_file = log_file_path(market_type, yyyy, mm, dd);
    if dest_file == "" {
        panic!("cannot open file");
    }

    let url = log_download_url(market_type, yyyy, mm, dd);

    fetch_url(url, dest_file).await.unwrap(); // TODO: error handling

    return Ok(());
}

async fn open_exec_log_file(market_type: &MarketType, yyyy: i32, mm: i32, dd: i32) -> File {
    let path_name = log_file_path(market_type, yyyy, mm, dd);

    match File::open(&path_name) {
        Ok(f) => {
            return f;
        }
        Err(e) => {
            // println!("try download");
        }
    }

    download_exec_logfile(market_type, yyyy, mm, dd).await;
    return File::open(&path_name).expect("open error");
}

#[test]
fn test_list_cache_files() {
    if let Some(base_path) = log_file_dir() {
        let data_dir = base_path.data_dir().join("BBLOG").join("BTCUSD");
        let paths = fs::read_dir(data_dir).unwrap();

        for path in paths {
            let path = path.unwrap();
            let name = path.path();
            let extension = name.extension().unwrap();

            println!("{}", extension.to_str().unwrap());
            if extension == "gz" {
                fs::remove_file(name.as_path()).unwrap();
            }
        }
    }
}

// use reqwest::Client;
use std::io::Cursor;

// TODO: when 404 returns, make error or ignore.
async fn fetch_url(url: String, file_name: String) -> BbResult<()> {
    let response = reqwest::get(url).await?;

    if response.status().is_success() {
        let mut file = std::fs::File::create(file_name)?;
        let mut content = Cursor::new(response.bytes().await?);
        std::io::copy(&mut content, &mut file)?;
        Ok(())
    } else {
        Ok(()) // TODO: should be Err
    }
}

// use std::io::{stdout, Write};

#[tokio::test]
async fn test_download_log_file() -> BbResult<()> {
    return download_exec_logfile(&MarketType::BTCUSD, 2022, 06, 01).await;
}

#[tokio::test]
async fn test_open_exec_log_file() -> BbResult<()> {
    let f = open_exec_log_file(&MarketType::BTCUSD, 2022, 5, 3).await;

    return Ok(());
}

use crate::bb::message;
use crate::exchange::Market;
use crate::exchange::Trade;

//
// yyyy mm dd で指定されたログファイルをダウンロードする。
// その後コールバック関数を用いて、Maketクラスへデータをロードする。
//
pub async fn load_log_file(
    market_type: &MarketType,
    yyyy: i32,
    mm: i32,
    dd: i32,
    callback: fn(m: &mut Market, t: &Trade),
    market: &mut Market,
) {
    let f = open_exec_log_file(market_type, yyyy, mm, dd).await;

    let buf_read = std::io::BufReader::new(f);
    let gzip_reader = std::io::BufReader::new(GzDecoder::new(buf_read)).lines();

    for (i, l) in gzip_reader.enumerate() {
        // first line is a header
        if i == 0 {
            // check if the header follows the format
        } else {
            let row = l.unwrap();

            match message::parse_log_rec(&row) {
                Ok(trade) => {
                    callback(market, &trade);
                }
                Err(_) => {
                    println!("log load error");
                }
            }
        }
    }
    market.flush_add_trade();
}

#[cfg(test)]
use chrono::{Datelike, Duration, Utc};

#[cfg(test)]
use crate::bb::testdata::CSVDATA;

#[cfg(test)]
pub fn load_dummy_data() -> Market {
    fn insert_callback(m: &mut Market, t: &Trade) {
        m.append_trade(t);
    }

    let mut market = Market::new();

    let data: String = CSVDATA.to_string();

    for (i, l) in data.lines().enumerate() {
        // first line is a header
        if i == 0 {
            // check if the header follows the format
        } else {
            let row = l;

            match message::parse_log_rec(&row) {
                Ok(trade) => {
                    insert_callback(&mut market, &trade);
                }
                Err(_) => {
                    println!("log load error {}", row);
                }
            }
        }
    }
    market.flush_add_trade();

    return market;
}

#[test]
fn test_log_file_path_operations() {
    // assert_eq!(log_file_path(2022, 6, 2), "~/BBLOG/BTCUSD/BTCUSD2022-06-02.csv.gz");
    println!("log_dir={}", log_file_path(&MarketType::BTCUSD, 2022, 6, 2));

    assert_eq!(
        log_download_url(&MarketType::BTCUSD, 2022, 6, 3),
        "https://public.bybit.com/trading/BTCUSD/BTCUSD2022-06-03.csv.gz"
    );
    println!(
        "log url={}",
        log_download_url(&MarketType::BTCUSD, 2022, 6, 3)
    );

    assert_eq!(
        bb_log_file_name(&MarketType::BTCUSD, 2022, 6, 2),
        "BTCUSD2022-06-02.csv.gz"
    );
    println!(
        "log filename ={}",
        bb_log_file_name(&MarketType::BTCUSD, 2022, 6, 2)
    );
}

#[test]
fn test_ndays() {
    let last_day = Utc::now() - Duration::days(1);

    println!(
        "{} {}-{}-{}",
        last_day,
        last_day.year(),
        last_day.month(),
        last_day.day()
    );

    let days = 10;

    for i in (0..days).rev() {
        let log_date = last_day - Duration::days(i);
        let year = log_date.year();
        let month = log_date.month() as i32;
        let day = log_date.day() as i32;

        println!("{} {} {}", year, month, day);
    }
}
