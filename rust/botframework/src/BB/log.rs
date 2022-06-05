use std::env;
use std::fs;

use flate2::bufread::GzDecoder;
use futures_util::io::BufWriter;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use std::io;

use directories::ProjectDirs;

use anyhow::Context;
use std::io::copy;
use thiserror::Error;

pub type BbError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type BbResult<T> = Result<T, BbError>;


fn log_file_dir() -> Option<ProjectDirs> {
    ProjectDirs::from("net", "takibi", "rusty-exchange")
}

// Create or return log directory path
// default "~/BBLOG/" will be used.
// TODO: if environment variable "BB_LOG_DIR" set, that will be used.

fn log_file_path(yyyy: i32, mm: i32, dd: i32) -> String {
    if let  Some(base_path) = log_file_dir() {
        let data_dir = base_path.data_dir().join("BBLOG").join("BTCUSD");
        let full_path = data_dir.join(bb_log_file_name(yyyy, mm, dd));
        fs::create_dir_all(data_dir);

        match full_path.to_str() {
            None => {
                return "".to_string();
            }
            Some(p) => return p.to_string(),
        }
    }

    return "".to_string();
}

fn log_download_url(yyyy: i32, mm: i32, dd: i32) -> String {
    let file_name = bb_log_file_name(yyyy, mm, dd);

    return format!("https://public.bybit.com/trading/BTCUSD/{}", file_name);
}

fn bb_log_file_name(yyyy: i32, mm: i32, dd: i32) -> String {
    return format!("BTCUSD{:04}-{:02}-{:02}.csv.gz", yyyy, mm, dd);
}

#[test]
fn test_log_file_path_operations() {
    // assert_eq!(log_file_path(2022, 6, 2), "~/BBLOG/BTCUSD/BTCUSD2022-06-02.csv.gz");
    println!("log_dir={}", log_file_path(2022, 6, 2));

    assert_eq!(
        log_download_url(2022, 6, 3),
        "https://public.bybit.com/trading/BTCUSD/BTCUSD2022-06-03.csv.gz"
    );
    println!("log url={}", log_download_url(2022, 6, 3));

    assert_eq!(bb_log_file_name(2022, 6, 2), "BTCUSD2022-06-02.csv.gz");
    println!("log filename ={}", bb_log_file_name(2022, 6, 2));
}

// Download log file
// Download Log file from bybit archive specified date(YYYY, MM, DD)
//
async fn download_exec_logfile(yyyy: i32, mm: i32, dd: i32) -> BbResult<()> {
    let dest_file = log_file_path(yyyy, mm, dd);
    if dest_file == "" {
        panic!("cannot open file");
    }

    let url = log_download_url(yyyy, mm, dd);

    fetch_url(url, dest_file).await;

    return Ok(());
}

async fn open_exec_log_file(yyyy: i32, mm: i32, dd: i32) -> File {
    let path_name = log_file_path(yyyy, mm, dd);

    match File::open(&path_name) {
        Ok(f) => {
            return f;
        }
        Err(e) => {
            download_exec_logfile(yyyy, mm, dd).await;

            return File::open(&path_name).expect("open error");
        }
    }
}



/* 
fn list_log_cache_files() -> vec![str] {



}
*/



#[test]
fn test_list_cache_files() {
    if let  Some(base_path) = log_file_dir() {
        let data_dir = base_path.data_dir().join("BBLOG").join("BTCUSD");
        let paths = fs::read_dir(data_dir).unwrap();

        for path in paths {
            // println!("directory -> {}", path.unwrap().path().display());
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



use reqwest::Client;
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

use curl::easy::Easy;
use std::io::{stdout, Write};

#[tokio::main]
#[test]
async fn test_download_log_file() -> BbResult<()> {
    return download_exec_logfile(2022, 06, 01).await;
}

#[tokio::main]
#[test]
async fn test_open_exec_log_file() -> BbResult<()> {
    let f = open_exec_log_file(2022, 5, 3).await;

    return Ok(());
}

use crate::bb::message;
use crate::exchange::Market;
use crate::exchange::Trade;

// 
//
//
//
pub async fn load_log_file(
    yyyy: i32,
    mm: i32,
    dd: i32,
    callback: fn(m: &mut Market, t: Trade),
    market: &mut Market,
) {
    let f = open_exec_log_file(yyyy, mm, dd).await;

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
                    callback(market, trade);
                }
                Err(_) => {
                    println!("log load error");
                }
            }
        }
    }
}

/*
#[tokio::main]
#[test]
async fn load_file() {
    fn nothing_callback(t:Trade){println!("{} {} {} {}",t.time_ns, t.bs, t.price, t.size)}

    load_log_file(2022, 6, 4, nothing_callback).await;
}

*/

/*
    // --snip--
    let filename = "./TESTDATA/BTCUSD2022-05-02.csv.gz";

    let path = Path::new(filename);

    let f = File::open(path).unwrap();
    let mut buf_read = std::io::BufReader::new(f);
    let mut gzip_reader = std::io::BufReader::new(GzDecoder::new(buf_read)).lines();

    for l in gzip_reader {
        let ln = l.unwrap();

        println!("{}", ln)
    }
}
*/
