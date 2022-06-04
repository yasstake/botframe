


use std::env;
use std::fs;

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use flate2::bufread::GzDecoder;
use futures_util::io::BufWriter;

use std::io;

use directories::{ProjectDirs};

use anyhow::Context;
use thiserror::Error;
use std::io::copy;


pub type BbError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type BbResult<T> = Result<T, BbError>;


// Create or return log directory path
// default "~/BBLOG/" will be used.
// TODO: if environment variable "BB_LOG_DIR" set, that will be used.


fn log_file_path(yyyy: i32, mm: i32, dd:i32) -> String {
    if let Some(base_path) = ProjectDirs::from("net", "takibi", "rusty-exchange"){
        let data_dir = base_path.data_dir().join("BBLOG").join("BTCUSD");
        let full_path = data_dir.join(bb_log_file_name(yyyy, mm, dd));
        fs::create_dir_all(data_dir);

        match full_path.to_str() {
            None => {return "".to_string();},
            Some(p) => {return p.to_string()}
        }

    }

    return "".to_string();
}

fn log_download_url(yyyy: i32, mm: i32, dd:i32) -> String {
    let file_name = bb_log_file_name(yyyy, mm, dd);    

    return format!("https://public.bybit.com/trading/BTCUSD/{}", file_name);    
}

fn bb_log_file_name(yyyy: i32, mm: i32, dd:i32) -> String {
    return format!("BTCUSD{:04}-{:02}-{:02}.csv.gz", yyyy, mm, dd);
}

#[test]
fn test_log_file_path_operations() {
    // assert_eq!(log_file_path(2022, 6, 2), "~/BBLOG/BTCUSD/BTCUSD2022-06-02.csv.gz");
    println!("log_dir={}", log_file_path(2022, 6, 2));

    assert_eq!(log_download_url(2022, 6, 3), "https://public.bybit.com/trading/BTCUSD/BTCUSD2022-06-03.csv.gz");
    println!("log url={}", log_download_url(2022, 6, 3));

    assert_eq!(bb_log_file_name(2022, 6, 2), "BTCUSD2022-06-02.csv.gz");    
    println!("log filename ={}", bb_log_file_name(2022, 6, 2));
}



// Download log file
// Download Log file from bybit archive specified date(YYYY, MM, DD)
// 
async fn download_exec_logfile(yyyy: i32, mm: i32, dd:i32) -> BbResult<()> {
    let dest_file = log_file_path(yyyy, mm, dd);
    if dest_file == "" {
        panic!("cannot open file");
    }

    let url = log_download_url(yyyy, mm, dd);

    fetch_url(url,dest_file).await;            

    return Ok(());
}

async fn open_exec_log_file(yyyy: i32, mm: i32, dd:i32) -> File {
    let path_name = log_file_path(yyyy, mm, dd);

    match File::open(&path_name){
        Ok(f) => {
            return f;
        },
        Err(e) => {
            download_exec_logfile(yyyy, mm, dd).await;

            return File::open(&path_name).expect("open error");
        }
    }
}        

use reqwest::Client;
use std::io::Cursor;

async fn fetch_url(url: String, file_name: String) -> BbResult<()> {
    let response = reqwest::get(url).await?;
    let mut file = std::fs::File::create(file_name)?;
    let mut content =  Cursor::new(response.bytes().await?);
    std::io::copy(&mut content, &mut file)?;
    Ok(())
}

use std::io::{stdout, Write};
use curl::easy::Easy;

#[tokio::main]
#[test]
async fn test_download_log_file() -> BbResult<()> {
    return download_exec_logfile(2022, 06, 01).await;
}

#[tokio::main]
#[test]
async fn test_open_exec_log_file() -> BbResult<()> {
    let f = open_exec_log_file(2022, 5, 3).await;

    return Ok(())
}




#[test]
fn load_file() {
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


