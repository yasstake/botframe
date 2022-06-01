

use std::fs::File;
use reqwest;
use std::io::{self, Read};

/*
fn run() -> Result<()> {
    let target = "https://www.rust-lang.org/logos/rust-logo-512x512.png";
    let mut response = reqwest::get(target)?;

    let mut dest = {
        let fname = "/tmp/response.html";
        println!("will be located under: '{:?}'", fname);
        File::create(fname)?
    };

    copy(&mut response, &mut dest)?;

    Ok(())
}
*/

const BTCUSD: &str = "BTCUSD";

#[test]
fn test_log_file_name() {
    let file_name = log_file_name("BTCUSD", 2022, 1, 31);

    assert_eq!(file_name, "BTCUSD2022-01-31.csv.gz")
}


fn log_file_name(symbol: &str, yyyy: i32, mm: i32, dd: i32) -> String {
    format!("{}{:04}-{:02}-{:02}.csv.gz", symbol, yyyy, mm, dd)
}

#[test]
fn test_make_url(){
    assert_eq!(download_url(2022, 05, 02), "https://public.bybit.com/trading/BTCUSD/BTCUSD2022-05-02.csv.gz");
}

fn download_url(yyyy: i32, mm: i32, dd: i32) -> String {
    let base_url = "https://public.bybit.com/trading";    
    let symbol = BTCUSD;

    let file_name =  log_file_name(symbol, yyyy, mm, dd);

    return format!("{}/{}/{}", base_url, symbol, file_name);
}

#[test]
fn test_temp_dir() {
    let dir = temp_dir();

    assert_eq!(dir, "/tmp/");
}


// TODO: hard coding for tmp directory
fn temp_dir() -> String {
    return String::from("/tmp/");
}

#[test]
fn test_download_log() {

}

fn download_log(yyyy: i32, mm: i32, dd: i32) -> io::Result<()> {
    let url = download_url(yyyy, mm, dd);
    let symbol = BTCUSD;

    let store_file = format!("{}/{}", temp_dir(), log_file_name(symbol, yyyy, mm, dd));

    Ok(())
}


/* 

fn download_log(yyyy: i32, mm: i32, dd: i32) -> io::Result<()> {
    let target_url = "http://www.yahoo.co.jp/";
    let mut response = reqwest::blocking::get(target_url).expect("");

    let mut target = File::create("/tmp/test.html")?;

    copy(&mut response, &mut target)?;

    Ok(())
}

use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let resp = reqwest::blocking::get("https://httpbin.org/ip")?
        .json::<HashMap<String, String>>()?;
    println!("{:#?}", resp);
    Ok(())
}

*/



use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let resp = reqwest::get("https://httpbin.org/ip").await?;

    let mut reader = resp.text().await?;
    //let mut reader: &[u8] = b"hello";
    let mut writer = File::create("/tmp/test.html")?;

    io::copy(&mut reader.as_bytes(), & mut writer)?;
    println!("{:#?}", reader);
    Ok(())
}
