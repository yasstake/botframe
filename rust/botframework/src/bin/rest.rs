// Copyright(c) yasstake 2022. All rights reserved. (no warranty)

/*
use reqwest::{blocking::Response, header};
use crate::error::{Error, Result};
// use std::collections::BTreeMap;


fn http_get(url: &str) -> Result<Response> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );

    let client = reqwest::blocking::Client::builder()
         .default_headers(headers)
         .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
         .gzip(true)
         .build()?;
    let response = client.get(full_url.as_str()).send()?;
    Ok(response)
}

*/

/*
// Returns the raw response directly.
pub(super) fn http_get_raw(url: &str, params: &BTreeMap<String, String>) -> Result<Response> {
    let mut full_url = url.to_string();
    let mut first = true;
    for (k, v) in params.iter() {
        if first {
            full_url.push_str(format!("?{}={}", k, v).as_str());
            first = false;
        } else {
            full_url.push_str(format!("&{}={}", k, v).as_str());
        }
    }
    // println!("{}", full_url);

    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );

    let client = reqwest::blocking::Client::builder()
         .default_headers(headers)
         .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
         .gzip(true)
         .build()?;
    let response = client.get(full_url.as_str()).send()?;
    Ok(response)
}

// Returns the text in response.
pub(super) fn http_get(url: &str, params: &BTreeMap<String, String>) -> Result<String> {
    match http_get_raw(url, params) {
        Ok(response) => match response.error_for_status() {
            Ok(resp) => Ok(resp.text()?),
            Err(error) => Err(Error::from(error)),
        },
        Err(err) => Err(err),
    }
}

macro_rules! gen_api {
    ( $path:expr$(, $param_name:ident )* ) => {
        {
            #[allow(unused_mut)]
            let mut params = BTreeMap::new();
            $(
                if let Some(param_name) = $param_name {
                    params.insert(stringify!($param_name).to_string(), param_name.to_string());
                }
            )*
            let url = if $path.starts_with("http") { $path.to_string() } else { format!("{}{}",BASE_URL, $path) };
            http_get(&url, &params)
        }
    }
}
*/

fn main() {}
