#![feature(iter_next_chunk)]

#![warn(clippy::nursery)]

use std::net::{IpAddr, Ipv6Addr};
use std::str::FromStr;
use rquest::StatusCode;
use rquest_util::Emulation;
use serde_json::json;
use std::time::Duration;

// follow first three steps of https://github.com/zu1k/http-proxy-ipv6-pool
// then change this to be your IPv6 prefix
const V6_PREFIX: &str = "dead:beef:1333:3337::";
const V6_SIZE: usize = 64;

pub fn random_ipv6() -> IpAddr {
    let ipv6 = Ipv6Addr::from_str(V6_PREFIX).unwrap().to_bits();
    let rand_bits = fastrand::u128(..);
    let rand_ipv6 = ipv6 | rand_bits >> V6_SIZE;
    IpAddr::V6(Ipv6Addr::from_bits(rand_ipv6))
}

const MULTI_SEARCH: &str = "https://search.kick.com/multi_search";
const MAX_ID: usize = 75000000; // idk
const THREADS: usize = 20;

#[tokio::main]
async fn main() {
    println!("id,slug,username");
    let mut handles = vec![];
    let per_thread = MAX_ID / THREADS;
    for i in 0..THREADS {
        let offset = i * per_thread;
        handles.push(tokio::spawn(scraper(offset, per_thread)));
    }

    for h in handles {
        let _ = h.await;
    }
}

const MAX_PER_SEARCH: usize = 1;
const MAX_SEARCHES: usize = 100;
const PER_REQUEST: usize = MAX_PER_SEARCH * MAX_SEARCHES;
async fn scraper(offset: usize, size: usize) {
    let mut c = rquest::ClientBuilder::new()
        .emulation(Emulation::Firefox133)
        .build()
        .unwrap();
    let mut r = offset..offset + size;
    while let Ok(win) = r.next_chunk::<PER_REQUEST>() {
        let mut searches = vec![];
        for ids in win.chunks(MAX_SEARCHES) {
            let filter_by = ids
                .iter()
                .map(|n| format!("id:{n}"))
                .collect::<Vec<String>>()
                .join("||");
            searches.push(
                json!({"collection":"channel", "q": "*", "filter_by": filter_by, "per_page": 50}),
            );
        }
        let search_json = json!({"searches": searches});
        let mut ok = false;
        while !ok {
            let res = match c
                .post(MULTI_SEARCH)
                .local_address(random_ipv6())
                .header("x-typesense-api-key", "FPQiWWORRCaFWpoaT3Tby0eiYWZtBQeB")
                .timeout(Duration::from_secs(5))
                .json(&search_json)
                .send()
                .await
            {
                Ok(res) => res,
                Err(e) => {
                    eprintln!("err {e}");
                    continue;
                }
            };

            if res.status() != StatusCode::OK {
                c = rquest::ClientBuilder::new()
                    .emulation(Emulation::Firefox133)
                    .build()
                    .unwrap();
                eprintln!(
                    "status {:?} text {}",
                    res.status(),
                    res.text().await.unwrap()
                );
                continue;
            }

            let Ok(j) = res.json::<serde_json::Value>().await else {
                continue;
            };
            for result in j["results"].as_array().unwrap() {
                for hit in result["hits"].as_array().unwrap() {
                    let id = hit["document"]["id"].as_str().unwrap_or_default();
                    let slug = hit["document"]["slug"].as_str().unwrap_or_default();
                    let username = hit["document"]["username"].as_str().unwrap_or_default();
                    println!("{id},{slug},{username}")
                }
            }
            ok = true;
        }
    }
}
