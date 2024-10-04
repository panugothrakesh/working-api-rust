// src/api.rs

use reqwest::Error;
use crate::models::{DepthHistoryResponse, Interval};  
use crate::db::insert_depth_history;  
use chrono::{Utc};  
use tokio::time::{sleep, Duration as TokioDuration}; 

pub async fn fetch_depth_history(client: &tokio_postgres::Client, mut from_timestamp: i64) -> Result<DepthHistoryResponse, Error> {
    let now = Utc::now();
    let current_timestamp = now.timestamp(); 
    let pool = "BTC.BTC"; 
    let interval = "hour"; 

    let mut all_intervals = Vec::new();  // To store all fetched intervals

    loop {
        let mut midgard_api_url = format!(
            "https://midgard.ninerealms.com/v2/history/depths/{}?interval={}&from={}&count=400",
            pool,
            interval,
            from_timestamp
        );

        println!("Fetching data from: {}", midgard_api_url);

        let response = reqwest::get(&midgard_api_url).await?;

        if !response.status().is_success() {
            if response.status().as_u16() == 429 {
                println!("Received status code 429 Too Many Requests. Waiting for 5 seconds...");
                sleep(TokioDuration::from_secs(5)).await; 
                continue; 
            } else {
                eprintln!("Error: Received status code {}", response.status());
                break; 
            }
        }

        let depth_history_response: DepthHistoryResponse = response.json().await?;

        if depth_history_response.intervals.is_empty() {
            println!("No new intervals fetched. Stopping fetch.");
            break;
        }

        // Insert the fetched intervals into the database
        match insert_depth_history(client, &depth_history_response.intervals).await {
            Ok(_) => println!("Inserted fetched intervals into the database."),
            Err(e) => eprintln!("Failed to insert intervals into the database: {}", e),
        }

        // Collect all intervals for later use
        all_intervals.extend(depth_history_response.intervals.clone());

        if let Some(last_interval) = depth_history_response.intervals.last() {
            // Update from_timestamp based on last_interval.end_time
            from_timestamp = last_interval.end_time + 1; 
        }

        if from_timestamp >= current_timestamp {
            println!("Reached the current date. Stopping fetch.");
            break; 
        }
    }

    // Return the collected intervals wrapped in DepthHistoryResponse
    Ok(DepthHistoryResponse { intervals: all_intervals })
}