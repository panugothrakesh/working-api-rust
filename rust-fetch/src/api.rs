use reqwest::Error;
use crate::models::DepthHistoryResponse;
use crate::db::insert_depth_history;
use chrono::{Utc, Duration};
use std::thread::sleep;
use std::time::Duration as StdDuration;

pub async fn fetch_depth_history(client: &tokio_postgres::Client) -> Result<(), Error> {
    let now = Utc::now();
    let current_timestamp = now.timestamp(); // Current timestamp
    let three_years_ago = now - Duration::days(3 * 365); // Approximation for 3 years
    let mut from_timestamp = three_years_ago.timestamp();

    let pool = "BTC.BTC"; // Define your pool here
    let interval = "hour"; // Define your interval here

    loop {
        // Construct the API URL
        let midgard_api_url = format!(
            "https://midgard.ninerealms.com/v2/history/depths/{}?interval={}&from={}&count=400",
            pool,
            interval,
            from_timestamp
        );

        println!("Fetching data from: {}", midgard_api_url);

        // Fetch the data from the API
        let response = reqwest::get(&midgard_api_url).await?;

        // Check for successful response
        if !response.status().is_success() {
            if response.status().as_u16() == 429 {
                println!("Received status code 429 Too Many Requests. Waiting for 5 seconds...");
                sleep(StdDuration::from_secs(5)); // Wait for 5 seconds before retrying
                continue; // Retry the same request
            } else {
                eprintln!("Error: Received status code {}", response.status());
                break; // Exit loop on other errors
            }
        }

        // Parse the JSON response
        let depth_history_response: DepthHistoryResponse = response.json().await?;

        // If there are no intervals returned, break out of the loop
        if depth_history_response.intervals.is_empty() {
            break;
        }

        // Insert data into the database immediately after fetching
        let intervals = depth_history_response.intervals.clone();
        if let Err(e) = insert_depth_history(client, &intervals).await {
            eprintln!("Failed to insert data into the database: {}", e);
            break; // Exit on insert failure
        } else {
            println!("Inserted {} intervals into the database successfully!", intervals.len());
        }

        // Get the last `end_time` from the current batch and set it as the next `from_timestamp`
        if let Some(last_interval) = depth_history_response.intervals.last() {
            let last_end_time: i64 = last_interval.end_time.parse().unwrap_or(0);
            from_timestamp = last_end_time + 1; // Increment to avoid overlap
        }

        // Check if the last timestamp fetched is greater than or equal to the current timestamp
        if from_timestamp >= current_timestamp {
            println!("Reached the current date. Stopping fetch.");
            break; // Stop fetching if we've reached or surpassed the current date
        }
    }

    Ok(())
}