use reqwest::Error;
use crate::models::{DepthHistoryResponse, RunePoolHistoryResponse, SwapHistoryResponse, EarningsHistoryResponse};  
use crate::db::{insert_depth_history, insert_rune_pool_history, insert_swap_history, insert_earnings_history};  
use chrono::{Utc};  
use tokio::time::{sleep, Duration as TokioDuration}; 

// Function to fetch Depth History
pub async fn fetch_depth_history(client: &tokio_postgres::Client, mut from_timestamp: i64) -> Result<DepthHistoryResponse, Error> {
    let now = Utc::now();
    let current_timestamp = now.timestamp(); 
    let pool = "BTC.BTC"; 
    let interval = "hour"; 

    let mut all_intervals = Vec::new();  // To store all fetched intervals

    loop {
        let midgard_api_url = format!(
            "https://midgard.ninerealms.com/v2/history/depths/{}?interval={}&from={}&count=400",
            pool,
            interval,
            from_timestamp
        );

        println!("Fetching depth history data from: {}", midgard_api_url);

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

// Function to fetch Rune Pool History
pub async fn fetch_rune_pool_history(client: &tokio_postgres::Client, mut from_timestamp: i64) -> Result<RunePoolHistoryResponse, Error> {
    let now = Utc::now();
    let current_timestamp = now.timestamp(); 
    let interval = "hour"; 

    let mut all_intervals = Vec::new();  // To store all fetched intervals

    loop {
        let midgard_api_url = format!(
            "https://midgard.ninerealms.com/v2/history/runepool?interval={}&from={}&count=400",
            interval,
            from_timestamp
        );

        println!("Fetching rune pool history data from: {}", midgard_api_url);

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

        let rune_pool_history_response: RunePoolHistoryResponse = response.json().await?;

        if rune_pool_history_response.intervals.is_empty() {
            println!("No new intervals fetched. Stopping fetch.");
            break;
        }

        // Insert the fetched intervals into the database
        match insert_rune_pool_history(client, &rune_pool_history_response.intervals).await {
            Ok(_) => println!("Inserted fetched rune pool intervals into the database."),
            Err(e) => eprintln!("Failed to insert rune pool intervals into the database: {}", e),
        }

        // Collect all intervals for later use
        all_intervals.extend(rune_pool_history_response.intervals.clone());

        if let Some(last_interval) = rune_pool_history_response.intervals.last() {
            // Update from_timestamp based on last_interval.end_time
            from_timestamp = last_interval.end_time + 1; 
        }

        if from_timestamp >= current_timestamp {
            println!("Reached the current date. Stopping fetch.");
            break; 
        }
    }

    // Return the collected intervals wrapped in RunePoolHistoryResponse
    Ok(RunePoolHistoryResponse { intervals: all_intervals })
}

// Function to fetch Swap History
pub async fn fetch_swap_history(client: &tokio_postgres::Client, mut from_timestamp: i64) -> Result<SwapHistoryResponse, Error> {
    let now = Utc::now();
    let current_timestamp = now.timestamp(); 
    let interval = "hour"; 

    let mut all_intervals = Vec::new();  // To store all fetched intervals

    loop {
        let midgard_api_url = format!(
            "https://midgard.ninerealms.com/v2/history/swaps?interval={}&from={}&count=400",
            interval,
            from_timestamp
        );

        println!("Fetching swap history data from: {}", midgard_api_url);

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

        let swap_history_response: SwapHistoryResponse = response.json().await?;

        if swap_history_response.intervals.is_empty() {
            println!("No new intervals fetched. Stopping fetch.");
            break;
        }

        // Insert the fetched intervals into the database
        match insert_swap_history(client, &swap_history_response.intervals).await {
            Ok(_) => println!("Inserted fetched swap intervals into the database."),
            Err(e) => eprintln!("Failed to insert swap intervals into the database: {}", e),
        }

        // Collect all intervals for later use
        all_intervals.extend(swap_history_response.intervals.clone());

        if let Some(last_interval) = swap_history_response.intervals.last() {
            // Update from_timestamp based on last_interval.end_time
            from_timestamp = last_interval.end_time + 1; 
        }

        if from_timestamp >= current_timestamp {
            println!("Reached the current date. Stopping fetch.");
            break; 
        }
    }

    // Return the collected intervals wrapped in SwapHistoryResponse
    Ok(SwapHistoryResponse { intervals: all_intervals })
}

// Function to fetch Earnings History
pub async fn fetch_earnings_history(client: &tokio_postgres::Client, mut from_timestamp: i64) -> Result<EarningsHistoryResponse, Error> {
    let now = Utc::now();
    let current_timestamp = now.timestamp(); 
    let interval = "hour";  // Set interval as 'hour' as you're fetching hourly data

    let mut all_intervals = Vec::new();  // To store all fetched intervals

    loop {
        let midgard_api_url = format!(
            "https://midgard.ninerealms.com/v2/history/earnings?interval={}&from={}&count=400",
            interval,
            from_timestamp
        );

        println!("Fetching earnings history data from: {}", midgard_api_url);

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

        // Deserialize the response
        let earnings_history_response: EarningsHistoryResponse = response.json().await?;

        if earnings_history_response.intervals.is_empty() {
            println!("No new intervals fetched. Stopping fetch.");
            break;  // Exit the loop if no new intervals are fetched
        }

        // Log the start of database insertion for earnings
        println!("Starting to insert earnings data into the database...");

        // Insert the fetched intervals into the database
        match insert_earnings_history(client, &earnings_history_response.intervals).await {
            Ok(_) => println!("Earnings data insertion attempt completed."),
            Err(e) => eprintln!("Failed to insert earnings intervals into the database: {}", e),
        }

        // Collect all intervals for later use
        all_intervals.extend(earnings_history_response.intervals.clone());

        // Update `from_timestamp` to the last `end_time` of the fetched intervals
        if let Some(last_interval) = earnings_history_response.intervals.last() {
            from_timestamp = last_interval.end_time + 1;  // Start from 1 second after the last interval's end_time
        }

        // Stop fetching once we reach the current timestamp
        if from_timestamp >= current_timestamp {
            println!("Reached the current date. Stopping fetch.");
            break; 
        }

        // Log the next fetch round
        println!("Fetching next batch of earnings data starting from: {}", from_timestamp);
    }

    // Return the collected intervals wrapped in EarningsHistoryResponse
    Ok(EarningsHistoryResponse { intervals: all_intervals })
}