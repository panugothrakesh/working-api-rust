use chrono::{Utc, Duration, TimeZone}; // Import the necessary traits
use tokio::time::{sleep, Duration as TokioDuration}; // For async sleep
use crate::db::{connect_db, get_last_timestamp, get_last_rune_pool_timestamp, get_last_swap_timestamp, get_last_earnings_timestamp}; // Database functions
use crate::api::{fetch_depth_history, fetch_rune_pool_history, fetch_swap_history, fetch_earnings_history}; // API fetching logic
use std::time::Instant;
use tokio::spawn;
use crate::server::start_server; // Assuming server.rs has start_server

// This function sets up the loop to run the task every hour
pub async fn start_scheduler() {
    // Run the loop indefinitely to check and fetch data every hour
    spawn(async {
        loop {
            let start_time = Instant::now();

            // Attempt to fetch the data
            if let Err(e) = check_and_fetch_data().await {
                eprintln!("Error in scheduled job: {}", e);
            }

            // Sleep for the remaining time of the hour
            let elapsed_time = Instant::now() - start_time;
            if elapsed_time < TokioDuration::from_secs(3600) {
                sleep(TokioDuration::from_secs(3600) - elapsed_time).await;
            }
        }
    });

    // Now, start the server after spawning the job
    start_server().await;
}

// Function to check and fetch depth history, rune pool history, swap history, and earnings history
async fn check_and_fetch_data() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new database client
    let client = match connect_db().await {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            return Ok(()); // Exit the function if the connection fails
        }
    };

    // Step 1: Check and fetch Depth History
    if let Err(e) = check_and_fetch_depth_history(&client).await {
        eprintln!("Error fetching depth history: {}", e);
        return Ok(()); // If depth history fails, stop and retry later
    }

    // Step 2: Check and fetch Rune Pool History
    if let Err(e) = check_and_fetch_rune_pool_history(&client).await {
        eprintln!("Error fetching rune pool history: {}", e);
    }

    // Step 3: Check and fetch Swap History
    if let Err(e) = check_and_fetch_swap_history(&client).await {
        eprintln!("Error fetching swap history: {}", e);
    }

    // Step 4: Check and fetch Earnings History
    if let Err(e) = check_and_fetch_earnings_history(&client).await {
        eprintln!("Error fetching earnings history: {}", e);
    }

    Ok(())
}

// Function to check the last entry and fetch depth history if necessary
async fn check_and_fetch_depth_history(client: &tokio_postgres::Client) -> Result<(), Box<dyn std::error::Error>> {
    // The fixed starting timestamp you want to use if the database is empty
    let from_timestamp_hardcoded = 1647910800; // Replace with your specific start timestamp

    // Check the last timestamp in the depth_history table
    let last_timestamp = get_last_timestamp(client).await.unwrap_or(0);  // If no data, return 0

    let from_timestamp = if last_timestamp == 0 {
        // If the database has no data or returns 0, use the hardcoded timestamp
        from_timestamp_hardcoded
    } else {
        last_timestamp
    };

    let now = Utc::now();

    if from_timestamp == from_timestamp_hardcoded {
        // If starting from the hardcoded timestamp, log it
        println!("Depth history database is empty. Fetching data from the provided start timestamp: {}", from_timestamp);
    } else {
        let last_entry_time = Utc.timestamp(from_timestamp, 0); // Convert to UTC time

        // Fetch only if the last entry is older than 1 hour
        if last_entry_time < now - Duration::hours(1) {
            println!("Fetching new depth history data from timestamp: {}", from_timestamp);
        } else {
            println!("No need to fetch new depth data. Last entry is within the last hour.");
            return Ok(());  // Exit if data is recent and within the last hour
        }
    }

    // Fetch and insert depth history data
    fetch_depth_history(client, from_timestamp).await?;
    println!("Depth history data fetched and inserted successfully.");

    Ok(())
}

// Function to check the last entry and fetch rune pool history if necessary
async fn check_and_fetch_rune_pool_history(client: &tokio_postgres::Client) -> Result<(), Box<dyn std::error::Error>> {
    // The fixed starting timestamp you want to use if the database is empty
    let from_timestamp_hardcoded = 1647910800; // Replace with your specific start timestamp
    
    // Check the last timestamp in the rune_pool_history table
    let last_timestamp = get_last_rune_pool_timestamp(client).await.unwrap_or(0);  // If no data, return 0

    let from_timestamp = if last_timestamp == 0 {
        // If the database has no data or returns 0, use the hardcoded timestamp
        from_timestamp_hardcoded
    } else {
        last_timestamp
    };

    let now = Utc::now();

    if from_timestamp == from_timestamp_hardcoded {
        // If starting from the hardcoded timestamp, log it
        println!("Rune pool history database is empty. Fetching data from the provided start timestamp: {}", from_timestamp);
    } else {
        let last_entry_time = Utc.timestamp(from_timestamp, 0); // Convert to UTC time

        // Fetch only if the last entry is older than 1 hour
        if last_entry_time < now - Duration::hours(1) {
            println!("Fetching new rune pool history data from timestamp: {}", from_timestamp);
        } else {
            println!("No need to fetch new rune pool data. Last entry is within the last hour.");
            return Ok(());  // Exit if data is recent and within the last hour
        }
    }

    // Fetch and insert rune pool history data
    fetch_rune_pool_history(client, from_timestamp).await?;
    println!("Rune pool history data fetched and inserted successfully.");

    Ok(())
}

// Function to check the last entry and fetch swap history if necessary
async fn check_and_fetch_swap_history(client: &tokio_postgres::Client) -> Result<(), Box<dyn std::error::Error>> {
    // The fixed starting timestamp you want to use if the database is empty
    let from_timestamp_hardcoded = 1647910800; // Replace with your specific start timestamp

    // Check the last timestamp in the swaps table
    let last_timestamp = get_last_swap_timestamp(client).await.unwrap_or(0);  // If no data, return 0

    let from_timestamp = if last_timestamp == 0 {
        // If the database has no data or returns 0, use the hardcoded timestamp
        from_timestamp_hardcoded
    } else {
        last_timestamp
    };

    let now = Utc::now();

    if from_timestamp == from_timestamp_hardcoded {
        // If starting from the hardcoded timestamp, log it
        println!("Swaps database is empty. Fetching data from the provided start timestamp: {}", from_timestamp);
    } else {
        let last_entry_time = Utc.timestamp(from_timestamp, 0); // Convert to UTC time

        // Fetch only if the last entry is older than 1 hour
        if last_entry_time < now - Duration::hours(1) {
            println!("Fetching new swap history data from timestamp: {}", from_timestamp);
        } else {
            println!("No need to fetch new swap data. Last entry is within the last hour.");
            return Ok(());  // Exit if data is recent and within the last hour
        }
    }

    // Fetch and insert swap history data
    fetch_swap_history(client, from_timestamp).await?;
    println!("Swap history data fetched and inserted successfully.");

    Ok(())
}

// Function to check the last entry and fetch earnings history if necessary
async fn check_and_fetch_earnings_history(client: &tokio_postgres::Client) -> Result<(), Box<dyn std::error::Error>> {
    // The fixed starting timestamp you want to use if the database is empty
    let from_timestamp_hardcoded = 1647910800; // Replace with your specific start timestamp

    // Check the last timestamp in the earnings_history table
    let last_timestamp = get_last_earnings_timestamp(client).await.unwrap_or(0);  // If no data, return 0

    let from_timestamp = if last_timestamp == 0 {
        // If the database has no data or returns 0, use the hardcoded timestamp
        from_timestamp_hardcoded
    } else {
        last_timestamp
    };

    let now = Utc::now();

    if from_timestamp == from_timestamp_hardcoded {
        // If starting from the hardcoded timestamp, log it
        println!("Earnings history database is empty. Fetching data from the provided start timestamp: {}", from_timestamp);
    } else {
        let last_entry_time = Utc.timestamp(from_timestamp, 0); // Convert to UTC time

        // Fetch only if the last entry is older than 1 hour
        if last_entry_time < now - Duration::hours(1) {
            println!("Fetching new earnings history data from timestamp: {}", from_timestamp);
        } else {
            println!("No need to fetch new earnings data. Last entry is within the last hour.");
            return Ok(());  // Exit if data is recent and within the last hour
        }
    }

    // Fetch and insert earnings history data
    fetch_earnings_history(client, from_timestamp).await?;
    println!("Earnings history data fetched and inserted successfully.");

    Ok(())
}