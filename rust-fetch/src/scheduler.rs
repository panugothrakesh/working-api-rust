use chrono::{Utc, Duration, TimeZone}; // Import the necessary traits
use tokio_schedule::{every, Job};
use crate::db::{connect_db, insert_depth_history};
use crate::api::fetch_depth_history;
use std::sync::Arc;
use tokio::spawn;

pub async fn start_scheduler() {
    let client = Arc::new(connect_db().await.unwrap());

    // This job will run every hour
    let hourly_job = every(1)
        .hour() // Set the task to run every hour
        .perform({
            let client = Arc::clone(&client); // Clone the client to move into the async block
            move || {
                let client = Arc::clone(&client);
                async move {
                    println!("Running scheduled job...");

                    // Check if the database is empty
                    let empty_check_query = "SELECT COUNT(*) FROM depth_history";
                    let count: i64 = client.query_one(empty_check_query, &[])
                        .await
                        .map(|row| row.get(0))
                        .unwrap_or(0);

                    // Determine the timestamp to fetch from
                    let mut from_timestamp = 1647910800; // Start fetching from this timestamp
                    let now = Utc::now();

                    if count == 0 {
                        // Database is empty, fetch from the beginning
                        println!("Database is empty. Fetching data from the start.");
                    } else {
                        // Check the last entry's timestamp
                        let last_entry_query = "SELECT end_time FROM depth_history ORDER BY end_time DESC LIMIT 1";
                        if let Ok(rows) = client.query(last_entry_query, &[]).await {
                            if let Some(row) = rows.first() {
                                let last_end_time: i64 = row.get(0);
                                let last_entry_time = Utc.timestamp(last_end_time, 0);
                                
                                // Update from_timestamp to the last entry's end_time + 1 second
                                from_timestamp = last_end_time + 1; 

                                // If the last entry is older than 1 hour, fetch new data
                                if last_entry_time < now - Duration::hours(1) {
                                    println!("Last entry is older than 1 hour. Fetching new data.");
                                } else {
                                    println!("No need to fetch new data. Last entry is within the last hour.");
                                    return; // Exit if data is recent
                                }
                            }
                        }
                    }

                    // Fetch data from the API and insert it into the database if necessary
                    match fetch_depth_history(&client, from_timestamp).await {
                        Ok(_) => println!("Data fetched and inserted successfully."),
                        Err(e) => eprintln!("Error fetching depth history: {}", e),
                    }
                }
            }
        });

    spawn(hourly_job); // Spawn the job in the background to run every hour
}