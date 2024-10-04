// src/main.rs

mod api;
mod db;
mod models;
mod server;
mod scheduler;

use db::{connect_db, get_last_timestamp}; // Import get_last_timestamp function
use api::fetch_depth_history;
use dotenv::dotenv;
use server::start_server;

#[tokio::main]
async fn main() {
    dotenv().ok(); // Load environment variables
    scheduler::start_scheduler().await;

    // Connect to the PostgreSQL database
    match connect_db().await {
        Ok(client) => {
            // Check if there is any data in the database
            let last_timestamp = get_last_timestamp(&client).await.unwrap_or(0); // Get the last timestamp, default to 0 if none

            // Determine the from_timestamp based on the last data
            let from_timestamp = if last_timestamp == 0 {
                // If no data, fetch from the beginning (or some default starting timestamp)
                0 // You can set this to an appropriate starting timestamp
            } else {
                // If there is data, start from the last timestamp
                last_timestamp + 1 // Ensure you don't fetch duplicate data
            };

            // Fetch data from the Midgard API
            if let Err(e) = fetch_depth_history(&client, from_timestamp).await {
                eprintln!("Error fetching data: {}", e);
            }

            // Start the Axum server
            start_server().await;
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
        }
    }
}