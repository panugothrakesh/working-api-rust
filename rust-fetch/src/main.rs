mod api;
mod db;
mod models;

use db::connect_db;
use api::fetch_depth_history;
use dotenv::dotenv;

#[tokio::main]
async fn main() {
    dotenv().ok(); // Load the .env file

    // Connect to the PostgreSQL database
    match connect_db().await {
        Ok(client) => {
            // Fetch data from the Midgard API
            if let Err(e) = fetch_depth_history(&client).await {
                eprintln!("Error fetching data: {}", e);
            }
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
        }
    }
}