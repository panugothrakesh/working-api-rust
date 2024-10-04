mod api;
mod db;
mod models;
mod server;

use db::connect_db;
use api::fetch_depth_history;
use dotenv::dotenv;
use server::start_server;

#[tokio::main]
async fn main() {
    dotenv().ok(); // Load environment variables

    // Connect to the PostgreSQL database
    match connect_db().await {
        Ok(client) => {
            // Fetch data from the Midgard API
            if let Err(e) = fetch_depth_history(&client).await {
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