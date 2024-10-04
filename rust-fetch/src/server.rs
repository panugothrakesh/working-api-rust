use axum::{
    routing::{get},
    Json, Router,
};
use std::net::SocketAddr;
use tokio_postgres::NoTls;
use crate::db::connect_db;
use crate::models::Interval;
use serde_json::json;

pub async fn start_server() {
    // Create the Axum router
    let app = Router::new()
        .route("/depth-history", get(get_depth_history)); // Define the route

    // Define the server address
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Server running at http://{}", addr);

    // Start the server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// Handler to query and return depth history data
async fn get_depth_history() -> Json<serde_json::Value> {
    match connect_db().await {
        Ok(client) => {
            // Query data from the database
            let query = "SELECT asset_depth, asset_price, asset_price_usd, end_time,
                        liquidity_units, luvi, members_count, rune_depth, start_time,
                        synth_supply, synth_units, units FROM depth_history";
            let rows = client.query(query, &[]).await.unwrap();

            // Convert database rows to a vector of Intervals
            let intervals: Vec<Interval> = rows.iter().map(|row| {
                Interval {
                    asset_depth: row.get("asset_depth"),
                    asset_price: row.get("asset_price"),
                    asset_price_usd: row.get("asset_price_usd"),
                    end_time: row.get("end_time"),
                    liquidity_units: row.get("liquidity_units"),
                    luvi: row.get("luvi"),
                    members_count: row.get("members_count"),
                    rune_depth: row.get("rune_depth"),
                    start_time: row.get("start_time"),
                    synth_supply: row.get("synth_supply"),
                    synth_units: row.get("synth_units"),
                    units: row.get("units"),
                }
            }).collect();

            // Return the result as JSON
            Json(json!({ "data": intervals }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}
