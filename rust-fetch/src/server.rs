use axum::{
    extract::Query,
    Json, Router, routing::get,
};
use std::net::SocketAddr;
use crate::db::connect_db; // Assuming you have a module for database connection
use crate::models::Interval; // Ensure you have the Interval struct defined in your models
use serde_json::json;
use serde::Deserialize;
use chrono::{NaiveDate, NaiveDateTime, Duration}; // For date parsing
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct QueryParams {
    from: Option<String>,  // e.g., "02-10-2024"
    to: Option<String>,    // e.g., "04-10-2024"
    liquidity_gt: Option<i64>,    // e.g., 100000
    order: Option<String>,          // e.g., "asc" or "desc"
    page: Option<i64>,              // e.g., 2
    limit: Option<i64>,             // e.g., 400
    interval: Option<String>,       // e.g., "day", "week", "month", "6months", "year"
}

// Function to start the server
pub async fn start_server() {
    let app = Router::new()
        .route("/depth-history", get(get_depth_history)); // Define the route

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Server running at http://{}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// Aggregation function for fixed intervals (e.g., days, weeks, months)
fn aggregate_by_interval(
    data: Vec<Interval>,
    interval_duration: Duration,   // The interval duration, e.g., 1 day, 7 days (for weeks)
    limit: usize                   // The maximum number of aggregated results
) -> Vec<Interval> {
    let mut aggregated_data: Vec<Interval> = Vec::new();
    let mut current_agg: Option<Interval> = None;
    let mut output_count = 0;
    let mut start_of_interval: Option<NaiveDateTime> = None;

    for entry in data {
        let start_time = NaiveDateTime::from_timestamp(entry.start_time, 0);

        if let Some(start_interval) = start_of_interval {
            if start_time - start_interval < interval_duration {
                // If the current entry is within the same interval, aggregate it
                if let Some(ref mut agg) = current_agg {
                    agg.asset_depth += entry.asset_depth;
                    agg.asset_price += entry.asset_price;
                    agg.asset_price_usd += entry.asset_price_usd;
                    agg.liquidity_units += entry.liquidity_units;
                    agg.luvi += entry.luvi;
                    agg.members_count += entry.members_count;
                    agg.rune_depth += entry.rune_depth;
                    agg.synth_supply += entry.synth_supply;
                    agg.synth_units += entry.synth_units;
                    agg.units += entry.units;
                    agg.end_time = entry.end_time.max(agg.end_time);
                }
            } else {
                // Push the current aggregation and start a new interval
                if let Some(agg) = current_agg.take() {
                    aggregated_data.push(agg);
                    output_count += 1;

                    // Stop if we have reached the limit
                    if output_count >= limit {
                        break;
                    }
                }
                start_of_interval = Some(start_time);
                current_agg = Some(entry.clone());
            }
        } else {
            // Initialize the first interval
            start_of_interval = Some(start_time);
            current_agg = Some(entry.clone());
        }
    }

    // Push the last aggregation if it exists
    if let Some(agg) = current_agg {
        aggregated_data.push(agg);
    }

    aggregated_data
}

// Handler to query and return depth history data
async fn get_depth_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match connect_db().await {
        Ok(client) => {
            // Build the SQL query based on the provided query parameters
            let mut query = String::from("SELECT asset_depth, asset_price, asset_price_usd, end_time, \
                        liquidity_units, luvi, members_count, rune_depth, start_time, \
                        synth_supply, synth_units, units FROM depth_history");

            let mut filters = Vec::new();

            // Handle date filtering (from and to)
            if let Some(from_date) = &params.from {
                match from_date.parse::<i64>() {
                    Ok(from_timestamp) => {
                        filters.push(format!("start_time >= {}", from_timestamp));
                    },
                    Err(e) => return Json(json!({ "error": e.to_string() })), // Return error as string
                }
            }

            if let Some(to_date) = &params.to {
                match to_date.parse::<i64>() {
                    Ok(to_timestamp) => {
                        filters.push(format!("start_time <= {}", to_timestamp));
                    },
                    Err(e) => return Json(json!({ "error": e.to_string() })), // Return error as string
                }
            }

            // Handle liquidity filter
            if let Some(liquidity_gt) = params.liquidity_gt {
                filters.push(format!("liquidity_units > {}", liquidity_gt));
            }

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            // Sorting
            query.push_str(" ORDER BY start_time ASC");

            // If the interval is provided, fetch all rows first for proper aggregation
            if let Some(interval) = &params.interval {
                let rows = client.query(&query, &[]).await.unwrap();

                let mut intervals: Vec<Interval> = rows.iter().map(|row| {
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

                let limit = params.limit.unwrap_or(400);

                // Aggregate based on the provided interval
                intervals = match interval.as_str() {
                    "day" => aggregate_by_interval(intervals, Duration::days(1), limit as usize),
                    "week" => aggregate_by_interval(intervals, Duration::weeks(1), limit as usize),
                    "month" => aggregate_by_interval(intervals, Duration::days(30), limit as usize),
                    "6months" => aggregate_by_interval(intervals, Duration::days(180), limit as usize),
                    "year" => aggregate_by_interval(intervals, Duration::days(365), limit as usize),
                    _ => intervals,
                };

                // Sorting based on the order parameter (asc or desc)
                let order = params.order.as_deref().unwrap_or("asc");
                intervals.sort_by(|a, b| {
                    if order == "asc" {
                        a.end_time.cmp(&b.end_time)
                    } else {
                        b.end_time.cmp(&a.end_time)
                    }
                });

                // Return the final JSON response with aggregated intervals
                Json(json!({ "data": intervals }))
            } else {
                // If no interval is provided, apply the raw limit directly and fetch limited rows
                let limit = params.limit.unwrap_or(400);
                query.push_str(&format!(" LIMIT {}", limit));

                let rows = client.query(&query, &[]).await.unwrap();

                let data: Vec<Interval> = rows.iter().map(|row| {
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

                // Return the final JSON response with raw data
                Json(json!({ "data": data }))
            }
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}