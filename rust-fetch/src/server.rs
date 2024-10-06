use axum::{
    extract::Query,
    Json, Router, routing::get,
};
use std::net::SocketAddr;
use crate::db::connect_db;
use crate::models::{Interval, RunePoolInterval, SwapInterval}; // Ensure SwapInterval is defined
use serde_json::json;
use serde::Deserialize;
use chrono::{NaiveDate, NaiveDateTime, Duration};
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
        .route("/depth-history", get(get_depth_history))   // Route for depth history
        .route("/rune-pool-history", get(get_rune_pool_history)) // Route for rune pool history
        .route("/swap-history", get(get_swap_history)); // Route for swap history

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Server running at http://{}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// Reuse the aggregation logic for all histories (depth, rune pool, and swaps)
fn aggregate_by_interval<T>(
    data: Vec<T>,
    interval_duration: Duration,
    limit: usize,
) -> Vec<T> 
where
    T: Clone + IntervalAggregation,  // IntervalAggregation trait handles aggregation for all history types
{
    let mut aggregated_data: Vec<T> = Vec::new();
    let mut current_agg: Option<T> = None;
    let mut output_count = 0;
    let mut start_of_interval: Option<NaiveDateTime> = None;

    for entry in data {
        let start_time = NaiveDateTime::from_timestamp(entry.get_start_time(), 0);

        if let Some(start_interval) = start_of_interval {
            if start_time - start_interval < interval_duration {
                // If the current entry is within the same interval, aggregate it
                if let Some(ref mut agg) = current_agg {
                    agg.aggregate(&entry);
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

// Trait for aggregating intervals, implemented by Interval, RunePoolInterval, and SwapInterval
pub trait IntervalAggregation {
    fn get_start_time(&self) -> i64;
    fn aggregate(&mut self, other: &Self);
}

// Implement IntervalAggregation for Interval (Depth History)
impl IntervalAggregation for Interval {
    fn get_start_time(&self) -> i64 {
        self.start_time
    }

    fn aggregate(&mut self, other: &Self) {
        self.asset_depth += other.asset_depth;
        self.asset_price += other.asset_price;
        self.asset_price_usd += other.asset_price_usd;
        self.liquidity_units += other.liquidity_units;
        self.luvi += other.luvi;
        self.members_count += other.members_count;
        self.rune_depth += other.rune_depth;
        self.synth_supply += other.synth_supply;
        self.synth_units += other.synth_units;
        self.units += other.units;
        self.end_time = self.end_time.max(other.end_time);
    }
}

// Implement IntervalAggregation for RunePoolInterval
impl IntervalAggregation for RunePoolInterval {
    fn get_start_time(&self) -> i64 {
        self.start_time
    }

    fn aggregate(&mut self, other: &Self) {
        self.units += other.units;
        self.count += other.count;
        self.end_time = self.end_time.max(other.end_time);
    }
}

// Implement IntervalAggregation for SwapInterval
impl IntervalAggregation for SwapInterval {
    fn get_start_time(&self) -> i64 {
        self.start_time
    }

    fn aggregate(&mut self, other: &Self) {
        self.to_asset_volume += other.to_asset_volume;
        self.to_rune_volume += other.to_rune_volume;
        self.from_trade_volume += other.from_trade_volume;
        self.total_volume += other.total_volume;
        self.end_time = self.end_time.max(other.end_time);
    }
}

// Handler for depth history
async fn get_depth_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match connect_db().await {
        Ok(client) => {
            let mut query = String::from("SELECT asset_depth, asset_price, asset_price_usd, end_time, \
                        liquidity_units, luvi, members_count, rune_depth, start_time, \
                        synth_supply, synth_units, units FROM depth_history");

            let mut filters = Vec::new();
            build_query_filters(&mut filters, &params);

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            query.push_str(" ORDER BY start_time ASC");

            // Pagination logic
            let limit = params.limit.unwrap_or(400);
            let page = params.page.unwrap_or(1);
            let offset = (page - 1) * limit;  // Calculate the offset for pagination

            query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

            let rows = client.query(&query, &[]).await.unwrap();
            let data: Vec<Interval> = parse_rows_to_intervals(rows);

            Json(json!({
                "page": page,
                "limit": limit,
                "data": data
            }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}

// Handler for rune pool history
async fn get_rune_pool_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match connect_db().await {
        Ok(client) => {
            let mut query = String::from("SELECT units, count, start_time, end_time FROM rune_pool_history");

            let mut filters = Vec::new();
            build_query_filters(&mut filters, &params);

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            query.push_str(" ORDER BY start_time ASC");

            // Pagination logic
            let limit = params.limit.unwrap_or(400);
            let page = params.page.unwrap_or(1);
            let offset = (page - 1) * limit;

            query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

            let rows = client.query(&query, &[]).await.unwrap();
            let data: Vec<RunePoolInterval> = parse_rows_to_rune_pool_intervals(rows);

            Json(json!({
                "page": page,
                "limit": limit,
                "data": data
            }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}

// Handler for swap history
async fn get_swap_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match connect_db().await {
        Ok(client) => {
            let mut query = String::from("SELECT start_time, end_time, to_asset_count, to_rune_count, to_trade_count, \
                                          from_trade_count, synth_mint_count, synth_redeem_count, total_count, \
                                          to_asset_volume, to_rune_volume, to_trade_volume, from_trade_volume, \
                                          synth_mint_volume, synth_redeem_volume, total_volume, to_asset_volume_usd, \
                                          to_rune_volume_usd, to_trade_volume_usd, from_trade_volume_usd, \
                                          synth_mint_volume_usd, synth_redeem_volume_usd, total_volume_usd, \
                                          to_asset_fees, to_rune_fees, to_trade_fees, from_trade_fees, \
                                          synth_mint_fees, synth_redeem_fees, total_fees, to_asset_average_slip, \
                                          to_rune_average_slip, to_trade_average_slip, from_trade_average_slip, \
                                          synth_mint_average_slip, synth_redeem_average_slip, average_slip, rune_price_usd \
                                          FROM swaps");

            let mut filters = Vec::new();
            build_query_filters(&mut filters, &params);

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            query.push_str(" ORDER BY start_time ASC");

            // Pagination logic
            let limit = params.limit.unwrap_or(400);
            let page = params.page.unwrap_or(1);
            let offset = (page - 1) * limit;

            query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

            let rows = client.query(&query, &[]).await.unwrap();
            let data: Vec<SwapInterval> = parse_rows_to_swap_intervals(rows);

            Json(json!({
                "page": page,
                "limit": limit,
                "data": data
            }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}

// Utility functions
fn build_query_filters(filters: &mut Vec<String>, params: &QueryParams) {
    if let Some(from_date) = &params.from {
        match from_date.parse::<i64>() {
            Ok(from_timestamp) => filters.push(format!("start_time >= {}", from_timestamp)),
            Err(_) => {}
        }
    }

    if let Some(to_date) = &params.to {
        match to_date.parse::<i64>() {
            Ok(to_timestamp) => filters.push(format!("start_time <= {}", to_timestamp)),
            Err(_) => {}
        }
    }

    if let Some(liquidity_gt) = params.liquidity_gt {
        filters.push(format!("liquidity_units > {}", liquidity_gt));
    }
}

// Parse rows for depth history
fn parse_rows_to_intervals(rows: Vec<tokio_postgres::Row>) -> Vec<Interval> {
    rows.iter()
        .map(|row| Interval {
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
        })
        .collect()
}

// Parse rows for rune pool history
fn parse_rows_to_rune_pool_intervals(rows: Vec<tokio_postgres::Row>) -> Vec<RunePoolInterval> {
    rows.iter()
        .map(|row| RunePoolInterval {
            start_time: row.get("start_time"),
            end_time: row.get("end_time"),
            units: row.get("units"),
            count: row.get("count"),
        })
        .collect()
}

// Parse rows for swap history
fn parse_rows_to_swap_intervals(rows: Vec<tokio_postgres::Row>) -> Vec<SwapInterval> {
    rows.iter()
        .map(|row| SwapInterval {
            start_time: row.get("start_time"),
            end_time: row.get("end_time"),
            to_asset_count: row.get("to_asset_count"),
            to_rune_count: row.get("to_rune_count"),
            to_trade_count: row.get("to_trade_count"),
            from_trade_count: row.get("from_trade_count"),
            synth_mint_count: row.get("synth_mint_count"),
            synth_redeem_count: row.get("synth_redeem_count"),
            total_count: row.get("total_count"),
            to_asset_volume: row.get("to_asset_volume"),
            to_rune_volume: row.get("to_rune_volume"),
            to_trade_volume: row.get("to_trade_volume"),
            from_trade_volume: row.get("from_trade_volume"),
            synth_mint_volume: row.get("synth_mint_volume"),
            synth_redeem_volume: row.get("synth_redeem_volume"),
            total_volume: row.get("total_volume"),
            to_asset_volume_usd: row.get("to_asset_volume_usd"),
            to_rune_volume_usd: row.get("to_rune_volume_usd"),
            to_trade_volume_usd: row.get("to_trade_volume_usd"),
            from_trade_volume_usd: row.get("from_trade_volume_usd"),
            synth_mint_volume_usd: row.get("synth_mint_volume_usd"),
            synth_redeem_volume_usd: row.get("synth_redeem_volume_usd"),
            total_volume_usd: row.get("total_volume_usd"),
            to_asset_fees: row.get("to_asset_fees"),
            to_rune_fees: row.get("to_rune_fees"),
            to_trade_fees: row.get("to_trade_fees"),
            from_trade_fees: row.get("from_trade_fees"),
            synth_mint_fees: row.get("synth_mint_fees"),
            synth_redeem_fees: row.get("synth_redeem_fees"),
            total_fees: row.get("total_fees"),
            to_asset_average_slip: row.get("to_asset_average_slip"),
            to_rune_average_slip: row.get("to_rune_average_slip"),
            to_trade_average_slip: row.get("to_trade_average_slip"),
            from_trade_average_slip: row.get("from_trade_average_slip"),
            synth_mint_average_slip: row.get("synth_mint_average_slip"),
            synth_redeem_average_slip: row.get("synth_redeem_average_slip"),
            average_slip: row.get("average_slip"),
            rune_price_usd: row.get("rune_price_usd"),
        })
        .collect()
}
