use axum::{
    extract::Query,
    Json, Router, routing::get,
};
use std::net::SocketAddr;
use crate::db::connect_db;
use crate::models::{Interval, RunePoolInterval, SwapInterval}; // Ensure SwapInterval is defined for swap history
use serde_json::json;
use serde::Deserialize;
use chrono::{NaiveDateTime, Duration};
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
        .route("/swap-history", get(get_swap_history));    // Route for swap history

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Server running at http://{}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// Aggregation logic for depth history
fn aggregate_depth_by_interval(
    data: Vec<Interval>,
    interval_duration: Duration,
    limit: usize,
) -> Vec<Interval> {
    let mut aggregated_data: Vec<Interval> = Vec::new();
    let mut current_agg: Option<Interval> = None;
    let mut output_count = 0;
    let mut start_of_interval: Option<NaiveDateTime> = None;

    for entry in data {
        let start_time = NaiveDateTime::from_timestamp(entry.start_time, 0);

        if let Some(start_interval) = start_of_interval {
            if start_time - start_interval < interval_duration {
                if let Some(ref mut agg) = current_agg {
                    agg.aggregate(&entry);
                }
            } else {
                if let Some(agg) = current_agg.take() {
                    aggregated_data.push(agg);
                    output_count += 1;
                    if output_count >= limit {
                        break;
                    }
                }
                start_of_interval = Some(start_time);
                current_agg = Some(entry.clone());
            }
        } else {
            start_of_interval = Some(start_time);
            current_agg = Some(entry.clone());
        }
    }

    if let Some(agg) = current_agg {
        aggregated_data.push(agg);
    }

    aggregated_data
}

// Aggregation logic for rune pool history
fn aggregate_rune_pool_by_interval(
    data: Vec<RunePoolInterval>,
    interval_duration: Duration,
    limit: usize,
) -> Vec<RunePoolInterval> {
    let mut aggregated_data: Vec<RunePoolInterval> = Vec::new();
    let mut current_agg: Option<RunePoolInterval> = None;
    let mut output_count = 0;
    let mut start_of_interval: Option<NaiveDateTime> = None;

    for entry in data {
        let start_time = NaiveDateTime::from_timestamp(entry.start_time, 0);

        if let Some(start_interval) = start_of_interval {
            if start_time - start_interval < interval_duration {
                if let Some(ref mut agg) = current_agg {
                    agg.aggregate(&entry);
                }
            } else {
                if let Some(agg) = current_agg.take() {
                    aggregated_data.push(agg);
                    output_count += 1;
                    if output_count >= limit {
                        break;
                    }
                }
                start_of_interval = Some(start_time);
                current_agg = Some(entry.clone());
            }
        } else {
            start_of_interval = Some(start_time);
            current_agg = Some(entry.clone());
        }
    }

    if let Some(agg) = current_agg {
        aggregated_data.push(agg);
    }

    aggregated_data
}

// Aggregation logic for swap history
fn aggregate_swap_by_interval(
    data: Vec<SwapInterval>,
    interval_duration: Duration,
    limit: usize,
) -> Vec<SwapInterval> {
    let mut aggregated_data: Vec<SwapInterval> = Vec::new();
    let mut current_agg: Option<SwapInterval> = None;
    let mut output_count = 0;
    let mut start_of_interval: Option<NaiveDateTime> = None;

    for entry in data {
        let start_time = NaiveDateTime::from_timestamp(entry.start_time, 0);

        if let Some(start_interval) = start_of_interval {
            if start_time - start_interval < interval_duration {
                if let Some(ref mut agg) = current_agg {
                    agg.aggregate(&entry);
                }
            } else {
                if let Some(agg) = current_agg.take() {
                    aggregated_data.push(agg);
                    output_count += 1;
                    if output_count >= limit {
                        break;
                    }
                }
                start_of_interval = Some(start_time);
                current_agg = Some(entry.clone());
            }
        } else {
            start_of_interval = Some(start_time);
            current_agg = Some(entry.clone());
        }
    }

    if let Some(agg) = current_agg {
        aggregated_data.push(agg);
    }

    aggregated_data
}

// Trait for aggregating depth history intervals
impl Interval {
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

// Trait for aggregating rune pool history intervals
impl RunePoolInterval {
    fn aggregate(&mut self, other: &Self) {
        self.units += other.units;
        self.count += other.count;
        self.end_time = self.end_time.max(other.end_time);
    }
}

// Trait for aggregating swap history intervals
impl SwapInterval {
    fn aggregate(&mut self, other: &Self) {
        self.to_asset_count += other.to_asset_count;
        self.to_rune_count += other.to_rune_count;
        self.to_trade_count += other.to_trade_count;
        self.from_trade_count += other.from_trade_count;
        self.synth_mint_count += other.synth_mint_count;
        self.synth_redeem_count += other.synth_redeem_count;
        self.total_count += other.total_count;
        self.to_asset_volume += other.to_asset_volume;
        self.to_rune_volume += other.to_rune_volume;
        self.to_trade_volume += other.to_trade_volume;
        self.from_trade_volume += other.from_trade_volume;
        self.synth_mint_volume += other.synth_mint_volume;
        self.synth_redeem_volume += other.synth_redeem_volume;
        self.total_volume += other.total_volume;
        self.total_volume_usd += other.total_volume_usd;
        self.to_asset_volume_usd += other.to_asset_volume_usd;
        self.to_rune_volume_usd += other.to_rune_volume_usd;
        self.to_trade_volume_usd += other.to_trade_volume_usd;
        self.from_trade_volume_usd += other.from_trade_volume_usd;
        self.synth_mint_volume_usd += other.synth_mint_volume_usd;
        self.synth_redeem_volume_usd += other.synth_redeem_volume_usd;
        self.to_asset_fees += other.to_asset_fees;
        self.to_rune_fees += other.to_rune_fees;
        self.to_trade_fees += other.to_trade_fees;
        self.from_trade_fees += other.from_trade_fees;
        self.synth_mint_fees += other.synth_mint_fees;
        self.synth_redeem_fees += other.synth_redeem_fees;
        self.total_fees += other.total_fees;
        self.to_asset_average_slip = (self.to_asset_average_slip + other.to_asset_average_slip) / 2.0;
        self.to_rune_average_slip = (self.to_rune_average_slip + other.to_rune_average_slip) / 2.0;
        self.to_trade_average_slip = (self.to_trade_average_slip + other.to_trade_average_slip) / 2.0;
        self.from_trade_average_slip = (self.from_trade_average_slip + other.from_trade_average_slip) / 2.0;
        self.synth_mint_average_slip = (self.synth_mint_average_slip + other.synth_mint_average_slip) / 2.0;
        self.synth_redeem_average_slip = (self.synth_redeem_average_slip + other.synth_redeem_average_slip) / 2.0;
        self.average_slip = (self.average_slip + other.average_slip) / 2.0;
        self.rune_price_usd = other.rune_price_usd;
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

            if let Some(interval) = &params.interval {
                let rows = client.query(&query, &[]).await.unwrap();
                let intervals: Vec<Interval> = parse_rows_to_intervals(rows);
                let limit = params.limit.unwrap_or(400);

                let aggregated = aggregate_depth_by_interval(intervals, get_interval_duration(interval), limit as usize);

                return Json(json!({ "data": aggregated }));
            } else {
                let limit = params.limit.unwrap_or(400);
                query.push_str(&format!(" LIMIT {}", limit));

                let rows = client.query(&query, &[]).await.unwrap();
                let data: Vec<Interval> = parse_rows_to_intervals(rows);

                return Json(json!({ "data": data }));
            }
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

            if let Some(interval) = &params.interval {
                let rows = client.query(&query, &[]).await.unwrap();
                let intervals: Vec<RunePoolInterval> = parse_rows_to_rune_pool_intervals(rows);
                let limit = params.limit.unwrap_or(400);

                let aggregated = aggregate_rune_pool_by_interval(intervals, get_interval_duration(interval), limit as usize);

                return Json(json!({ "data": aggregated }));
            } else {
                let limit = params.limit.unwrap_or(400);
                query.push_str(&format!(" LIMIT {}", limit));

                let rows = client.query(&query, &[]).await.unwrap();
                let data: Vec<RunePoolInterval> = parse_rows_to_rune_pool_intervals(rows);

                return Json(json!({ "data": data }));
            }
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
                                          synth_mint_volume, synth_redeem_volume, total_volume, total_volume_usd, \
                                          to_asset_volume_usd, to_rune_volume_usd, to_trade_volume_usd, from_trade_volume_usd, \
                                          synth_mint_volume_usd, synth_redeem_volume_usd, to_asset_fees, to_rune_fees, \
                                          to_trade_fees, from_trade_fees, synth_mint_fees, synth_redeem_fees, total_fees, \
                                          to_asset_average_slip, to_rune_average_slip, to_trade_average_slip, \
                                          from_trade_average_slip, synth_mint_average_slip, synth_redeem_average_slip, \
                                          average_slip, rune_price_usd FROM swaps");

            let mut filters = Vec::new();
            build_query_filters(&mut filters, &params);

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            query.push_str(" ORDER BY start_time ASC");

            if let Some(interval) = &params.interval {
                let rows = client.query(&query, &[]).await.unwrap();
                let intervals: Vec<SwapInterval> = parse_rows_to_swap_intervals(rows);
                let limit = params.limit.unwrap_or(400);

                let aggregated = aggregate_swap_by_interval(intervals, get_interval_duration(interval), limit as usize);

                return Json(json!({ "data": aggregated }));
            } else {
                let limit = params.limit.unwrap_or(400);
                query.push_str(&format!(" LIMIT {}", limit));

                let rows = client.query(&query, &[]).await.unwrap();
                let data: Vec<SwapInterval> = parse_rows_to_swap_intervals(rows);

                return Json(json!({ "data": data }));
            }
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}

// Utility functions for query filters and interval parsing
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
}

fn get_interval_duration(interval: &str) -> Duration {
    match interval {
        "day" => Duration::days(1),
        "week" => Duration::weeks(1),
        "month" => Duration::days(30),
        "6months" => Duration::days(180),
        "year" => Duration::days(365),
        _ => Duration::days(1), // Default to 1-day interval if none is provided
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
            total_volume_usd: row.get("total_volume_usd"),
            to_asset_volume_usd: row.get("to_asset_volume_usd"),
            to_rune_volume_usd: row.get("to_rune_volume_usd"),
            to_trade_volume_usd: row.get("to_trade_volume_usd"),
            from_trade_volume_usd: row.get("from_trade_volume_usd"),
            synth_mint_volume_usd: row.get("synth_mint_volume_usd"),
            synth_redeem_volume_usd: row.get("synth_redeem_volume_usd"),
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