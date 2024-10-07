use axum::{
    extract::Query,
    Json, Router, routing::get,
};
use std::net::SocketAddr;
use crate::db::connect_db;
use crate::models::{Interval, RunePoolInterval, SwapInterval, EarningsInterval, Pool, QueryParams}; // Ensure SwapInterval is defined for swap history
use serde_json::json;
use chrono::{NaiveDateTime, Duration};
use std::convert::TryInto;
use std::collections::HashMap;


// Function to start the server
pub async fn start_server() {
    let app = Router::new()
        .route("/depth-history", get(get_depth_history))   // Route for depth history
        .route("/rune-pool-history", get(get_rune_pool_history)) // Route for rune pool history
        .route("/swap-history", get(get_swap_history))    // Route for swap history
        .route("/earnings-history", get(get_earnings_history)); // Route for earnings history with pools

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    println!("Server running at http://{}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn get_earnings_history(Query(params): Query<QueryParams>) -> Json<serde_json::Value> {
    match connect_db().await {
        Ok(client) => {
            // Build the earnings query
            let mut earnings_query = String::from(
                "SELECT start_time, end_time, block_rewards, earnings, bonding_earnings, liquidity_earnings, \
                 avg_node_count, rune_price_usd, liquidity_fees FROM earnings_history"
            );            

            let mut filters = Vec::new();
            build_query_filters(&mut filters, &params);

            if !filters.is_empty() {
                earnings_query.push_str(" WHERE ");
                earnings_query.push_str(&filters.join(" AND "));
            }

            // Apply ordering if provided in the query
            let order_by = params.order.as_deref().unwrap_or("asc");
            earnings_query.push_str(&format!(" ORDER BY start_time {}", order_by));

            // Fetch all earnings history data without a limit
            let earnings_rows = client.query(&earnings_query, &[]).await.unwrap();
            let mut earnings_intervals: Vec<EarningsInterval> = parse_rows_to_earnings_intervals(earnings_rows);

            // Convert params.limit (i64) to usize safely
            let limit: usize = params.limit.unwrap_or(400).try_into().unwrap_or(400);

            // Apply interval aggregation to earnings history if an interval is specified
            if let Some(interval) = &params.interval {
                let interval_duration = get_interval_duration(interval);
                // Aggregate the data first, and then apply the limit of 400
                earnings_intervals = aggregate_earnings_by_interval(earnings_intervals, interval_duration, limit);
            }

            // Pagination logic: Convert page and index to `usize`
            let page: usize = params.page.unwrap_or(1).try_into().unwrap_or(1);
            let start_index: usize = (page - 1) * limit;
            let end_index: usize = std::cmp::min(start_index + limit, earnings_intervals.len());

            // Ensure we are slicing with `usize` types
            let mut paged_intervals = if start_index < earnings_intervals.len() {
                earnings_intervals[start_index..end_index].to_vec()
            } else {
                Vec::new()
            };

            // Fetch pools associated with the earnings intervals
            let start_times: Vec<i64> = paged_intervals.iter().map(|e| e.start_time).collect();
            let pools_query = format!(
                "SELECT earnings_start_time, pool_name, asset_liquidity_fees, rune_liquidity_fees, total_liquidity_fees_rune, \
                saver_earning, rewards, earnings FROM pool_history WHERE earnings_start_time = ANY($1)"
            );

            let pool_rows = client.query(&pools_query, &[&start_times]).await.unwrap();
            let pools = parse_rows_to_pools(pool_rows);

            // Create a mapping from earnings_start_time to pools
            let mut pools_map: HashMap<i64, Vec<Pool>> = HashMap::new();
            for pool_with_time in pools {
                pools_map.entry(pool_with_time.earnings_start_time)
                    .or_insert_with(Vec::new)
                    .push(pool_with_time.pool);  // Extract the `pool` field from `PoolWithStartTime`
            }

            // Add pools to the corresponding earnings interval
            for earnings in &mut paged_intervals {
                if let Some(pools) = pools_map.get(&earnings.start_time) {
                    earnings.pools = pools.clone();
                }
            }

            // Apply ordering (ASC or DESC)
            if let Some(order) = &params.order {
                if order == "desc" {
                    paged_intervals.reverse();
                }
            }

            // Return the final response in JSON format
            Json(json!({ "intervals": paged_intervals }))
        }
        Err(e) => {
            eprintln!("Failed to connect to the database: {}", e);
            Json(json!({ "error": "Failed to connect to database" }))
        }
    }
}

// Parse rows for pool history
fn parse_rows_to_pools(rows: Vec<tokio_postgres::Row>) -> Vec<PoolWithStartTime> {
    rows.iter()
        .map(|row| PoolWithStartTime {
            earnings_start_time: row.get("earnings_start_time"),
            pool: Pool {
                pool_name: row.get("pool_name"),
                asset_liquidity_fees: row.get("asset_liquidity_fees"),
                rune_liquidity_fees: row.get("rune_liquidity_fees"),
                total_liquidity_fees_rune: row.get("total_liquidity_fees_rune"),
                saver_earning: row.get("saver_earning"),
                rewards: row.get("rewards"),
                earnings: row.get("earnings"),
            }
        })
        .collect()
}

// Define PoolWithStartTime struct to map pools to start time
struct PoolWithStartTime {
    earnings_start_time: i64,
    pool: Pool,
}


fn aggregate_earnings_by_interval(
    data: Vec<EarningsInterval>,
    interval_duration: Duration,
    limit: usize
) -> Vec<EarningsInterval> {
    let mut aggregated_data: Vec<EarningsInterval> = Vec::new();
    let mut current_agg: Option<EarningsInterval> = None;
    let mut output_count = 0;
    let mut start_of_interval: Option<NaiveDateTime> = None;

    for entry in data {
        let start_time = NaiveDateTime::from_timestamp(entry.start_time, 0);

        if let Some(start_interval) = start_of_interval {
            if start_time - start_interval < interval_duration {
                if let Some(ref mut agg) = current_agg {
                    agg.aggregate(&entry);  // Aggregate earnings and pools data
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

impl EarningsInterval {
    fn aggregate(&mut self, other: &Self) {
        self.liquidity_fees += other.liquidity_fees;
        self.block_rewards += other.block_rewards;
        self.earnings += other.earnings;
        self.bonding_earnings += other.bonding_earnings;
        self.liquidity_earnings += other.liquidity_earnings;
        self.avg_node_count = (self.avg_node_count + other.avg_node_count) / 2.0;
        self.rune_price_usd = other.rune_price_usd;
        self.end_time = self.end_time.max(other.end_time);

        for other_pool in &other.pools {
            if let Some(self_pool) = self.pools.iter_mut().find(|p| p.pool_name == other_pool.pool_name) {
                self_pool.asset_liquidity_fees += other_pool.asset_liquidity_fees;
                self_pool.rune_liquidity_fees += other_pool.rune_liquidity_fees;
                self_pool.total_liquidity_fees_rune += other_pool.total_liquidity_fees_rune;
                self_pool.saver_earning += other_pool.saver_earning;
                self_pool.rewards += other_pool.rewards;
                self_pool.earnings += other_pool.earnings;
            } else {
                self.pools.push(other_pool.clone());
            }
        }
    }
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
            // Build the depth history query
            let mut query = String::from(
                "SELECT asset_depth, asset_price, asset_price_usd, end_time, \
                        liquidity_units, luvi, members_count, rune_depth, start_time, \
                        synth_supply, synth_units, units FROM depth_history"
            );

            let mut filters = Vec::new();
            build_query_filters(&mut filters, &params);

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            query.push_str(" ORDER BY start_time ASC");

            // Fetch all depth history data
            let rows = client.query(&query, &[]).await.unwrap();
            let mut intervals: Vec<Interval> = parse_rows_to_intervals(rows);

            // Convert params.limit (i64) to usize safely
            let limit: usize = params.limit.unwrap_or(400).try_into().unwrap_or(400);

            // Apply interval aggregation if an interval is specified
            if let Some(interval) = &params.interval {
                let interval_duration = get_interval_duration(interval);
                intervals = aggregate_depth_by_interval(intervals, interval_duration, limit);
            }

            // Pagination logic: Convert page and index to `usize`
            let page: usize = params.page.unwrap_or(1).try_into().unwrap_or(1);
            let start_index: usize = (page - 1) * limit;
            let end_index: usize = std::cmp::min(start_index + limit, intervals.len());

            // Ensure we are slicing with `usize` types
            let mut paged_intervals = if start_index < intervals.len() {
                intervals[start_index..end_index].to_vec()
            } else {
                Vec::new()
            };

            // Apply ordering (ASC or DESC)
            if let Some(order) = &params.order {
                if order == "desc" {
                    paged_intervals.reverse();
                }
            }

            // Return the final response in JSON format
            Json(json!({ "data": paged_intervals }))
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
            // Build the rune pool history query
            let mut query = String::from(
                "SELECT units, count, start_time, end_time FROM rune_pool_history"
            );

            let mut filters = Vec::new();
            build_query_filters(&mut filters, &params);

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            query.push_str(" ORDER BY start_time ASC");

            // Fetch all rune pool history data
            let rows = client.query(&query, &[]).await.unwrap();
            let mut intervals: Vec<RunePoolInterval> = parse_rows_to_rune_pool_intervals(rows);

            // Convert params.limit (i64) to usize safely
            let limit: usize = params.limit.unwrap_or(400).try_into().unwrap_or(400);

            // Apply interval aggregation if an interval is specified
            if let Some(interval) = &params.interval {
                let interval_duration = get_interval_duration(interval);
                intervals = aggregate_rune_pool_by_interval(intervals, interval_duration, limit);
            }

            // Pagination logic: Convert page and index to `usize`
            let page: usize = params.page.unwrap_or(1).try_into().unwrap_or(1);
            let start_index: usize = (page - 1) * limit;
            let end_index: usize = std::cmp::min(start_index + limit, intervals.len());

            // Ensure we are slicing with `usize` types
            let mut paged_intervals = if start_index < intervals.len() {
                intervals[start_index..end_index].to_vec()
            } else {
                Vec::new()
            };

            // Apply ordering (ASC or DESC)
            if let Some(order) = &params.order {
                if order == "desc" {
                    paged_intervals.reverse();
                }
            }

            // Return the final response in JSON format
            Json(json!({ "data": paged_intervals }))
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
            // Build the swap history query
            let mut query = String::from(
                "SELECT start_time, end_time, to_asset_count, to_rune_count, to_trade_count, \
                                          from_trade_count, synth_mint_count, synth_redeem_count, total_count, \
                                          to_asset_volume, to_rune_volume, to_trade_volume, from_trade_volume, \
                                          synth_mint_volume, synth_redeem_volume, total_volume, total_volume_usd, \
                                          to_asset_volume_usd, to_rune_volume_usd, to_trade_volume_usd, from_trade_volume_usd, \
                                          synth_mint_volume_usd, synth_redeem_volume_usd, to_asset_fees, to_rune_fees, \
                                          to_trade_fees, from_trade_fees, synth_mint_fees, synth_redeem_fees, total_fees, \
                                          to_asset_average_slip, to_rune_average_slip, to_trade_average_slip, \
                                          from_trade_average_slip, synth_mint_average_slip, synth_redeem_average_slip, \
                                          average_slip, rune_price_usd FROM swaps"
            );

            let mut filters = Vec::new();
            build_query_filters(&mut filters, &params);

            if !filters.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&filters.join(" AND "));
            }

            query.push_str(" ORDER BY start_time ASC");

            // Fetch all swap history data
            let rows = client.query(&query, &[]).await.unwrap();
            let mut intervals: Vec<SwapInterval> = parse_rows_to_swap_intervals(rows);

            // Convert params.limit (i64) to usize safely
            let limit: usize = params.limit.unwrap_or(400).try_into().unwrap_or(400);

            // Apply interval aggregation if an interval is specified
            if let Some(interval) = &params.interval {
                let interval_duration = get_interval_duration(interval);
                intervals = aggregate_swap_by_interval(intervals, interval_duration, limit);
            }

            // Pagination logic: Convert page and index to `usize`
            let page: usize = params.page.unwrap_or(1).try_into().unwrap_or(1);
            let start_index: usize = (page - 1) * limit;
            let end_index: usize = std::cmp::min(start_index + limit, intervals.len());

            // Ensure we are slicing with `usize` types
            let mut paged_intervals = if start_index < intervals.len() {
                intervals[start_index..end_index].to_vec()
            } else {
                Vec::new()
            };

            // Apply ordering (ASC or DESC)
            if let Some(order) = &params.order {
                if order == "desc" {
                    paged_intervals.reverse();
                }
            }

            // Return the final response in JSON format
            Json(json!({ "data": paged_intervals }))
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

fn parse_rows_to_earnings_intervals(rows: Vec<tokio_postgres::Row>) -> Vec<EarningsInterval> {
    rows.iter()
        .map(|row| EarningsInterval {
            start_time: row.get("start_time"),
            end_time: row.get("end_time"),
            liquidity_fees: row.get("liquidity_fees"),
            block_rewards: row.get("block_rewards"),
            earnings: row.get("earnings"),
            bonding_earnings: row.get("bonding_earnings"),
            liquidity_earnings: row.get("liquidity_earnings"),
            avg_node_count: row.get("avg_node_count"),
            rune_price_usd: row.get("rune_price_usd"),
            pools: Vec::new(), // This will be populated later
        })
        .collect()
}