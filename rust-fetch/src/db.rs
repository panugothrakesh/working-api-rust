use tokio_postgres::{Client, Error as PgError, NoTls};
use crate::models::{Interval, RunePoolInterval, SwapInterval, QueryParams}; // Ensure QueryParams, RunePoolInterval, and SwapInterval are imported
use std::env;
use chrono::{NaiveDateTime};

pub async fn connect_db() -> Result<Client, PgError> {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    // Spawn the connection in a background task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

// Function to insert fetched data into the depth_history table
pub async fn insert_depth_history(client: &Client, intervals: &[Interval]) -> Result<(), PgError> {
    for interval in intervals {
        // Check if the record already exists based on start_time (or any other unique field)
        let exists_query = "
            SELECT 1 FROM depth_history WHERE start_time = $1
        ";
        let existing_rows = client.query(exists_query, &[&interval.start_time]).await?;

        // If no rows are found, insert new data
        if existing_rows.is_empty() {
            let query = "
                INSERT INTO depth_history (
                    asset_depth, asset_price, asset_price_usd, end_time,
                    liquidity_units, luvi, members_count, rune_depth,
                    start_time, synth_supply, synth_units, units
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ";

            client.execute(query, &[
                &interval.asset_depth, &interval.asset_price, &interval.asset_price_usd, &interval.end_time,
                &interval.liquidity_units, &interval.luvi, &interval.members_count, &interval.rune_depth,
                &interval.start_time, &interval.synth_supply, &interval.synth_units, &interval.units
            ]).await?;
        }
    }
    Ok(())
}

// New function to insert fetched data into the rune_pool_history table
pub async fn insert_rune_pool_history(client: &Client, intervals: &[RunePoolInterval]) -> Result<(), PgError> {
    for interval in intervals {
        let exists_query = "
            SELECT 1 FROM rune_pool_history WHERE start_time = $1
        ";
        let existing_rows = client.query(exists_query, &[&interval.start_time]).await?;

        if existing_rows.is_empty() {
            let query = "
                INSERT INTO rune_pool_history (
                    start_time, end_time, units, count
                ) VALUES ($1, $2, $3, $4)
            ";

            client.execute(query, &[
                &interval.start_time, &interval.end_time, &interval.units, &interval.count
            ]).await?;
        }
    }
    Ok(())
}

// Function to insert fetched data into the swaps table
pub async fn insert_swap_history(client: &Client, intervals: &[SwapInterval]) -> Result<(), PgError> {
    for interval in intervals {
        let exists_query = "
            SELECT 1 FROM swaps WHERE start_time = $1
        ";
        let existing_rows = client.query(exists_query, &[&interval.start_time]).await?;

        // Only insert if the entry doesn't already exist
        if existing_rows.is_empty() {
            let query = "
                INSERT INTO swaps (
                    start_time, end_time, to_asset_count, to_rune_count, to_trade_count,
                    from_trade_count, synth_mint_count, synth_redeem_count, total_count,
                    to_asset_volume, to_rune_volume, to_trade_volume, from_trade_volume,
                    synth_mint_volume, synth_redeem_volume, total_volume,
                    to_asset_volume_usd, to_rune_volume_usd, to_trade_volume_usd, from_trade_volume_usd,
                    synth_mint_volume_usd, synth_redeem_volume_usd, total_volume_usd,
                    to_asset_fees, to_rune_fees, to_trade_fees, from_trade_fees,
                    synth_mint_fees, synth_redeem_fees, total_fees,
                    to_asset_average_slip, to_rune_average_slip, to_trade_average_slip,
                    from_trade_average_slip, synth_mint_average_slip, synth_redeem_average_slip, 
                    average_slip, rune_price_usd
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                    $31, $32, $33, $34, $35, $36, $37, $38
                )
            ";

            // The values here should match the number of columns in the INSERT statement
            client.execute(query, &[
                &interval.start_time, 
                &interval.end_time,
                &interval.to_asset_count, &interval.to_rune_count, &interval.to_trade_count,
                &interval.from_trade_count, &interval.synth_mint_count, &interval.synth_redeem_count, &interval.total_count,
                &interval.to_asset_volume, &interval.to_rune_volume, &interval.to_trade_volume, &interval.from_trade_volume,
                &interval.synth_mint_volume, &interval.synth_redeem_volume, &interval.total_volume,
                &interval.to_asset_volume_usd, &interval.to_rune_volume_usd, &interval.to_trade_volume_usd, &interval.from_trade_volume_usd,
                &interval.synth_mint_volume_usd, &interval.synth_redeem_volume_usd, &interval.total_volume_usd,
                &interval.to_asset_fees, &interval.to_rune_fees, &interval.to_trade_fees, &interval.from_trade_fees,
                &interval.synth_mint_fees, &interval.synth_redeem_fees, &interval.total_fees,
                &interval.to_asset_average_slip, &interval.to_rune_average_slip, &interval.to_trade_average_slip,
                &interval.from_trade_average_slip, &interval.synth_mint_average_slip, &interval.synth_redeem_average_slip,
                &interval.average_slip, &interval.rune_price_usd
            ]).await?;
        }
    }
    Ok(())
}

// Function to get the last timestamp from the depth_history table
pub async fn get_last_timestamp(client: &Client) -> Result<i64, PgError> {
    // Query to get the maximum end_time
    let row = client.query_one("SELECT COALESCE(MAX(end_time), 0) FROM depth_history", &[]).await?;
    Ok(row.get(0)) // Assuming end_time is stored as i64
}

// Function to get the last timestamp from the rune_pool_history table
pub async fn get_last_rune_pool_timestamp(client: &Client) -> Result<i64, PgError> {
    // Query to get the maximum end_time
    let row = client.query_one("SELECT COALESCE(MAX(end_time), 0) FROM rune_pool_history", &[]).await?;
    Ok(row.get(0)) // Assuming end_time is stored as i64
}

// Function to get the last timestamp from the swaps table
pub async fn get_last_swap_timestamp(client: &Client) -> Result<i64, PgError> {
    // Query to get the maximum end_time
    let row = client.query_one("SELECT COALESCE(MAX(end_time), 0) FROM swaps", &[]).await?;
    Ok(row.get(0)) // Assuming end_time is stored as i64
}

// New function to get depth history based on query parameters
async fn get_depth_history(client: &Client) -> Result<Vec<Interval>, Box<dyn std::error::Error>> {
    let query = "SELECT asset_depth, asset_price, asset_price_usd, end_time,
                 liquidity_units, luvi, members_count, rune_depth, start_time,
                 synth_supply, synth_units, units 
                 FROM depth_history ORDER BY end_time DESC LIMIT 400";
    
    let rows = client.query(query, &[]).await?;
    
    // Convert database rows to a vector of Intervals
    let intervals: Vec<Interval> = rows.iter().map(|row| {
        let end_time_str: String = row.get("end_time");
        let start_time_str: String = row.get("start_time");
        
        // Convert date strings to Unix timestamps
        let end_time: i64 = NaiveDateTime::parse_from_str(&end_time_str, "%Y-%m-%d %H:%M:%S")
            .map(|dt| dt.timestamp())
            .unwrap_or_default(); // Provide a default if parsing fails

        let start_time: i64 = NaiveDateTime::parse_from_str(&start_time_str, "%Y-%m-%d %H:%M:%S")
            .map(|dt| dt.timestamp())
            .unwrap_or_default(); // Provide a default if parsing fails

        Interval {
            asset_depth: row.get::<_, String>("asset_depth").parse::<i64>().unwrap_or_default(), // Convert to i64
            asset_price: row.get::<_, String>("asset_price").parse::<f64>().unwrap_or_default(), // Convert to f64
            asset_price_usd: row.get::<_, String>("asset_price_usd").parse::<f64>().unwrap_or_default(), // Convert to f64
            end_time, // Use the converted end_time
            liquidity_units: row.get::<_, String>("liquidity_units").parse::<i64>().unwrap_or_default(), // Convert to i64
            luvi: row.get::<_, String>("luvi").parse::<f64>().unwrap_or_default(), // Convert to f64
            members_count: row.get::<_, String>("members_count").parse::<i32>().unwrap_or_default(), // Convert to i32
            rune_depth: row.get::<_, String>("rune_depth").parse::<i64>().unwrap_or_default(), // Convert to i64
            start_time, // Use the converted start_time
            synth_supply: row.get::<_, String>("synth_supply").parse::<i64>().unwrap_or_default(), // Convert to i64
            synth_units: row.get::<_, String>("synth_units").parse::<i64>().unwrap_or_default(), // Convert to i64
            units: row.get::<_, String>("units").parse::<i64>().unwrap_or_default(), // Convert to i64
        }
    }).collect();

    Ok(intervals)
}