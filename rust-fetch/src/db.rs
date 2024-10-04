use tokio_postgres::{Client, Error as PgError, NoTls};
use crate::models::{Interval, QueryParams}; // Ensure QueryParams is imported
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

// Function to get the last timestamp from the depth_history table
pub async fn get_last_timestamp(client: &Client) -> Result<i64, PgError> {
    // Query to get the maximum end_time
    let row = client.query_one("SELECT COALESCE(MAX(end_time), 0) FROM depth_history", &[]).await?;
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