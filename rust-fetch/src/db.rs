use tokio_postgres::{Client, Error as PgError, NoTls};
use crate::models::Interval;
use std::env;

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

// Function to insert fetched data into the new depth_history table

pub async fn insert_depth_history(client: &Client, intervals: &[Interval]) -> Result<(), PgError> {
    for interval in intervals {
        let query = "
            INSERT INTO depth_history (
                asset_depth, asset_price, asset_price_usd, end_time,
                liquidity_units, luvi, members_count, rune_depth,
                start_time, synth_supply, synth_units, units
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ";

        let asset_depth: i64 = interval.asset_depth.parse().unwrap_or(0);
        let asset_price: f64 = interval.asset_price.parse().unwrap_or(0.0);
        let asset_price_usd: f64 = interval.asset_price_usd.parse().unwrap_or(0.0);
        let end_time: i64 = interval.end_time.parse().unwrap_or(0);
        let liquidity_units: i64 = interval.liquidity_units.parse().unwrap_or(0);
        let luvi: f64 = interval.luvi.parse().unwrap_or(0.0);
        let members_count: i32 = interval.members_count.parse().unwrap_or(0);
        let rune_depth: i64 = interval.rune_depth.parse().unwrap_or(0);
        let start_time: i64 = interval.start_time.parse().unwrap_or(0);
        let synth_supply: i64 = interval.synth_supply.parse().unwrap_or(0);
        let synth_units: i64 = interval.synth_units.parse().unwrap_or(0);
        let units: i64 = interval.units.parse().unwrap_or(0);

        // Execute the query
        client.execute(query, &[
            &asset_depth, &asset_price, &asset_price_usd, &end_time,
            &liquidity_units, &luvi, &members_count, &rune_depth,
            &start_time, &synth_supply, &synth_units, &units
        ]).await?;
    }

    Ok(())
}
