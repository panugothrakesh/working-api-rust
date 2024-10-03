// src/models.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)] // Make sure to derive Clone
pub struct DepthHistoryResponse {
    pub intervals: Vec<Interval>,
}

#[derive(Debug, Serialize, Deserialize, Clone)] // Make sure to derive Clone
pub struct Interval {
    #[serde(rename = "assetDepth")]
    pub asset_depth: String,
    #[serde(rename = "assetPrice")]
    pub asset_price: String,
    #[serde(rename = "assetPriceUSD")]
    pub asset_price_usd: String,
    #[serde(rename = "endTime")]
    pub end_time: String,
    #[serde(rename = "liquidityUnits")]
    pub liquidity_units: String,
    #[serde(rename = "luvi")] // Add this line
    pub luvi: String, // Add the luvi field
    #[serde(rename = "membersCount")]
    pub members_count: String,
    #[serde(rename = "runeDepth")]
    pub rune_depth: String,
    #[serde(rename = "startTime")]
    pub start_time: String,
    #[serde(rename = "synthSupply")]
    pub synth_supply: String,
    #[serde(rename = "synthUnits")]
    pub synth_units: String,
    #[serde(rename = "units")]
    pub units: String,
}