use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DepthHistoryResponse {
    pub intervals: Vec<Interval>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Interval {
    #[serde(rename = "assetDepth")]
    pub asset_depth: i64,  // Changed from String to i64
    #[serde(rename = "assetPrice")]
    pub asset_price: f64,  // Changed from String to f64
    #[serde(rename = "assetPriceUSD")]
    pub asset_price_usd: f64,  // Changed from String to f64
    #[serde(rename = "endTime")]
    pub end_time: i64,  // Changed from String to i64
    #[serde(rename = "liquidityUnits")]
    pub liquidity_units: i64,  // Changed from String to i64
    #[serde(rename = "luvi")]
    pub luvi: f64,  // Changed from String to f64
    #[serde(rename = "membersCount")]
    pub members_count: i32,  // Changed from String to i32
    #[serde(rename = "runeDepth")]
    pub rune_depth: i64,  // Changed from String to i64
    #[serde(rename = "startTime")]
    pub start_time: i64,  // Changed from String to i64
    #[serde(rename = "synthSupply")]
    pub synth_supply: i64,  // Changed from String to i64
    #[serde(rename = "synthUnits")]
    pub synth_units: i64,  // Changed from String to i64
    #[serde(rename = "units")]
    pub units: i64,  // Changed from String to i64
}