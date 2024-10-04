use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DepthHistoryResponse {
    pub intervals: Vec<Interval>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Interval {
    #[serde(rename = "assetDepth", with = "string_as_i64")]
    pub asset_depth: i64,  // Changed from String to i64
    #[serde(rename = "assetPrice", with = "string_as_f64")]
    pub asset_price: f64,  // Changed from String to f64
    #[serde(rename = "assetPriceUSD", with = "string_as_f64")]
    pub asset_price_usd: f64,  // Changed from String to f64
    #[serde(rename = "endTime", with = "string_as_i64")]
    pub end_time: i64,  // Changed from String to i64
    #[serde(rename = "liquidityUnits", with = "string_as_i64")]
    pub liquidity_units: i64,  // Changed from String to i64
    #[serde(rename = "luvi", with = "string_as_f64")]
    pub luvi: f64,  // Changed from String to f64
    #[serde(rename = "membersCount", with = "string_as_i32")]
    pub members_count: i32,  // Changed from String to i32
    #[serde(rename = "runeDepth", with = "string_as_i64")]
    pub rune_depth: i64,  // Changed from String to i64
    #[serde(rename = "startTime", with = "string_as_i64")]
    pub start_time: i64,  // Changed from String to i64
    #[serde(rename = "synthSupply", with = "string_as_i64")]
    pub synth_supply: i64,  // Changed from String to i64
    #[serde(rename = "synthUnits", with = "string_as_i64")]
    pub synth_units: i64,  // Changed from String to i64
    #[serde(rename = "units", with = "string_as_i64")]
    pub units: i64,  // Changed from String to i64
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub date_range: Option<String>, // e.g., "2023-08-01,2023-09-01"
    pub liquidity_gt: Option<i64>,   // e.g., minimum liquidity
    pub sort_by: Option<String>,      // e.g., "timestamp"
    pub order: Option<String>,         // e.g., "asc" or "desc"
    pub page: Option<u32>,            // for pagination
    pub limit: Option<u32>,           // limit of records
    pub interval: Option<String>,
}

// Helper modules for deserialization
mod string_as_i64 {
    use serde::{self, Deserialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(*value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        i64::from_str(&s).map_err(serde::de::Error::custom)
    }
}

mod string_as_f64 {
    use serde::{self, Deserialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(*value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        f64::from_str(&s).map_err(serde::de::Error::custom)
    }
}

mod string_as_i32 {
    use serde::{self, Deserialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(value: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i32(*value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        i32::from_str(&s).map_err(serde::de::Error::custom)
    }
}