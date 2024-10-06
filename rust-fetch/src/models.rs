use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DepthHistoryResponse {
    pub intervals: Vec<Interval>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Interval {
    #[serde(rename = "assetDepth", with = "string_as_i64")]
    pub asset_depth: i64,
    #[serde(rename = "assetPrice", with = "string_as_f64")]
    pub asset_price: f64,
    #[serde(rename = "assetPriceUSD", with = "string_as_f64")]
    pub asset_price_usd: f64,
    #[serde(rename = "endTime", with = "string_as_i64")]
    pub end_time: i64,
    #[serde(rename = "liquidityUnits", with = "string_as_i64")]
    pub liquidity_units: i64,
    #[serde(rename = "luvi", with = "string_as_f64")]
    pub luvi: f64,
    #[serde(rename = "membersCount", with = "string_as_i32")]
    pub members_count: i32,
    #[serde(rename = "runeDepth", with = "string_as_i64")]
    pub rune_depth: i64,
    #[serde(rename = "startTime", with = "string_as_i64")]
    pub start_time: i64,
    #[serde(rename = "synthSupply", with = "string_as_i64")]
    pub synth_supply: i64,
    #[serde(rename = "synthUnits", with = "string_as_i64")]
    pub synth_units: i64,
    #[serde(rename = "units", with = "string_as_i64")]
    pub units: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunePoolHistoryResponse {
    pub intervals: Vec<RunePoolInterval>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunePoolInterval {
    #[serde(rename = "startTime", with = "string_as_i64")]
    pub start_time: i64,
    #[serde(rename = "endTime", with = "string_as_i64")]
    pub end_time: i64,
    #[serde(rename = "units", with = "string_as_i64")]
    pub units: i64,
    #[serde(rename = "count", with = "string_as_i64")]
    pub count: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SwapHistoryResponse {
    pub intervals: Vec<SwapInterval>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SwapInterval {
    #[serde(rename = "startTime", with = "string_as_i64")]
    pub start_time: i64,
    #[serde(rename = "endTime", with = "string_as_i64")]
    pub end_time: i64,
    #[serde(rename = "toAssetCount", with = "string_as_i64")]
    pub to_asset_count: i64,
    #[serde(rename = "toRuneCount", with = "string_as_i64")]
    pub to_rune_count: i64,
    #[serde(rename = "toTradeCount", with = "string_as_i64")]
    pub to_trade_count: i64,
    #[serde(rename = "fromTradeCount", with = "string_as_i64")]
    pub from_trade_count: i64,
    #[serde(rename = "synthMintCount", with = "string_as_i64")]
    pub synth_mint_count: i64,
    #[serde(rename = "synthRedeemCount", with = "string_as_i64")]
    pub synth_redeem_count: i64,
    #[serde(rename = "totalCount", with = "string_as_i64")]
    pub total_count: i64,
    #[serde(rename = "toAssetVolume", with = "string_as_f64")]
    pub to_asset_volume: f64,
    #[serde(rename = "toRuneVolume", with = "string_as_f64")]
    pub to_rune_volume: f64,
    #[serde(rename = "toTradeVolume", with = "string_as_f64")]
    pub to_trade_volume: f64,
    #[serde(rename = "fromTradeVolume", with = "string_as_f64")]
    pub from_trade_volume: f64,
    #[serde(rename = "synthMintVolume", with = "string_as_f64")]
    pub synth_mint_volume: f64,
    #[serde(rename = "synthRedeemVolume", with = "string_as_f64")]
    pub synth_redeem_volume: f64,
    #[serde(rename = "totalVolume", with = "string_as_f64")]
    pub total_volume: f64,
    #[serde(rename = "toAssetVolumeUSD", with = "string_as_f64")]
    pub to_asset_volume_usd: f64,
    #[serde(rename = "toRuneVolumeUSD", with = "string_as_f64")]
    pub to_rune_volume_usd: f64,
    #[serde(rename = "toTradeVolumeUSD", with = "string_as_f64")]
    pub to_trade_volume_usd: f64,
    #[serde(rename = "fromTradeVolumeUSD", with = "string_as_f64")]
    pub from_trade_volume_usd: f64,
    #[serde(rename = "synthMintVolumeUSD", with = "string_as_f64")]
    pub synth_mint_volume_usd: f64,
    #[serde(rename = "synthRedeemVolumeUSD", with = "string_as_f64")]
    pub synth_redeem_volume_usd: f64,
    #[serde(rename = "totalVolumeUSD", with = "string_as_f64")]
    pub total_volume_usd: f64,
    #[serde(rename = "toAssetFees", with = "string_as_f64")]
    pub to_asset_fees: f64,
    #[serde(rename = "toRuneFees", with = "string_as_f64")]
    pub to_rune_fees: f64,
    #[serde(rename = "toTradeFees", with = "string_as_f64")]
    pub to_trade_fees: f64,
    #[serde(rename = "fromTradeFees", with = "string_as_f64")]
    pub from_trade_fees: f64,
    #[serde(rename = "synthMintFees", with = "string_as_f64")]
    pub synth_mint_fees: f64,
    #[serde(rename = "synthRedeemFees", with = "string_as_f64")]
    pub synth_redeem_fees: f64,
    #[serde(rename = "totalFees", with = "string_as_f64")]
    pub total_fees: f64,
    #[serde(rename = "toAssetAverageSlip", with = "string_as_f64")]
    pub to_asset_average_slip: f64,
    #[serde(rename = "toRuneAverageSlip", with = "string_as_f64")]
    pub to_rune_average_slip: f64,
    #[serde(rename = "toTradeAverageSlip", with = "string_as_f64")]
    pub to_trade_average_slip: f64,
    #[serde(rename = "fromTradeAverageSlip", with = "string_as_f64")]
    pub from_trade_average_slip: f64,
    #[serde(rename = "synthMintAverageSlip", with = "string_as_f64")]
    pub synth_mint_average_slip: f64,
    #[serde(rename = "synthRedeemAverageSlip", with = "string_as_f64")]
    pub synth_redeem_average_slip: f64,
    #[serde(rename = "averageSlip", with = "string_as_f64")]
    pub average_slip: f64,
    #[serde(rename = "runePriceUSD", with = "string_as_f64")]
    pub rune_price_usd: f64,
}


// Query Params Struct
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