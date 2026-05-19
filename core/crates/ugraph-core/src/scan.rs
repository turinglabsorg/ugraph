use std::{collections::BTreeMap, thread, time::Duration};

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use thiserror::Error;

use crate::{
    build_indexing_plan, decode_event_params, DecodeError, DecodedEventParam, PlanError, SourcePlan,
};

const DEFAULT_RPC_TIMEOUT_SECS: u64 = 15;

#[derive(Debug, Error)]
pub enum ScanError {
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error("rpc request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc returned error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("rpc response did not include a result")]
    MissingResult,
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

#[derive(Clone, Debug)]
pub struct ScanOptions {
    pub manifest: std::path::PathBuf,
    pub rpc_url: String,
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub max_block_range: u64,
    pub rpc_retries: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScanReport {
    pub rpc_url: String,
    pub from_block: Option<u64>,
    pub to_block: u64,
    pub log_count: usize,
    pub ordered_logs: Vec<MatchedLog>,
    pub sources: Vec<ScanSourceReport>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScanSourceReport {
    pub name: String,
    pub address: String,
    pub from_block: u64,
    pub to_block: u64,
    pub skipped: bool,
    pub trigger_count: usize,
    pub log_count: usize,
    pub logs: Vec<MatchedLog>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MatchedLog {
    pub source: String,
    pub template: bool,
    pub handler: String,
    pub signature: String,
    pub network: Option<String>,
    pub topic0: String,
    pub address: String,
    pub block_number: Option<u64>,
    pub block_hash: Option<String>,
    pub block_timestamp: Option<u64>,
    pub transaction_hash: Option<String>,
    pub transaction_index: Option<u64>,
    pub log_index: Option<u64>,
    pub topics: Vec<String>,
    pub data: String,
    pub params: Vec<DecodedEventParam>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RawEthereumLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: Option<String>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub transaction_index: Option<String>,
    pub log_index: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

pub fn scan_static_sources(options: ScanOptions) -> Result<ScanReport, ScanError> {
    let plan = build_indexing_plan(&options.manifest)?;
    let client = rpc_client()?;
    let to_block = match options.to_block {
        Some(block) => block,
        None => eth_block_number(&client, &options.rpc_url)?,
    };
    let mut sources = Vec::new();

    for source in plan
        .sources
        .into_iter()
        .filter(|source| !source.template && source.address.is_some())
    {
        sources.push(scan_source_range(
            &client,
            &options.rpc_url,
            &source,
            options.from_block,
            to_block,
            options.max_block_range,
            options.rpc_retries,
        )?);
    }

    let mut ordered_logs = sources
        .iter()
        .flat_map(|source| source.logs.iter().cloned())
        .collect::<Vec<_>>();
    ordered_logs.sort_by_key(|log| {
        (
            log.block_number.unwrap_or(u64::MAX),
            log.transaction_index.unwrap_or(u64::MAX),
            log.log_index.unwrap_or(u64::MAX),
        )
    });

    Ok(ScanReport {
        rpc_url: options.rpc_url,
        from_block: options.from_block,
        to_block,
        log_count: ordered_logs.len(),
        ordered_logs,
        sources,
    })
}

pub fn scan_planned_source(
    rpc_url: &str,
    source: &SourcePlan,
    from_block: Option<u64>,
    to_block: u64,
    max_block_range: u64,
    rpc_retries: u32,
) -> Result<ScanSourceReport, ScanError> {
    let client = rpc_client()?;
    scan_source_range(
        &client,
        rpc_url,
        source,
        from_block,
        to_block,
        max_block_range,
        rpc_retries,
    )
}

fn scan_source_range(
    client: &Client,
    rpc_url: &str,
    source: &SourcePlan,
    from_block: Option<u64>,
    to_block: u64,
    max_block_range: u64,
    rpc_retries: u32,
) -> Result<ScanSourceReport, ScanError> {
    let address = source.address.clone().unwrap_or_default();
    let source_from_block = source.start_block.unwrap_or(0);
    let from_block = from_block
        .map(|block| block.max(source_from_block))
        .unwrap_or(source_from_block);
    if address.is_empty() || from_block > to_block {
        return Ok(ScanSourceReport {
            name: source.name.clone(),
            address,
            from_block,
            to_block,
            skipped: true,
            trigger_count: source.triggers.len(),
            log_count: 0,
            logs: Vec::new(),
        });
    }

    let trigger_by_topic = source
        .triggers
        .iter()
        .map(|trigger| (trigger.topic0.to_lowercase(), trigger))
        .collect::<BTreeMap<_, _>>();
    let topic0s = source
        .triggers
        .iter()
        .map(|trigger| trigger.topic0.clone())
        .collect::<Vec<_>>();
    let mut logs = Vec::new();
    for log in eth_get_logs_chunked(LogScanRequest {
        client,
        rpc_url,
        address: &address,
        from_block,
        to_block,
        topic0s: &topic0s,
        max_block_range,
        rpc_retries,
    })? {
        let Some(topic0) = log.topics.first().map(|topic| topic.to_lowercase()) else {
            continue;
        };
        let Some(trigger) = trigger_by_topic.get(&topic0) else {
            continue;
        };
        logs.push(MatchedLog {
            source: source.name.clone(),
            template: source.dynamic,
            handler: trigger.handler.clone(),
            signature: trigger.signature.clone(),
            network: source.network.clone(),
            topic0: topic0.clone(),
            address: log.address,
            block_number: log.block_number.as_deref().and_then(parse_hex_u64),
            block_hash: log.block_hash,
            block_timestamp: None,
            transaction_hash: log.transaction_hash,
            transaction_index: log.transaction_index.as_deref().and_then(parse_hex_u64),
            log_index: log.log_index.as_deref().and_then(parse_hex_u64),
            params: decode_event_params(&trigger.inputs, &log.topics, &log.data)?,
            topics: log.topics,
            data: log.data,
        });
    }
    Ok(ScanSourceReport {
        name: source.name.clone(),
        address,
        from_block,
        to_block,
        skipped: false,
        trigger_count: source.triggers.len(),
        log_count: logs.len(),
        logs,
    })
}

pub fn latest_block_number(rpc_url: &str) -> Result<u64, ScanError> {
    let client = rpc_client()?;
    eth_block_number(&client, rpc_url)
}

pub fn scan_raw_logs(
    rpc_url: &str,
    address: &str,
    from_block: u64,
    to_block: u64,
    topic0s: &[String],
    max_block_range: u64,
    rpc_retries: u32,
) -> Result<Vec<RawEthereumLog>, ScanError> {
    let client = rpc_client()?;
    eth_get_logs_chunked(LogScanRequest {
        client: &client,
        rpc_url,
        address,
        from_block,
        to_block,
        topic0s,
        max_block_range,
        rpc_retries,
    })
}

pub fn parse_rpc_u64(value: &str) -> Option<u64> {
    parse_hex_u64(value)
}

fn eth_block_number(client: &Client, rpc_url: &str) -> Result<u64, ScanError> {
    let block = rpc::<String>(client, rpc_url, "eth_blockNumber", json!([]))?;
    parse_hex_u64(&block).ok_or(ScanError::MissingResult)
}

struct LogScanRequest<'a> {
    client: &'a Client,
    rpc_url: &'a str,
    address: &'a str,
    from_block: u64,
    to_block: u64,
    topic0s: &'a [String],
    max_block_range: u64,
    rpc_retries: u32,
}

fn eth_get_logs_chunked(request: LogScanRequest<'_>) -> Result<Vec<RawEthereumLog>, ScanError> {
    if request.topic0s.is_empty() || request.from_block > request.to_block {
        return Ok(Vec::new());
    }
    let max_block_range = request.max_block_range.max(1);
    let mut logs = Vec::new();
    for (chunk_from, chunk_to) in
        block_chunks(request.from_block, request.to_block, max_block_range)
    {
        logs.extend(eth_get_logs_resilient(
            request.client,
            request.rpc_url,
            request.address,
            chunk_from,
            chunk_to,
            request.topic0s,
            request.rpc_retries,
        )?);
    }
    Ok(logs)
}

fn eth_get_logs_resilient(
    client: &Client,
    rpc_url: &str,
    address: &str,
    from_block: u64,
    to_block: u64,
    topic0s: &[String],
    rpc_retries: u32,
) -> Result<Vec<RawEthereumLog>, ScanError> {
    match rpc_with_retries(
        client,
        rpc_url,
        "eth_getLogs",
        json!([{
            "address": address,
            "fromBlock": block_hex(from_block),
            "toBlock": block_hex(to_block),
            "topics": [topic0s],
        }]),
        rpc_retries,
    ) {
        Ok(logs) => Ok(logs),
        Err(error) if from_block < to_block && should_split_get_logs_error(&error) => {
            let mid = from_block + (to_block - from_block) / 2;
            let mut left = eth_get_logs_resilient(
                client,
                rpc_url,
                address,
                from_block,
                mid,
                topic0s,
                rpc_retries,
            )?;
            let right = eth_get_logs_resilient(
                client,
                rpc_url,
                address,
                mid.saturating_add(1),
                to_block,
                topic0s,
                rpc_retries,
            )?;
            left.extend(right);
            Ok(left)
        }
        Err(error) => Err(error),
    }
}

fn rpc<T: for<'de> Deserialize<'de>>(
    client: &Client,
    rpc_url: &str,
    method: &str,
    params: JsonValue,
) -> Result<T, ScanError> {
    rpc_with_retries(client, rpc_url, method, params, 1)
}

fn rpc_with_retries<T: for<'de> Deserialize<'de>>(
    client: &Client,
    rpc_url: &str,
    method: &str,
    params: JsonValue,
    retries: u32,
) -> Result<T, ScanError> {
    let attempts = retries.max(1);
    let mut last_error = None;
    for attempt in 0..attempts {
        match rpc_once(client, rpc_url, method, params.clone()) {
            Ok(value) => return Ok(value),
            Err(error) => {
                if attempt + 1 >= attempts || !retryable_rpc_error(&error) {
                    return Err(error);
                }
                last_error = Some(error);
                thread::sleep(Duration::from_millis(100 * 2_u64.pow(attempt.min(5))));
            }
        }
    }
    Err(last_error.unwrap_or(ScanError::MissingResult))
}

fn rpc_once<T: for<'de> Deserialize<'de>>(
    client: &Client,
    rpc_url: &str,
    method: &str,
    params: JsonValue,
) -> Result<T, ScanError> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    let response = client
        .post(rpc_url)
        .json(&body)
        .send()?
        .error_for_status()?
        .json::<JsonRpcResponse<T>>()?;
    if let Some(error) = response.error {
        return Err(ScanError::Rpc {
            code: error.code,
            message: error.message,
        });
    }
    response.result.ok_or(ScanError::MissingResult)
}

fn rpc_client() -> Result<Client, ScanError> {
    Client::builder()
        .timeout(rpc_timeout())
        .build()
        .map_err(ScanError::Http)
}

fn rpc_timeout() -> Duration {
    let seconds = std::env::var("UGRAPH_RPC_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RPC_TIMEOUT_SECS)
        .max(1);
    Duration::from_secs(seconds)
}

fn retryable_rpc_error(error: &ScanError) -> bool {
    match error {
        ScanError::Http(_) | ScanError::MissingResult => true,
        ScanError::Rpc { code, .. } => matches!(*code, -32005 | -32002 | -32603 | 429),
        ScanError::Plan(_) | ScanError::Decode(_) => false,
    }
}

fn should_split_get_logs_error(error: &ScanError) -> bool {
    match error {
        ScanError::Rpc { message, .. } => {
            let message = message.to_ascii_lowercase();
            message.contains("range")
                || message.contains("too many")
                || message.contains("more than")
                || message.contains("limit")
                || message.contains("timeout")
        }
        ScanError::Http(_) | ScanError::MissingResult => true,
        ScanError::Plan(_) | ScanError::Decode(_) => false,
    }
}

fn block_chunks(from_block: u64, to_block: u64, max_block_range: u64) -> Vec<(u64, u64)> {
    if from_block > to_block {
        return Vec::new();
    }
    let max_block_range = max_block_range.max(1);
    let mut chunks = Vec::new();
    let mut cursor = from_block;
    loop {
        let chunk_to = cursor.saturating_add(max_block_range - 1).min(to_block);
        chunks.push((cursor, chunk_to));
        if chunk_to == to_block {
            break;
        }
        cursor = chunk_to.saturating_add(1);
    }
    chunks
}

fn block_hex(block: u64) -> String {
    format!("0x{block:x}")
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_and_parses_block_hex() {
        assert_eq!(block_hex(17_000_001), "0x1036641");
        assert_eq!(parse_hex_u64("0x1036641"), Some(17_000_001));
    }

    #[test]
    fn builds_inclusive_block_chunks() {
        assert_eq!(block_chunks(10, 15, 2), vec![(10, 11), (12, 13), (14, 15)]);
        assert_eq!(block_chunks(10, 10, 2), vec![(10, 10)]);
        assert!(block_chunks(12, 10, 2).is_empty());
    }
}
