use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_CHAINLIST_REGISTRY_URL: &str = "https://chainid.network/chains.json";
const DEFAULT_RPC_TIMEOUT_SECS: u64 = 15;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainRegistryEntry {
    pub name: String,
    pub chain_id: u64,
    #[serde(default)]
    pub network: Option<String>,
    #[serde(default)]
    pub rpc: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct RpcResolverOptions {
    pub chain_id: u64,
    pub explicit_rpc_url: Option<String>,
    pub registry_url: String,
}

impl RpcResolverOptions {
    pub fn for_chain(chain_id: u64) -> Self {
        Self {
            chain_id,
            explicit_rpc_url: None,
            registry_url: DEFAULT_CHAINLIST_REGISTRY_URL.to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RpcResolution {
    pub chain_id: u64,
    pub source: String,
    pub urls: Vec<String>,
}

#[derive(Debug, Error)]
pub enum RpcResolverError {
    #[error("no RPC URL found for chain {chain_id}")]
    NoRpc { chain_id: u64 },
    #[error("failed to fetch Chainlist registry {url}: {source}")]
    Fetch {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to parse Chainlist registry: {0}")]
    Parse(#[from] serde_json::Error),
}

pub fn default_env_rpc_url() -> Option<String> {
    ["UGRAPH_RPC_URL", "RPC_URL", "ETH_RPC_URL"]
        .into_iter()
        .find_map(|key| std::env::var(key).ok())
        .filter(|value| !value.trim().is_empty())
}

pub fn resolve_rpc_urls(opts: RpcResolverOptions) -> Result<RpcResolution, RpcResolverError> {
    if let Some(url) = opts
        .explicit_rpc_url
        .or_else(default_env_rpc_url)
        .map(|url| url.trim().to_string())
        .filter(|url| !url.is_empty())
    {
        return Ok(RpcResolution {
            chain_id: opts.chain_id,
            source: "user".to_string(),
            urls: vec![url],
        });
    }

    let client = reqwest::blocking::Client::builder()
        .timeout(rpc_timeout())
        .build()
        .map_err(|source| RpcResolverError::Fetch {
            url: opts.registry_url.clone(),
            source,
        })?;
    let body = client
        .get(&opts.registry_url)
        .send()
        .map_err(|source| RpcResolverError::Fetch {
            url: opts.registry_url.clone(),
            source,
        })?
        .error_for_status()
        .map_err(|source| RpcResolverError::Fetch {
            url: opts.registry_url.clone(),
            source,
        })?
        .text()
        .map_err(|source| RpcResolverError::Fetch {
            url: opts.registry_url.clone(),
            source,
        })?;

    let urls = rpc_urls_from_chainlist_json(&body, opts.chain_id)?;
    if urls.is_empty() {
        return Err(RpcResolverError::NoRpc {
            chain_id: opts.chain_id,
        });
    }

    Ok(RpcResolution {
        chain_id: opts.chain_id,
        source: opts.registry_url,
        urls,
    })
}

fn rpc_timeout() -> Duration {
    let seconds = std::env::var("UGRAPH_RPC_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RPC_TIMEOUT_SECS)
        .max(1);
    Duration::from_secs(seconds)
}

pub fn rpc_urls_from_chainlist_json(
    json: &str,
    chain_id: u64,
) -> Result<Vec<String>, RpcResolverError> {
    let chains: Vec<ChainRegistryEntry> = serde_json::from_str(json)?;
    Ok(chains
        .into_iter()
        .find(|chain| chain.chain_id == chain_id)
        .map(|chain| {
            chain
                .rpc
                .into_iter()
                .filter(|url| is_usable_public_http_rpc(url))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default())
}

fn is_usable_public_http_rpc(url: &str) -> bool {
    let lower = url.to_lowercase();
    lower.starts_with("https://")
        && !lower.contains("${")
        && !lower.contains('<')
        && !lower.contains('>')
        && !lower.contains("api_key")
        && !lower.contains("apikey")
        && !lower.contains("your_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_chainlist_rpc_urls_to_public_https() -> Result<(), RpcResolverError> {
        let json = r#"
        [
          {
            "name": "Ethereum Sepolia",
            "chainId": 11155111,
            "rpc": [
              "https://rpc.sepolia.org",
              "wss://ethereum-sepolia-rpc.publicnode.com",
              "https://sepolia.infura.io/v3/${INFURA_API_KEY}",
              "https://example.com/<api_key>",
              "https://ethereum-sepolia-rpc.publicnode.com"
            ]
          }
        ]
        "#;

        let urls = rpc_urls_from_chainlist_json(json, 11155111)?;
        assert_eq!(
            urls,
            vec![
                "https://rpc.sepolia.org".to_string(),
                "https://ethereum-sepolia-rpc.publicnode.com".to_string()
            ]
        );
        Ok(())
    }
}
