use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;
use serde_json::Value as JsonValue;
use thiserror::Error;
use tiny_keccak::{Hasher, Keccak};

use crate::{Manifest, ManifestError};

#[derive(Debug, Error)]
pub enum AbiError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("failed to read ABI file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse ABI file {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct AbiEvent {
    pub name: String,
    pub signature: String,
    pub topic0: String,
    pub inputs: Vec<AbiEventInput>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AbiEventInput {
    pub name: Option<String>,
    pub kind: String,
    pub indexed: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct AbiEventReport {
    pub ok: bool,
    pub events: Vec<AbiEventCheck>,
    pub missing_events: Vec<AbiEventCheck>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AbiEventCheck {
    pub data_source: String,
    pub template: bool,
    pub abi: Option<String>,
    pub abi_path: Option<PathBuf>,
    pub event: String,
    pub handler: String,
    pub normalized_signature: String,
    pub topic0: Option<String>,
    pub inputs: Vec<AbiEventInput>,
    pub exists_in_abi: bool,
}

pub fn load_abi_events(path: impl AsRef<Path>) -> Result<Vec<AbiEvent>, AbiError> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path).map_err(|source| AbiError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let json = serde_json::from_str::<JsonValue>(&raw).map_err(|source| AbiError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(parse_abi_events(&json))
}

pub fn check_manifest_abi_events(
    manifest_path: impl AsRef<Path>,
) -> Result<AbiEventReport, AbiError> {
    let manifest_path = manifest_path.as_ref();
    let manifest = Manifest::load(manifest_path)?;
    manifest.validate_files(manifest_path)?;
    let base = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let mut events = Vec::new();

    for source in &manifest.data_sources {
        events.extend(check_source_events(
            base,
            &source.name,
            false,
            source
                .source
                .as_ref()
                .and_then(|source| source.abi.as_deref()),
            &source.mapping.abis,
            &source.mapping.event_handlers,
        )?);
    }

    for template in &manifest.templates {
        events.extend(check_source_events(
            base,
            &template.name,
            true,
            template
                .source
                .as_ref()
                .and_then(|source| source.abi.as_deref()),
            &template.mapping.abis,
            &template.mapping.event_handlers,
        )?);
    }

    let missing_events = events
        .iter()
        .filter(|event| !event.exists_in_abi)
        .cloned()
        .collect::<Vec<_>>();

    Ok(AbiEventReport {
        ok: missing_events.is_empty(),
        events,
        missing_events,
    })
}

pub fn normalize_manifest_event_signature(event: &str) -> String {
    let Some(open) = event.find('(') else {
        return event.trim().to_string();
    };
    let Some(close) = event.rfind(')') else {
        return event.trim().to_string();
    };
    let name = event[..open].trim();
    let params = split_top_level(&event[open + 1..close])
        .into_iter()
        .map(normalize_manifest_event_param)
        .collect::<Vec<_>>();
    format!("{name}({})", params.join(","))
}

pub fn event_topic0(signature: &str) -> String {
    let mut hasher = Keccak::v256();
    let mut out = [0_u8; 32];
    hasher.update(signature.as_bytes());
    hasher.finalize(&mut out);
    format!("0x{}", hex::encode(out))
}

fn check_source_events(
    base: &Path,
    data_source: &str,
    template: bool,
    source_abi: Option<&str>,
    abi_refs: &[crate::AbiRef],
    event_handlers: &[crate::EventHandler],
) -> Result<Vec<AbiEventCheck>, AbiError> {
    let abi_ref = source_abi.and_then(|name| abi_refs.iter().find(|abi| abi.name == name));
    let abi_path = abi_ref.map(|abi| base.join(&abi.file));
    let abi_events = abi_path
        .as_ref()
        .map(load_abi_events)
        .transpose()?
        .unwrap_or_default();
    let abi_by_signature = abi_events
        .into_iter()
        .map(|event| (event.signature.clone(), event))
        .collect::<BTreeMap<_, _>>();

    Ok(event_handlers
        .iter()
        .map(|handler| {
            let normalized_signature = normalize_manifest_event_signature(&handler.event);
            let abi_event = abi_by_signature.get(&normalized_signature);
            AbiEventCheck {
                data_source: data_source.to_string(),
                template,
                abi: source_abi.map(str::to_string),
                abi_path: abi_path.clone(),
                event: handler.event.clone(),
                handler: handler.handler.clone(),
                topic0: abi_event.map(|event| event.topic0.clone()),
                inputs: abi_event
                    .map(|event| event.inputs.clone())
                    .unwrap_or_default(),
                normalized_signature,
                exists_in_abi: abi_event.is_some(),
            }
        })
        .collect())
}

fn parse_abi_events(json: &JsonValue) -> Vec<AbiEvent> {
    let Some(entries) = json
        .as_array()
        .or_else(|| json.get("abi").and_then(JsonValue::as_array))
    else {
        return Vec::new();
    };
    let mut seen = BTreeSet::new();
    let mut events = Vec::new();
    for entry in entries {
        if entry.get("type").and_then(JsonValue::as_str) != Some("event") {
            continue;
        }
        let Some(name) = entry.get("name").and_then(JsonValue::as_str) else {
            continue;
        };
        let Some(inputs) = entry.get("inputs").and_then(JsonValue::as_array) else {
            continue;
        };
        let types = inputs.iter().map(canonical_abi_type).collect::<Vec<_>>();
        let signature = format!("{name}({})", types.join(","));
        if seen.insert(signature.clone()) {
            let inputs = inputs
                .iter()
                .map(|input| AbiEventInput {
                    name: input
                        .get("name")
                        .and_then(JsonValue::as_str)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string),
                    kind: canonical_abi_type(input),
                    indexed: input
                        .get("indexed")
                        .and_then(JsonValue::as_bool)
                        .unwrap_or(false),
                })
                .collect();
            events.push(AbiEvent {
                name: name.to_string(),
                topic0: event_topic0(&signature),
                signature,
                inputs,
            });
        }
    }
    events
}

fn canonical_abi_type(input: &JsonValue) -> String {
    let raw_type = input
        .get("type")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    if let Some(suffix) = raw_type.strip_prefix("tuple") {
        let components = input
            .get("components")
            .and_then(JsonValue::as_array)
            .map(|components| {
                components
                    .iter()
                    .map(canonical_abi_type)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        format!("({components}){suffix}")
    } else {
        raw_type.to_string()
    }
}

fn normalize_manifest_event_param(param: &str) -> String {
    let normalized = param
        .trim()
        .strip_prefix("indexed ")
        .unwrap_or(param.trim());
    normalized
        .split_whitespace()
        .next()
        .unwrap_or(normalized)
        .to_string()
}

fn split_top_level(input: &str) -> Vec<&str> {
    if input.trim().is_empty() {
        return Vec::new();
    }
    let mut parts = Vec::new();
    let mut depth = 0_i32;
    let mut start = 0;
    for (index, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&input[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&input[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_manifest_event_signatures() {
        assert_eq!(
            normalize_manifest_event_signature("Transfer(indexed address,indexed address,uint256)"),
            "Transfer(address,address,uint256)"
        );
        assert_eq!(
            normalize_manifest_event_signature("Complex((address,uint256),indexed bytes32)"),
            "Complex((address,uint256),bytes32)"
        );
    }

    #[test]
    fn computes_ethereum_event_topic0() {
        assert_eq!(
            event_topic0("Transfer(address,address,uint256)"),
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        );
    }

    #[test]
    fn parses_hardhat_artifact_abi_events() {
        let json = serde_json::json!({
            "_format": "hh-sol-artifact-1",
            "contractName": "Token",
            "abi": [
                {
                    "anonymous": false,
                    "inputs": [
                        { "indexed": true, "name": "from", "type": "address" },
                        { "indexed": true, "name": "to", "type": "address" },
                        { "indexed": false, "name": "value", "type": "uint256" }
                    ],
                    "name": "Transfer",
                    "type": "event"
                }
            ]
        });

        let events = parse_abi_events(&json);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].signature, "Transfer(address,address,uint256)");
    }

    #[test]
    fn growfi_manifest_events_exist_in_declared_abis() -> Result<(), AbiError> {
        let report = check_manifest_abi_events("../../examples/growfi/subgraph.yaml")?;
        assert!(report.ok, "missing ABI events: {:?}", report.missing_events);
        assert_eq!(report.events.len(), 85);
        Ok(())
    }
}
