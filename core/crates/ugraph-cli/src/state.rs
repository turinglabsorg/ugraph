use std::{collections::BTreeMap, fs, path::Path};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use ugraph_core::EntitySchema;
use ugraph_runtime::{EntityData, EntityStore};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreSnapshot {
    pub version: u32,
    pub manifest: String,
    pub checkpoint: SyncCheckpoint,
    pub schema: EntitySchema,
    pub entities: Vec<EntitySnapshot>,
    pub dynamic_sources: Vec<DynamicSourceSnapshot>,
    #[serde(default)]
    pub processed_logs: Vec<ProcessedLogSnapshot>,
    #[serde(default)]
    pub history: Vec<HistoricalSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncCheckpoint {
    pub from_block: Option<u64>,
    pub to_block: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<String>,
    pub scanned_logs: usize,
    pub executed_logs: usize,
    pub validation_errors: usize,
    #[serde(default)]
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitySnapshot {
    pub entity: String,
    pub id: String,
    pub data: EntityData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicSourceSnapshot {
    pub name: String,
    pub params: Vec<String>,
    pub address: String,
    pub created_at_block: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub context: EntityData,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ProcessedLogSnapshot {
    pub source: String,
    pub template: bool,
    pub address: String,
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
    pub topic0: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalSnapshot {
    pub checkpoint: SyncCheckpoint,
    pub entities: Vec<EntitySnapshot>,
    pub dynamic_sources: Vec<DynamicSourceSnapshot>,
}

pub fn load_snapshot(path: impl AsRef<Path>) -> anyhow::Result<StoreSnapshot> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("reading state snapshot {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parsing state snapshot {}", path.display()))
}

pub fn try_load_snapshot(path: impl AsRef<Path>) -> anyhow::Result<Option<StoreSnapshot>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(None);
    }
    load_snapshot(path).map(Some)
}

pub fn write_snapshot(path: impl AsRef<Path>, snapshot: &StoreSnapshot) -> anyhow::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating state directory {}", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(snapshot)?;
    fs::write(path, raw).with_context(|| format!("writing state snapshot {}", path.display()))
}

pub fn snapshot_from_store(
    manifest: &Path,
    checkpoint: SyncCheckpoint,
    schema: EntitySchema,
    store: &EntityStore,
    dynamic_sources: Vec<DynamicSourceSnapshot>,
    processed_logs: Vec<ProcessedLogSnapshot>,
) -> StoreSnapshot {
    let entities = store
        .iter()
        .map(|((entity, id), data)| EntitySnapshot {
            entity: entity.clone(),
            id: id.clone(),
            data: data.clone(),
        })
        .collect();

    StoreSnapshot {
        version: 1,
        manifest: manifest.display().to_string(),
        checkpoint,
        schema,
        entities,
        dynamic_sources: dedupe_dynamic_sources(dynamic_sources),
        processed_logs,
        history: Vec::new(),
    }
}

pub fn historical_snapshot_from_store(
    checkpoint: SyncCheckpoint,
    store: &EntityStore,
    dynamic_sources: Vec<DynamicSourceSnapshot>,
) -> HistoricalSnapshot {
    let entities = store
        .iter()
        .map(|((entity, id), data)| EntitySnapshot {
            entity: entity.clone(),
            id: id.clone(),
            data: data.clone(),
        })
        .collect();

    HistoricalSnapshot {
        checkpoint,
        entities,
        dynamic_sources: dedupe_dynamic_sources(dynamic_sources),
    }
}

pub fn materialize_historical_snapshot(
    current: &StoreSnapshot,
    historical: &HistoricalSnapshot,
) -> StoreSnapshot {
    StoreSnapshot {
        version: current.version,
        manifest: current.manifest.clone(),
        checkpoint: historical.checkpoint.clone(),
        schema: current.schema.clone(),
        entities: historical.entities.clone(),
        dynamic_sources: historical.dynamic_sources.clone(),
        processed_logs: Vec::new(),
        history: current.history.clone(),
    }
}

pub fn entity_store_from_snapshot(snapshot: &StoreSnapshot) -> EntityStore {
    snapshot
        .entities
        .iter()
        .map(|entity| {
            (
                (entity.entity.clone(), entity.id.clone()),
                entity.data.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>()
}

fn dedupe_dynamic_sources(sources: Vec<DynamicSourceSnapshot>) -> Vec<DynamicSourceSnapshot> {
    sources
        .into_iter()
        .map(|source| {
            (
                (
                    source.name.clone(),
                    source.address.to_lowercase(),
                    source.created_at_block,
                ),
                source,
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect()
}
