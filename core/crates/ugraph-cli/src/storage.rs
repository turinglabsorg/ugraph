use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Context;
use postgres::{Client, NoTls};
use serde_json::Value;
use ugraph_core::{
    decode_event_params, parse_rpc_u64, EntitySchema, RawEthereumLog, ScanSourceReport, SourcePlan,
};
use ugraph_runtime::EntityData;

use crate::state::{
    load_snapshot, try_load_snapshot, write_snapshot, DynamicSourceSnapshot, EntitySnapshot,
    HistoricalSnapshot, ProcessedLogSnapshot, StoreSnapshot, SyncCheckpoint,
};

#[derive(Debug, Clone)]
pub enum SnapshotStore {
    Json { path: PathBuf },
    Postgres { url: String, deployment: String },
}

pub struct IndexerLock {
    client: Option<Client>,
    key: String,
}

#[derive(Debug, Clone)]
pub struct FeedSubscription {
    pub chain_id: u64,
    pub deployment: String,
    pub source: String,
    pub template: bool,
    pub address: String,
    pub from_block: u64,
    pub cursor_block: Option<u64>,
    pub topic0s: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FeedIngestReport {
    pub chain_id: u64,
    pub subscriptions: usize,
    pub to_block: Option<u64>,
    pub inserted_logs: u64,
}

impl SnapshotStore {
    pub fn label(&self) -> String {
        match self {
            Self::Json { path } => path.display().to_string(),
            Self::Postgres { deployment, .. } => format!("postgres:{deployment}"),
        }
    }

    pub fn load(&self) -> anyhow::Result<StoreSnapshot> {
        match self {
            Self::Json { path } => load_snapshot(path),
            Self::Postgres { url, deployment } => {
                let mut client = connect(url)?;
                migrate(&mut client)?;
                load_postgres_snapshot(&mut client, deployment)?
                    .with_context(|| format!("loading postgres snapshot `{deployment}`"))
            }
        }
    }

    pub fn try_load(&self) -> anyhow::Result<Option<StoreSnapshot>> {
        match self {
            Self::Json { path } => try_load_snapshot(path),
            Self::Postgres { url, deployment } => {
                let mut client = connect(url)?;
                migrate(&mut client)?;
                load_postgres_snapshot(&mut client, deployment)
            }
        }
    }

    pub fn write(&self, snapshot: &StoreSnapshot) -> anyhow::Result<()> {
        match self {
            Self::Json { path } => write_snapshot(path, snapshot),
            Self::Postgres { url, deployment } => {
                let mut client = connect(url)?;
                migrate(&mut client)?;
                write_postgres_snapshot(&mut client, deployment, snapshot)
            }
        }
    }

    pub fn acquire_indexer_lock(&self) -> anyhow::Result<Option<IndexerLock>> {
        match self {
            Self::Json { .. } => Ok(None),
            Self::Postgres { url, deployment } => {
                let mut client = connect(url)?;
                migrate(&mut client)?;
                let key = format!("ugraph:indexer:{deployment}");
                let locked: bool = client
                    .query_one(
                        "select pg_try_advisory_lock(hashtextextended($1, 0))",
                        &[&key],
                    )?
                    .get(0);
                if !locked {
                    anyhow::bail!("indexer lock is already held for deployment `{deployment}`");
                }
                Ok(Some(IndexerLock {
                    client: Some(client),
                    key,
                }))
            }
        }
    }
}

impl Drop for IndexerLock {
    fn drop(&mut self) {
        let Some(mut client) = self.client.take() else {
            return;
        };
        let _ = client.query_one(
            "select pg_advisory_unlock(hashtextextended($1, 0))",
            &[&self.key],
        );
    }
}

fn connect(url: &str) -> anyhow::Result<Client> {
    Client::connect(url, NoTls).context("connecting to postgres")
}

fn migrate(client: &mut Client) -> anyhow::Result<()> {
    client.batch_execute(POSTGRES_SCHEMA)?;
    Ok(())
}

fn load_postgres_snapshot(
    client: &mut Client,
    deployment: &str,
) -> anyhow::Result<Option<StoreSnapshot>> {
    let Some(row) = client.query_opt(
        "select version, manifest, checkpoint, schema, history from ugraph_deployments where id = $1",
        &[&deployment],
    )?
    else {
        return Ok(None);
    };
    let version: i32 = row.get("version");
    let manifest: String = row.get("manifest");
    let checkpoint_value: Value = row.get("checkpoint");
    let schema_value: Value = row.get("schema");
    let history_value: Value = row.get("history");
    let checkpoint: SyncCheckpoint =
        serde_json::from_value(checkpoint_value).context("decoding postgres checkpoint")?;
    let schema: EntitySchema =
        serde_json::from_value(schema_value).context("decoding postgres schema")?;
    let legacy_history: Vec<HistoricalSnapshot> =
        serde_json::from_value(history_value).context("decoding postgres history")?;
    let stored_history = load_postgres_history(client, deployment)?;
    let history = if stored_history.is_empty() {
        legacy_history
    } else {
        stored_history
    };

    let entities = client
        .query(
            "select entity, id, data from ugraph_entities where deployment = $1 order by entity, id",
            &[&deployment],
        )?
        .into_iter()
        .map(|row| {
            let data_value: Value = row.get("data");
            let data: EntityData =
                serde_json::from_value(data_value).context("decoding postgres entity data")?;
            Ok(EntitySnapshot {
                entity: row.get("entity"),
                id: row.get("id"),
                data,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let dynamic_sources = client
        .query(
            "select name, params, address, created_at_block, context from ugraph_dynamic_sources where deployment = $1 order by created_at_block, name, address",
            &[&deployment],
        )?
        .into_iter()
        .map(|row| {
            let params_value: Value = row.get("params");
            let params = serde_json::from_value(params_value)
                .context("decoding postgres dynamic source params")?;
            let context_value: Value = row.get("context");
            let context = serde_json::from_value(context_value)
                .context("decoding postgres dynamic source context")?;
            let created_at_block: i64 = row.get("created_at_block");
            Ok(DynamicSourceSnapshot {
                name: row.get("name"),
                params,
                address: row.get("address"),
                created_at_block: u64::try_from(created_at_block)
                    .context("dynamic source block is negative")?,
                context,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let processed_logs = client
        .query(
            "select source, template, address, block_number, transaction_index, log_index, topic0 from ugraph_processed_logs where deployment = $1 order by block_number, transaction_index, log_index",
            &[&deployment],
        )?
        .into_iter()
        .map(|row| {
            let block_number: i64 = row.get("block_number");
            let transaction_index: i64 = row.get("transaction_index");
            let log_index: i64 = row.get("log_index");
            Ok(ProcessedLogSnapshot {
                source: row.get("source"),
                template: row.get("template"),
                address: row.get("address"),
                block_number: u64::try_from(block_number).context("processed log block is negative")?,
                transaction_index: u64::try_from(transaction_index)
                    .context("processed log transaction index is negative")?,
                log_index: u64::try_from(log_index).context("processed log index is negative")?,
                topic0: row.get("topic0"),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(Some(StoreSnapshot {
        version: u32::try_from(version).context("snapshot version is negative")?,
        manifest,
        checkpoint,
        schema,
        entities,
        dynamic_sources,
        processed_logs,
        history,
    }))
}

fn write_postgres_snapshot(
    client: &mut Client,
    deployment: &str,
    snapshot: &StoreSnapshot,
) -> anyhow::Result<()> {
    let mut tx = client.transaction()?;
    let checkpoint = serde_json::to_value(&snapshot.checkpoint)?;
    let schema = serde_json::to_value(&snapshot.schema)?;
    let history = serde_json::to_value(&snapshot.history)?;
    let version = i32::try_from(snapshot.version).context("snapshot version overflows postgres")?;
    tx.execute(
        r#"
        insert into ugraph_deployments (id, version, manifest, checkpoint, schema, history, updated_at)
        values ($1, $2, $3, $4, $5, $6, now())
        on conflict (id) do update set
          version = excluded.version,
          manifest = excluded.manifest,
          checkpoint = excluded.checkpoint,
          schema = excluded.schema,
          history = excluded.history,
          updated_at = now()
        "#,
        &[
            &deployment,
            &version,
            &snapshot.manifest,
            &checkpoint,
            &schema,
            &history,
        ],
    )?;

    tx.execute(
        "delete from ugraph_entities where deployment = $1",
        &[&deployment],
    )?;
    for entity in &snapshot.entities {
        let data = serde_json::to_value(&entity.data)?;
        tx.execute(
            "insert into ugraph_entities (deployment, entity, id, data) values ($1, $2, $3, $4)",
            &[&deployment, &entity.entity, &entity.id, &data],
        )?;
    }

    tx.execute(
        "delete from ugraph_dynamic_sources where deployment = $1",
        &[&deployment],
    )?;
    for source in &snapshot.dynamic_sources {
        let params = serde_json::to_value(&source.params)?;
        let context = serde_json::to_value(&source.context)?;
        let created_at_block = i64::try_from(source.created_at_block)
            .context("dynamic source block overflows postgres")?;
        tx.execute(
            "insert into ugraph_dynamic_sources (deployment, name, address, created_at_block, params, context) values ($1, $2, $3, $4, $5, $6)",
            &[&deployment, &source.name, &source.address, &created_at_block, &params, &context],
        )?;
    }

    tx.execute(
        "delete from ugraph_processed_logs where deployment = $1",
        &[&deployment],
    )?;
    for log in &snapshot.processed_logs {
        let block_number =
            i64::try_from(log.block_number).context("processed log block overflows postgres")?;
        let transaction_index = i64::try_from(log.transaction_index)
            .context("processed log transaction index overflows postgres")?;
        let log_index =
            i64::try_from(log.log_index).context("processed log index overflows postgres")?;
        tx.execute(
            "insert into ugraph_processed_logs (deployment, source, template, address, block_number, transaction_index, log_index, topic0) values ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[&deployment, &log.source, &log.template, &log.address, &block_number, &transaction_index, &log_index, &log.topic0],
        )?;
    }
    write_postgres_history(&mut tx, deployment, &snapshot.history)?;
    tx.commit()?;
    Ok(())
}

fn load_postgres_history(
    client: &mut Client,
    deployment: &str,
) -> anyhow::Result<Vec<HistoricalSnapshot>> {
    let snapshot_rows = client.query(
        "select block_number, checkpoint, dynamic_sources, storage_mode from ugraph_history_snapshots where deployment = $1 order by block_number",
        &[&deployment],
    )?;
    if snapshot_rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut history = BTreeMap::new();
    let mut storage_modes = BTreeMap::new();
    for row in snapshot_rows {
        let block_number: i64 = row.get("block_number");
        let block_number = u64::try_from(block_number).context("history block is negative")?;
        let checkpoint_value: Value = row.get("checkpoint");
        let dynamic_sources_value: Value = row.get("dynamic_sources");
        let storage_mode: String = row.get("storage_mode");
        storage_modes.insert(block_number, storage_mode);
        history.insert(
            block_number,
            HistoricalSnapshot {
                checkpoint: serde_json::from_value(checkpoint_value)
                    .context("decoding postgres history checkpoint")?,
                entities: Vec::new(),
                dynamic_sources: serde_json::from_value(dynamic_sources_value)
                    .context("decoding postgres history dynamic sources")?,
            },
        );
    }

    let mut versions_by_block = BTreeMap::<u64, Vec<EntityVersionRow>>::new();
    for row in client.query(
        "select block_number, entity, id, data, removed from ugraph_entity_versions where deployment = $1 order by block_number, entity, id",
        &[&deployment],
    )? {
        let block_number: i64 = row.get("block_number");
        let block_number = u64::try_from(block_number).context("entity version block is negative")?;
        let data_value: Value = row.get("data");
        versions_by_block
            .entry(block_number)
            .or_default()
            .push(EntityVersionRow {
                entity: row.get("entity"),
                id: row.get("id"),
                data: serde_json::from_value(data_value)
                    .context("decoding postgres historical entity data")?,
                removed: row.get("removed"),
            });
    }

    let mut materialized = Vec::with_capacity(history.len());
    let mut entity_state = BTreeMap::<(String, String), EntityData>::new();
    for (block_number, mut snapshot) in history {
        let rows = versions_by_block.remove(&block_number).unwrap_or_default();
        match storage_modes
            .get(&block_number)
            .map(String::as_str)
            .unwrap_or("snapshot")
        {
            "delta" => {
                for row in rows {
                    let key = (row.entity, row.id);
                    if row.removed {
                        entity_state.remove(&key);
                    } else {
                        entity_state.insert(key, row.data);
                    }
                }
                snapshot.entities = entity_state
                    .iter()
                    .map(|((entity, id), data)| EntitySnapshot {
                        entity: entity.clone(),
                        id: id.clone(),
                        data: data.clone(),
                    })
                    .collect();
            }
            _ => {
                snapshot.entities = rows
                    .into_iter()
                    .filter(|row| !row.removed)
                    .map(|row| EntitySnapshot {
                        entity: row.entity,
                        id: row.id,
                        data: row.data,
                    })
                    .collect();
                entity_state = entity_map(&snapshot.entities);
            }
        }
        materialized.push(snapshot);
    }

    Ok(materialized)
}

struct EntityVersionRow {
    entity: String,
    id: String,
    data: EntityData,
    removed: bool,
}

fn entity_map(entities: &[EntitySnapshot]) -> BTreeMap<(String, String), EntityData> {
    entities
        .iter()
        .map(|entity| {
            (
                (entity.entity.clone(), entity.id.clone()),
                entity.data.clone(),
            )
        })
        .collect()
}

fn changed_entity_versions(
    previous: &BTreeMap<(String, String), EntityData>,
    current: &BTreeMap<(String, String), EntityData>,
) -> Vec<EntityVersionRow> {
    let mut rows = Vec::new();
    for ((entity, id), data) in current {
        if previous.get(&(entity.clone(), id.clone())) != Some(data) {
            rows.push(EntityVersionRow {
                entity: entity.clone(),
                id: id.clone(),
                data: data.clone(),
                removed: false,
            });
        }
    }
    for (entity, id) in previous.keys() {
        if !current.contains_key(&(entity.clone(), id.clone())) {
            rows.push(EntityVersionRow {
                entity: entity.clone(),
                id: id.clone(),
                data: EntityData::new(),
                removed: true,
            });
        }
    }
    rows
}

fn write_postgres_history(
    tx: &mut postgres::Transaction<'_>,
    deployment: &str,
    history: &[HistoricalSnapshot],
) -> anyhow::Result<()> {
    let retained_blocks = history
        .iter()
        .map(|snapshot| {
            i64::try_from(snapshot.checkpoint.to_block).context("history block overflows postgres")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if retained_blocks.is_empty() {
        tx.execute(
            "delete from ugraph_history_snapshots where deployment = $1",
            &[&deployment],
        )?;
    } else {
        tx.execute(
            "delete from ugraph_history_snapshots where deployment = $1 and not (block_number = any($2))",
            &[&deployment, &retained_blocks],
        )?;
    }

    let mut previous_entities = BTreeMap::<(String, String), EntityData>::new();
    for snapshot in history {
        let block_number = i64::try_from(snapshot.checkpoint.to_block)
            .context("history block overflows postgres")?;
        let checkpoint = serde_json::to_value(&snapshot.checkpoint)?;
        let dynamic_sources = serde_json::to_value(&snapshot.dynamic_sources)?;
        let entity_count = i32::try_from(snapshot.entities.len())
            .context("history entity count overflows postgres")?;
        let storage_mode = "delta";
        let block_hash = snapshot.checkpoint.block_hash.as_deref();
        tx.execute(
            r#"
            insert into ugraph_history_snapshots (
              deployment, block_number, block_hash, checkpoint, dynamic_sources,
              entity_count, storage_mode, updated_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, now())
            on conflict (deployment, block_number) do update set
              block_hash = excluded.block_hash,
              checkpoint = excluded.checkpoint,
              dynamic_sources = excluded.dynamic_sources,
              entity_count = excluded.entity_count,
              storage_mode = excluded.storage_mode,
              updated_at = now()
            "#,
            &[
                &deployment,
                &block_number,
                &block_hash,
                &checkpoint,
                &dynamic_sources,
                &entity_count,
                &storage_mode,
            ],
        )?;
        tx.execute(
            "delete from ugraph_entity_versions where deployment = $1 and block_number = $2",
            &[&deployment, &block_number],
        )?;
        let current_entities = entity_map(&snapshot.entities);
        for row in changed_entity_versions(&previous_entities, &current_entities) {
            let data = serde_json::to_value(&row.data)?;
            let removed = row.removed;
            tx.execute(
                "insert into ugraph_entity_versions (deployment, block_number, entity, id, data, removed) values ($1, $2, $3, $4, $5, $6)",
                &[&deployment, &block_number, &row.entity, &row.id, &data, &removed],
            )?;
        }
        previous_entities = current_entities;
    }
    Ok(())
}

pub fn migrate_postgres(url: &str) -> anyhow::Result<()> {
    let mut client = connect(url)?;
    migrate(&mut client)
}

pub fn register_feed_source_subscription(
    url: &str,
    deployment: &str,
    chain_id: u64,
    source: &SourcePlan,
) -> anyhow::Result<bool> {
    let Some(address) = source
        .address
        .as_deref()
        .filter(|address| !address.is_empty())
    else {
        return Ok(false);
    };
    if source.triggers.is_empty() {
        return Ok(false);
    }
    let mut client = connect(url)?;
    migrate(&mut client)?;
    register_feed_source_subscription_with_client(
        &mut client,
        deployment,
        chain_id,
        source,
        address,
    )
}

pub fn register_feed_source_subscriptions(
    url: &str,
    deployment: &str,
    chain_id: u64,
    sources: &[SourcePlan],
) -> anyhow::Result<usize> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let mut count = 0;
    for source in sources {
        let Some(address) = source
            .address
            .as_deref()
            .filter(|address| !address.is_empty())
        else {
            continue;
        };
        if source.triggers.is_empty() {
            continue;
        }
        if register_feed_source_subscription_with_client(
            &mut client,
            deployment,
            chain_id,
            source,
            address,
        )? {
            count += 1;
        }
    }
    Ok(count)
}

fn register_feed_source_subscription_with_client(
    client: &mut Client,
    deployment: &str,
    chain_id: u64,
    source: &SourcePlan,
    address: &str,
) -> anyhow::Result<bool> {
    let chain_id = i64::try_from(chain_id).context("chain id overflows postgres")?;
    let from_block = i64::try_from(source.start_block.unwrap_or(0))
        .context("subscription start block overflows postgres")?;
    let topic0s = source
        .triggers
        .iter()
        .map(|trigger| trigger.topic0.to_lowercase())
        .collect::<Vec<_>>();
    let topic0s_json = serde_json::to_value(&topic0s)?;
    let address = address.to_lowercase();
    let template = source.dynamic;
    let inserted = client.execute(
        r#"
        insert into ugraph_feed_subscriptions (
          chain_id, deployment, source, template, address, from_block, topic0s,
          active, updated_at
        )
        values ($1, $2, $3, $4, $5, $6, $7, true, now())
        on conflict (chain_id, deployment, source, template, address, from_block)
        do update set
          topic0s = excluded.topic0s,
          active = true,
          updated_at = now()
        "#,
        &[
            &chain_id,
            &deployment,
            &source.name,
            &template,
            &address,
            &from_block,
            &topic0s_json,
        ],
    )?;
    Ok(inserted > 0)
}

pub fn list_feed_subscriptions(url: &str, chain_id: u64) -> anyhow::Result<Vec<FeedSubscription>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let chain_id_i64 = i64::try_from(chain_id).context("chain id overflows postgres")?;
    client
        .query(
            r#"
            select chain_id, deployment, source, template, address, from_block,
              cursor_block, topic0s
            from ugraph_feed_subscriptions
            where chain_id = $1 and active = true
            order by deployment, source, address, from_block
            "#,
            &[&chain_id_i64],
        )?
        .into_iter()
        .map(row_to_feed_subscription)
        .collect()
}

fn row_to_feed_subscription(row: postgres::Row) -> anyhow::Result<FeedSubscription> {
    let chain_id: i64 = row.get("chain_id");
    let from_block: i64 = row.get("from_block");
    let cursor_block: Option<i64> = row.get("cursor_block");
    let topic0s_value: Value = row.get("topic0s");
    Ok(FeedSubscription {
        chain_id: u64::try_from(chain_id).context("subscription chain id is negative")?,
        deployment: row.get("deployment"),
        source: row.get("source"),
        template: row.get("template"),
        address: row.get("address"),
        from_block: u64::try_from(from_block).context("subscription from block is negative")?,
        cursor_block: cursor_block
            .map(|block| u64::try_from(block).context("subscription cursor block is negative"))
            .transpose()?,
        topic0s: serde_json::from_value(topic0s_value).context("decoding subscription topic0s")?,
    })
}

pub fn write_feed_logs(
    url: &str,
    subscription: &FeedSubscription,
    logs: &[RawEthereumLog],
    to_block: u64,
    to_block_hash: Option<&str>,
) -> anyhow::Result<u64> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let mut tx = client.transaction()?;
    let chain_id = i64::try_from(subscription.chain_id).context("chain id overflows postgres")?;
    let mut inserted = 0_u64;
    for log in logs {
        let Some(block_number) = log.block_number.as_deref().and_then(parse_rpc_u64) else {
            continue;
        };
        let Some(transaction_index) = log.transaction_index.as_deref().and_then(parse_rpc_u64)
        else {
            continue;
        };
        let Some(log_index) = log.log_index.as_deref().and_then(parse_rpc_u64) else {
            continue;
        };
        let Some(topic0) = log.topics.first() else {
            continue;
        };
        let block_number = i64::try_from(block_number).context("log block overflows postgres")?;
        let transaction_index =
            i64::try_from(transaction_index).context("log tx index overflows postgres")?;
        let log_index = i64::try_from(log_index).context("log index overflows postgres")?;
        let address = log.address.to_lowercase();
        let topic0 = topic0.to_lowercase();
        let topics = serde_json::to_value(&log.topics)?;
        inserted += tx.execute(
            r#"
            insert into ugraph_raw_logs (
              chain_id, block_number, block_hash, transaction_hash,
              transaction_index, log_index, address, topic0, topics, data
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            on conflict (chain_id, block_number, transaction_index, log_index)
            do nothing
            "#,
            &[
                &chain_id,
                &block_number,
                &log.block_hash,
                &log.transaction_hash,
                &transaction_index,
                &log_index,
                &address,
                &topic0,
                &topics,
                &log.data,
            ],
        )?;
    }
    let to_block_i64 = i64::try_from(to_block).context("cursor block overflows postgres")?;
    tx.execute(
        r#"
        insert into ugraph_raw_blocks (chain_id, block_number, block_hash, updated_at)
        values ($1, $2, $3, now())
        on conflict (chain_id, block_number) do update set
          block_hash = excluded.block_hash,
          updated_at = now()
        "#,
        &[&chain_id, &to_block_i64, &to_block_hash],
    )?;
    let from_block = i64::try_from(subscription.from_block)
        .context("subscription from block overflows postgres")?;
    tx.execute(
        r#"
        update ugraph_feed_subscriptions
        set cursor_block = $1, cursor_hash = $2, updated_at = now()
        where chain_id = $3
          and deployment = $4
          and source = $5
          and template = $6
          and address = $7
          and from_block = $8
        "#,
        &[
            &to_block_i64,
            &to_block_hash,
            &chain_id,
            &subscription.deployment,
            &subscription.source,
            &subscription.template,
            &subscription.address,
            &from_block,
        ],
    )?;
    tx.commit()?;
    Ok(inserted)
}

pub fn latest_feed_block(url: &str, chain_id: u64) -> anyhow::Result<Option<u64>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let chain_id = i64::try_from(chain_id).context("chain id overflows postgres")?;
    let row = client.query_one(
        "select max(cursor_block) as block from ugraph_feed_subscriptions where chain_id = $1",
        &[&chain_id],
    )?;
    let block: Option<i64> = row.get("block");
    block
        .map(|block| u64::try_from(block).context("feed cursor block is negative"))
        .transpose()
}

pub fn feed_block_hash(
    url: &str,
    chain_id: u64,
    block_number: u64,
) -> anyhow::Result<Option<String>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let chain_id = i64::try_from(chain_id).context("chain id overflows postgres")?;
    let block_number = i64::try_from(block_number).context("block number overflows postgres")?;
    let row = client.query_opt(
        "select block_hash from ugraph_raw_blocks where chain_id = $1 and block_number = $2",
        &[&chain_id, &block_number],
    )?;
    Ok(row.and_then(|row| row.get("block_hash")))
}

pub fn feed_source_caught_up(
    url: &str,
    deployment: &str,
    chain_id: u64,
    source: &SourcePlan,
    to_block: u64,
) -> anyhow::Result<bool> {
    let Some(address) = source
        .address
        .as_deref()
        .filter(|address| !address.is_empty())
    else {
        return Ok(true);
    };
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let chain_id = i64::try_from(chain_id).context("chain id overflows postgres")?;
    let from_block = i64::try_from(source.start_block.unwrap_or(0))
        .context("subscription start block overflows postgres")?;
    let to_block = i64::try_from(to_block).context("to block overflows postgres")?;
    let row = client.query_opt(
        r#"
        select cursor_block
        from ugraph_feed_subscriptions
        where chain_id = $1
          and deployment = $2
          and source = $3
          and template = $4
          and address = $5
          and from_block = $6
        "#,
        &[
            &chain_id,
            &deployment,
            &source.name,
            &source.dynamic,
            &address.to_lowercase(),
            &from_block,
        ],
    )?;
    let Some(row) = row else {
        return Ok(false);
    };
    let cursor_block: Option<i64> = row.get("cursor_block");
    Ok(cursor_block.is_some_and(|cursor| cursor >= to_block))
}

pub fn load_feed_source_report(
    url: &str,
    chain_id: u64,
    source: &SourcePlan,
    from_block: Option<u64>,
    to_block: u64,
) -> anyhow::Result<ScanSourceReport> {
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

    let mut client = connect(url)?;
    migrate(&mut client)?;
    let chain_id_i64 = i64::try_from(chain_id).context("chain id overflows postgres")?;
    let from_block_i64 = i64::try_from(from_block).context("from block overflows postgres")?;
    let to_block_i64 = i64::try_from(to_block).context("to block overflows postgres")?;
    let topic0s = source
        .triggers
        .iter()
        .map(|trigger| trigger.topic0.to_lowercase())
        .collect::<Vec<_>>();
    let trigger_by_topic = source
        .triggers
        .iter()
        .map(|trigger| (trigger.topic0.to_lowercase(), trigger))
        .collect::<BTreeMap<_, _>>();
    let mut logs = Vec::new();
    for row in client.query(
        r#"
        select block_number, block_hash, transaction_hash, transaction_index,
          log_index, address, topic0, topics, data
        from ugraph_raw_logs
        where chain_id = $1
          and address = $2
          and topic0 = any($3)
          and block_number between $4 and $5
        order by block_number, transaction_index, log_index
        "#,
        &[
            &chain_id_i64,
            &address.to_lowercase(),
            &topic0s,
            &from_block_i64,
            &to_block_i64,
        ],
    )? {
        let topic0: String = row.get("topic0");
        let Some(trigger) = trigger_by_topic.get(&topic0) else {
            continue;
        };
        let topics_value: Value = row.get("topics");
        let topics: Vec<String> =
            serde_json::from_value(topics_value).context("decoding raw log topics")?;
        let data: String = row.get("data");
        let block_number: i64 = row.get("block_number");
        let transaction_index: i64 = row.get("transaction_index");
        let log_index: i64 = row.get("log_index");
        logs.push(ugraph_core::MatchedLog {
            source: source.name.clone(),
            template: source.dynamic,
            handler: trigger.handler.clone(),
            signature: trigger.signature.clone(),
            network: source.network.clone(),
            topic0,
            address: row.get("address"),
            block_number: Some(u64::try_from(block_number).context("raw log block is negative")?),
            block_hash: row.get("block_hash"),
            transaction_hash: row.get("transaction_hash"),
            transaction_index: Some(
                u64::try_from(transaction_index).context("raw log tx index is negative")?,
            ),
            log_index: Some(u64::try_from(log_index).context("raw log index is negative")?),
            params: decode_event_params(&trigger.inputs, &topics, &data)
                .context("decoding raw feed log")?,
            topics,
            data,
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

const POSTGRES_SCHEMA: &str = r#"
create table if not exists ugraph_deployments (
  id text primary key,
  version integer not null,
  manifest text not null,
  checkpoint jsonb not null,
  schema jsonb not null,
  history jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now()
);

alter table ugraph_deployments
  add column if not exists history jsonb not null default '[]'::jsonb;

create table if not exists ugraph_entities (
  deployment text not null references ugraph_deployments(id) on delete cascade,
  entity text not null,
  id text not null,
  data jsonb not null,
  primary key (deployment, entity, id)
);

create index if not exists ugraph_entities_data_gin on ugraph_entities using gin (data);

create table if not exists ugraph_dynamic_sources (
  deployment text not null references ugraph_deployments(id) on delete cascade,
  name text not null,
  address text not null,
  created_at_block bigint not null,
  params jsonb not null,
  context jsonb not null default '{}'::jsonb,
  primary key (deployment, name, address, created_at_block)
);

alter table ugraph_dynamic_sources
  add column if not exists context jsonb not null default '{}'::jsonb;

create table if not exists ugraph_processed_logs (
  deployment text not null references ugraph_deployments(id) on delete cascade,
  source text not null,
  template boolean not null,
  address text not null,
  block_number bigint not null,
  transaction_index bigint not null,
  log_index bigint not null,
  topic0 text not null,
  primary key (
    deployment,
    source,
    template,
    address,
    block_number,
    transaction_index,
    log_index,
    topic0
  )
);

create table if not exists ugraph_history_snapshots (
  deployment text not null references ugraph_deployments(id) on delete cascade,
  block_number bigint not null,
  block_hash text,
  checkpoint jsonb not null,
  dynamic_sources jsonb not null,
  entity_count integer not null default 0,
  storage_mode text not null default 'snapshot',
  updated_at timestamptz not null default now(),
  primary key (deployment, block_number)
);

alter table ugraph_history_snapshots
  add column if not exists entity_count integer not null default 0;

alter table ugraph_history_snapshots
  add column if not exists storage_mode text not null default 'snapshot';

create index if not exists ugraph_history_snapshots_hash
  on ugraph_history_snapshots (deployment, block_hash);

create table if not exists ugraph_entity_versions (
  deployment text not null,
  block_number bigint not null,
  entity text not null,
  id text not null,
  data jsonb not null,
  removed boolean not null default false,
  primary key (deployment, block_number, entity, id),
  foreign key (deployment, block_number)
    references ugraph_history_snapshots(deployment, block_number)
    on delete cascade
);

alter table ugraph_entity_versions
  add column if not exists removed boolean not null default false;

create index if not exists ugraph_entity_versions_entity_id
  on ugraph_entity_versions (deployment, entity, id, block_number);

create index if not exists ugraph_entity_versions_data_gin
  on ugraph_entity_versions using gin (data);

create table if not exists ugraph_feed_subscriptions (
  chain_id bigint not null,
  deployment text not null,
  source text not null,
  template boolean not null,
  address text not null,
  from_block bigint not null,
  topic0s jsonb not null,
  cursor_block bigint,
  cursor_hash text,
  active boolean not null default true,
  updated_at timestamptz not null default now(),
  primary key (chain_id, deployment, source, template, address, from_block)
);

create index if not exists ugraph_feed_subscriptions_due
  on ugraph_feed_subscriptions (chain_id, active, cursor_block);

create table if not exists ugraph_raw_blocks (
  chain_id bigint not null,
  block_number bigint not null,
  block_hash text,
  updated_at timestamptz not null default now(),
  primary key (chain_id, block_number)
);

create table if not exists ugraph_raw_logs (
  chain_id bigint not null,
  block_number bigint not null,
  block_hash text,
  transaction_hash text,
  transaction_index bigint not null,
  log_index bigint not null,
  address text not null,
  topic0 text not null,
  topics jsonb not null,
  data text not null,
  inserted_at timestamptz not null default now(),
  primary key (chain_id, block_number, transaction_index, log_index)
);

create index if not exists ugraph_raw_logs_lookup
  on ugraph_raw_logs (chain_id, address, topic0, block_number, transaction_index, log_index);
"#;

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env};

    use ugraph_core::{EntityField, EntitySchema, EntityType};
    use ugraph_runtime::StoreValue;

    use super::*;

    #[test]
    fn postgres_schema_contains_current_state_tables() {
        for table in [
            "ugraph_deployments",
            "ugraph_entities",
            "ugraph_dynamic_sources",
            "ugraph_processed_logs",
            "ugraph_history_snapshots",
            "ugraph_entity_versions",
            "ugraph_feed_subscriptions",
            "ugraph_raw_blocks",
            "ugraph_raw_logs",
        ] {
            assert!(POSTGRES_SCHEMA.contains(table));
        }
        assert!(POSTGRES_SCHEMA.contains("storage_mode"));
        assert!(POSTGRES_SCHEMA.contains("removed boolean"));
    }

    #[test]
    fn compact_history_rows_track_changes_and_removals() {
        let mut previous = BTreeMap::new();
        let mut old_data = BTreeMap::new();
        old_data.insert("id".to_string(), StoreValue::Bytes("0xold".to_string()));
        previous.insert(("Protocol".to_string(), "0xold".to_string()), old_data);

        let mut current = BTreeMap::new();
        let mut new_data = BTreeMap::new();
        new_data.insert("id".to_string(), StoreValue::Bytes("0xnew".to_string()));
        current.insert(("Protocol".to_string(), "0xnew".to_string()), new_data);

        let rows = changed_entity_versions(&previous, &current);

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.id == "0xold" && row.removed));
        assert!(rows.iter().any(|row| row.id == "0xnew" && !row.removed));
    }

    #[test]
    fn postgres_roundtrip_when_url_is_set() -> anyhow::Result<()> {
        let Ok(url) = env::var("UGRAPH_TEST_POSTGRES_URL") else {
            return Ok(());
        };
        let deployment = format!("ugraph_test_{}", std::process::id());
        let store = SnapshotStore::Postgres {
            url: url.clone(),
            deployment: deployment.clone(),
        };
        let snapshot = fixture_snapshot();
        store.write(&snapshot)?;
        let loaded = store.load()?;
        assert_eq!(loaded.version, snapshot.version);
        assert_eq!(loaded.checkpoint.to_block, snapshot.checkpoint.to_block);
        assert_eq!(loaded.entities.len(), 1);
        assert_eq!(loaded.entities[0].entity, "Protocol");
        assert_eq!(loaded.dynamic_sources.len(), 1);
        assert_eq!(loaded.history.len(), snapshot.history.len());
        cleanup(&url, &deployment)?;
        Ok(())
    }

    fn cleanup(url: &str, deployment: &str) -> anyhow::Result<()> {
        let mut client = connect(url)?;
        client.execute(
            "delete from ugraph_deployments where id = $1",
            &[&deployment],
        )?;
        Ok(())
    }

    fn fixture_snapshot() -> StoreSnapshot {
        let mut schema = EntitySchema::default();
        schema.entities.insert(
            "Protocol".to_string(),
            EntityType {
                name: "Protocol".to_string(),
                fields: [EntityField {
                    name: "id".to_string(),
                    kind: "Bytes".to_string(),
                    list: false,
                    required: true,
                    derived: false,
                    derived_from: None,
                }]
                .into_iter()
                .map(|field| (field.name.clone(), field))
                .collect(),
            },
        );
        let mut data = BTreeMap::new();
        data.insert("id".to_string(), StoreValue::Bytes("0xabc".to_string()));
        let mut historical_data = BTreeMap::new();
        historical_data.insert("id".to_string(), StoreValue::Bytes("0xold".to_string()));
        StoreSnapshot {
            version: 1,
            manifest: "subgraph.yaml".to_string(),
            checkpoint: SyncCheckpoint {
                from_block: Some(1),
                to_block: 2,
                block_hash: Some("0xblock".to_string()),
                scanned_logs: 1,
                executed_logs: 1,
                validation_errors: 0,
                complete: true,
            },
            schema,
            entities: vec![EntitySnapshot {
                entity: "Protocol".to_string(),
                id: "0xabc".to_string(),
                data,
            }],
            dynamic_sources: vec![DynamicSourceSnapshot {
                name: "Campaign".to_string(),
                params: vec!["0xdef".to_string()],
                address: "0xdef".to_string(),
                created_at_block: 2,
                context: EntityData::new(),
            }],
            processed_logs: Vec::new(),
            history: vec![HistoricalSnapshot {
                checkpoint: SyncCheckpoint {
                    from_block: Some(1),
                    to_block: 1,
                    block_hash: Some("0xoldblock".to_string()),
                    scanned_logs: 1,
                    executed_logs: 1,
                    validation_errors: 0,
                    complete: true,
                },
                entities: vec![EntitySnapshot {
                    entity: "Protocol".to_string(),
                    id: "0xold".to_string(),
                    data: historical_data,
                }],
                dynamic_sources: Vec::new(),
            }],
        }
    }
}
