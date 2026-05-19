use std::{collections::BTreeMap, fs::File, io::Read, path::PathBuf};

use anyhow::Context;
use postgres::{Client, NoTls};
use serde_json::Value;
use tiny_keccak::{Hasher, Keccak};
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
    pub cursor_hash: Option<String>,
    pub topic0s: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FeedIngestReport {
    pub chain_id: u64,
    pub subscriptions: usize,
    pub to_block: Option<u64>,
    pub inserted_logs: u64,
    pub rollback: Option<FeedRollbackReport>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FeedRollbackReport {
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub deleted_blocks: u64,
    pub deleted_logs: u64,
    pub updated_subscriptions: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UserRecord {
    pub id: String,
    pub email: String,
    pub display_name: Option<String>,
    pub role: String,
    pub created_at: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ApiKeyRecord {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub prefix: String,
    pub scopes: Vec<String>,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CreatedApiKey {
    pub key: String,
    pub record: ApiKeyRecord,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeploymentMetadataRecord {
    pub deployment: String,
    pub version_label: Option<String>,
    pub visibility: String,
    pub owner_user_id: Option<String>,
    pub owner_email: Option<String>,
    pub created_by_key_id: Option<String>,
    pub created_by_key_prefix: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeploymentVersionRecord {
    pub deployment: String,
    pub version_label: String,
    pub storage_deployment: String,
    pub visibility: String,
    pub owner_user_id: Option<String>,
    pub owner_email: Option<String>,
    pub created_by_key_id: Option<String>,
    pub created_by_key_prefix: Option<String>,
    pub promoted_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

pub struct DeploymentVersionInput<'a> {
    pub deployment: &'a str,
    pub version_label: &'a str,
    pub storage_deployment: &'a str,
    pub visibility: &'a str,
    pub owner_email: Option<&'a str>,
    pub api_key: Option<&'a str>,
    pub promote: bool,
}

struct DeploymentVersionWrite<'a> {
    deployment: &'a str,
    version_label: &'a str,
    storage_deployment: &'a str,
    visibility: &'a str,
    owner_user_id: Option<&'a str>,
    created_by_key_id: Option<&'a str>,
    promote: bool,
}

#[derive(Debug, Clone)]
pub struct SyncBlockActivity {
    pub block_number: u64,
    pub block_hash: Option<String>,
    pub block_timestamp: Option<u64>,
    pub created: usize,
    pub updated: usize,
    pub removed: usize,
    pub changes: Vec<EntityChangeRecord>,
}

#[derive(Debug, Clone)]
pub struct EntityChangeRecord {
    pub entity: String,
    pub id: String,
    pub action: EntityChangeAction,
    pub data: EntityData,
    pub previous_data: Option<EntityData>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EntityChangeAction {
    Created,
    Updated,
    Removed,
}

#[derive(Debug, Clone)]
pub struct SyncActivityPage {
    pub activities: Vec<SyncBlockActivity>,
    pub stats: SyncActivityStats,
    pub page: usize,
    pub limit: usize,
    pub has_previous: bool,
    pub has_next: bool,
    pub show_empty: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SyncActivityStats {
    pub change_blocks: usize,
    pub entity_changes: usize,
    pub indexed_checkpoints: usize,
    pub created: usize,
    pub updated: usize,
    pub removed: usize,
}

impl EntityChangeAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Removed => "removed",
        }
    }
}

fn row_to_entity_change_action(value: &str) -> anyhow::Result<EntityChangeAction> {
    match value {
        "created" => Ok(EntityChangeAction::Created),
        "updated" => Ok(EntityChangeAction::Updated),
        "removed" => Ok(EntityChangeAction::Removed),
        other => anyhow::bail!("unknown entity change action `{other}`"),
    }
}

#[derive(Debug, Clone)]
pub struct StoreStatus {
    pub checkpoint: SyncCheckpoint,
    pub entities: usize,
    pub dynamic_sources: usize,
    pub history_snapshots: usize,
    pub history_earliest_block: Option<u64>,
    pub history_latest_block: Option<u64>,
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
                load_postgres_snapshot(&mut client, deployment, true)?
                    .with_context(|| format!("loading postgres snapshot `{deployment}`"))
            }
        }
    }

    pub fn load_current(&self) -> anyhow::Result<StoreSnapshot> {
        match self {
            Self::Json { path } => {
                let mut snapshot = load_snapshot(path)?;
                snapshot.processed_logs = Vec::new();
                snapshot.history = Vec::new();
                Ok(snapshot)
            }
            Self::Postgres { url, deployment } => {
                let mut client = connect(url)?;
                migrate(&mut client)?;
                load_postgres_snapshot(&mut client, deployment, false)?
                    .with_context(|| format!("loading postgres current snapshot `{deployment}`"))
            }
        }
    }

    pub fn try_load(&self) -> anyhow::Result<Option<StoreSnapshot>> {
        match self {
            Self::Json { path } => try_load_snapshot(path),
            Self::Postgres { url, deployment } => {
                let mut client = connect(url)?;
                migrate(&mut client)?;
                load_postgres_snapshot(&mut client, deployment, true)
            }
        }
    }

    pub fn status(&self) -> anyhow::Result<StoreStatus> {
        match self {
            Self::Json { path } => {
                let snapshot = load_snapshot(path)?;
                Ok(StoreStatus {
                    checkpoint: snapshot.checkpoint,
                    entities: snapshot.entities.len(),
                    dynamic_sources: snapshot.dynamic_sources.len(),
                    history_snapshots: snapshot.history.len(),
                    history_earliest_block: snapshot
                        .history
                        .iter()
                        .map(|entry| entry.checkpoint.to_block)
                        .min(),
                    history_latest_block: snapshot
                        .history
                        .iter()
                        .map(|entry| entry.checkpoint.to_block)
                        .max(),
                })
            }
            Self::Postgres { url, deployment } => {
                let mut client = connect(url)?;
                migrate(&mut client)?;
                load_postgres_status(&mut client, deployment)?
                    .with_context(|| format!("loading postgres status `{deployment}`"))
            }
        }
    }

    #[cfg(test)]
    pub fn write(&self, snapshot: &StoreSnapshot) -> anyhow::Result<()> {
        self.write_with_activity(snapshot, &snapshot.history, None, true)
    }

    pub fn write_with_activity(
        &self,
        snapshot: &StoreSnapshot,
        activity: &[HistoricalSnapshot],
        activity_baseline: Option<&StoreSnapshot>,
        replace_activity: bool,
    ) -> anyhow::Result<()> {
        match self {
            Self::Json { path } => write_snapshot(path, snapshot),
            Self::Postgres { url, deployment } => {
                let mut client = connect(url)?;
                migrate(&mut client)?;
                write_postgres_snapshot(
                    &mut client,
                    deployment,
                    snapshot,
                    activity,
                    activity_baseline,
                    replace_activity,
                )
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

pub fn create_or_update_user(
    url: &str,
    email: &str,
    display_name: Option<&str>,
    role: &str,
) -> anyhow::Result<UserRecord> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let normalized_email = normalize_email(email)?;
    let user_id = stable_id("user", &normalized_email);
    let row = client.query_one(
        r#"
        insert into ugraph_users (id, email, display_name, role)
        values ($1, $2, $3, $4)
        on conflict (email) do update
        set display_name = coalesce(excluded.display_name, ugraph_users.display_name),
            role = excluded.role
        returning id, email, display_name, role, created_at::text
        "#,
        &[&user_id, &normalized_email, &display_name, &role],
    )?;
    Ok(row_to_user(row))
}

pub fn list_users(url: &str) -> anyhow::Result<Vec<UserRecord>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    Ok(client
        .query(
            "select id, email, display_name, role, created_at::text from ugraph_users order by created_at, email",
            &[],
        )?
        .into_iter()
        .map(row_to_user)
        .collect())
}

pub fn create_api_key(
    url: &str,
    email: &str,
    name: &str,
    scopes: &[String],
) -> anyhow::Result<CreatedApiKey> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let normalized_email = normalize_email(email)?;
    let user = client
        .query_opt(
            "select id, email, display_name, role, created_at::text from ugraph_users where email = $1",
            &[&normalized_email],
        )?
        .map(row_to_user)
        .with_context(|| format!("user `{normalized_email}` does not exist"))?;
    let key = generate_api_key()?;
    let key_hash = hash_api_key(&key);
    let prefix = key_prefix(&key);
    let id = stable_id("key", &key_hash);
    let scopes_value = serde_json::to_value(scopes).context("encoding api key scopes")?;
    let row = client.query_one(
        r#"
        insert into ugraph_api_keys (id, user_id, name, prefix, key_hash, scopes)
        values ($1, $2, $3, $4, $5, $6)
        returning id, user_id, name, prefix, scopes, created_at::text, last_used_at::text, revoked_at::text
        "#,
        &[&id, &user.id, &name, &prefix, &key_hash, &scopes_value],
    )?;
    Ok(CreatedApiKey {
        key,
        record: row_to_api_key(row)?,
    })
}

pub fn verify_api_key(url: &str, key: &str) -> anyhow::Result<Option<UserRecord>> {
    verify_api_key_for_scope(url, key, None)
}

pub fn verify_api_key_scope(
    url: &str,
    key: &str,
    scope: &str,
) -> anyhow::Result<Option<UserRecord>> {
    verify_api_key_for_scope(url, key, Some(scope))
}

fn verify_api_key_for_scope(
    url: &str,
    key: &str,
    required_scope: Option<&str>,
) -> anyhow::Result<Option<UserRecord>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let key_hash = hash_api_key(key);
    let Some(row) = client.query_opt(
        r#"
        select u.id, u.email, u.display_name, u.role, u.created_at::text, k.id as key_id, k.scopes
        from ugraph_api_keys k
        join ugraph_users u on u.id = k.user_id
        where k.key_hash = $1 and k.revoked_at is null
        "#,
        &[&key_hash],
    )?
    else {
        return Ok(None);
    };
    let key_id: String = row.get("key_id");
    let scopes_value: Value = row.get("scopes");
    let scopes: Vec<String> =
        serde_json::from_value(scopes_value).context("decoding api key scopes")?;
    if let Some(scope) = required_scope {
        if !scopes
            .iter()
            .any(|candidate| candidate == "*" || candidate == scope)
        {
            return Ok(None);
        }
    }
    client.execute(
        "update ugraph_api_keys set last_used_at = now() where id = $1",
        &[&key_id],
    )?;
    Ok(Some(row_to_user(row)))
}

pub fn revoke_api_key(url: &str, prefix: &str) -> anyhow::Result<u64> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    Ok(client.execute(
        "update ugraph_api_keys set revoked_at = now() where prefix = $1 and revoked_at is null",
        &[&prefix],
    )?)
}

pub fn set_public_signup(url: &str, enabled: bool) -> anyhow::Result<()> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let value = serde_json::to_value(enabled)?;
    client.execute(
        r#"
        insert into ugraph_settings (key, value)
        values ('public_user_signup', $1)
        on conflict (key) do update set value = excluded.value, updated_at = now()
        "#,
        &[&value],
    )?;
    Ok(())
}

pub fn public_signup_enabled(url: &str) -> anyhow::Result<bool> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let row = client.query_one(
        "select value from ugraph_settings where key = 'public_user_signup'",
        &[],
    )?;
    let value: Value = row.get("value");
    Ok(value.as_bool().unwrap_or(false))
}

pub fn record_deployment_metadata(
    url: &str,
    deployment: &str,
    version_label: Option<&str>,
    visibility: &str,
    owner_email: Option<&str>,
    api_key: Option<&str>,
) -> anyhow::Result<DeploymentMetadataRecord> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let (owner_user_id, created_by_key_id) =
        resolve_metadata_actor(&mut client, owner_email, api_key)?;
    client.execute(
        r#"
        insert into ugraph_deployment_metadata
          (deployment, version_label, visibility, owner_user_id, created_by_key_id)
        values ($1, $2, $3, $4, $5)
        on conflict (deployment) do update
        set version_label = excluded.version_label,
            visibility = excluded.visibility,
            owner_user_id = coalesce(excluded.owner_user_id, ugraph_deployment_metadata.owner_user_id),
            created_by_key_id = coalesce(excluded.created_by_key_id, ugraph_deployment_metadata.created_by_key_id),
            updated_at = now()
        "#,
        &[
            &deployment,
            &version_label,
            &visibility,
            &owner_user_id,
            &created_by_key_id,
        ],
    )?;
    if let Some(version_label) = version_label {
        let deployment_exists = client
            .query_opt(
                "select 1 from ugraph_deployments where id = $1",
                &[&deployment],
            )?
            .is_some();
        if deployment_exists {
            record_deployment_version_with_client(
                &mut client,
                &DeploymentVersionWrite {
                    deployment,
                    version_label,
                    storage_deployment: deployment,
                    visibility,
                    owner_user_id: owner_user_id.as_deref(),
                    created_by_key_id: created_by_key_id.as_deref(),
                    promote: true,
                },
            )?;
        }
    }
    load_deployment_metadata(&mut client, deployment)?
        .with_context(|| format!("deployment metadata `{deployment}` was not stored"))
}

pub fn record_deployment_version(
    url: &str,
    input: DeploymentVersionInput<'_>,
) -> anyhow::Result<DeploymentVersionRecord> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let (owner_user_id, created_by_key_id) =
        resolve_metadata_actor(&mut client, input.owner_email, input.api_key)?;
    record_deployment_version_with_client(
        &mut client,
        &DeploymentVersionWrite {
            deployment: input.deployment,
            version_label: input.version_label,
            storage_deployment: input.storage_deployment,
            visibility: input.visibility,
            owner_user_id: owner_user_id.as_deref(),
            created_by_key_id: created_by_key_id.as_deref(),
            promote: input.promote,
        },
    )?;
    load_deployment_version(&mut client, input.deployment, input.version_label)?.with_context(
        || {
            format!(
                "deployment version `{}/{}` was not stored",
                input.deployment, input.version_label
            )
        },
    )
}

pub fn promote_deployment_version(
    url: &str,
    deployment: &str,
    version_label: &str,
) -> anyhow::Result<DeploymentVersionRecord> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let version =
        load_deployment_version(&mut client, deployment, version_label)?.with_context(|| {
            format!("deployment version `{deployment}/{version_label}` does not exist")
        })?;
    client.execute(
        r#"
        update ugraph_deployment_versions
        set promoted_at = now(), updated_at = now()
        where deployment = $1 and version_label = $2
        "#,
        &[&deployment, &version_label],
    )?;
    client.execute(
        r#"
        insert into ugraph_deployment_metadata
          (deployment, version_label, visibility, owner_user_id, created_by_key_id)
        values ($1, $2, $3, $4, $5)
        on conflict (deployment) do update
        set version_label = excluded.version_label,
            visibility = excluded.visibility,
            owner_user_id = coalesce(excluded.owner_user_id, ugraph_deployment_metadata.owner_user_id),
            created_by_key_id = coalesce(excluded.created_by_key_id, ugraph_deployment_metadata.created_by_key_id),
            updated_at = now()
        "#,
        &[
            &version.deployment,
            &version.version_label,
            &version.visibility,
            &version.owner_user_id,
            &version.created_by_key_id,
        ],
    )?;
    load_deployment_version(&mut client, deployment, version_label)?.with_context(|| {
        format!("deployment version `{deployment}/{version_label}` does not exist after promote")
    })
}

pub fn list_deployment_versions(
    url: &str,
    deployment: Option<&str>,
) -> anyhow::Result<Vec<DeploymentVersionRecord>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let rows = match deployment {
        Some(deployment) => client.query(
            &format!(
                "{DEPLOYMENT_VERSION_SELECT} where v.deployment = $1 order by v.updated_at desc, v.version_label desc"
            ),
            &[&deployment],
        )?,
        None => client.query(
            &format!(
                "{DEPLOYMENT_VERSION_SELECT} order by v.updated_at desc, v.deployment, v.version_label desc"
            ),
            &[],
        )?,
    };
    Ok(rows.into_iter().map(row_to_deployment_version).collect())
}

pub fn resolve_deployment_storage(
    url: &str,
    deployment: &str,
    version_label: &str,
) -> anyhow::Result<Option<String>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let requested_version = if version_label == "latest" {
        load_deployment_metadata(&mut client, deployment)?
            .and_then(|metadata| metadata.version_label)
    } else {
        Some(version_label.to_string())
    };
    let Some(requested_version) = requested_version else {
        return Ok(if version_label == "latest" {
            Some(deployment.to_string())
        } else {
            None
        });
    };
    if let Some(version) = load_deployment_version(&mut client, deployment, &requested_version)? {
        return Ok(Some(version.storage_deployment));
    }
    Ok(load_deployment_metadata(&mut client, deployment)?
        .filter(|metadata| metadata.version_label.as_deref() == Some(requested_version.as_str()))
        .map(|_| deployment.to_string()))
}

pub fn list_deployment_metadata(url: &str) -> anyhow::Result<Vec<DeploymentMetadataRecord>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    Ok(client
        .query(
            &format!("{DEPLOYMENT_METADATA_SELECT} order by m.updated_at desc, m.deployment"),
            &[],
        )?
        .into_iter()
        .map(row_to_deployment_metadata)
        .collect())
}

pub fn list_public_deployment_versions(url: &str) -> anyhow::Result<Vec<DeploymentVersionRecord>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    Ok(client
        .query(
            &format!(
                "{DEPLOYMENT_VERSION_SELECT} where v.visibility = 'public' order by v.deployment, v.version_label desc"
            ),
            &[],
        )?
        .into_iter()
        .map(row_to_deployment_version)
        .collect())
}

pub fn recent_sync_activity(
    url: &str,
    deployment: &str,
    page: usize,
    limit: usize,
    entity_limit: usize,
    show_empty: bool,
) -> anyhow::Result<SyncActivityPage> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let page = page.max(1);
    let limit = limit.clamp(1, 50);
    let offset = page.saturating_sub(1).saturating_mul(limit);
    let query_limit = i64::try_from(limit.saturating_add(1)).context("limit overflows postgres")?;
    let query_offset = i64::try_from(offset).context("offset overflows postgres")?;
    let stats = sync_activity_stats(&mut client, deployment)?;
    let mut block_rows = if show_empty {
        client.query(
            r#"
            select block_number, block_hash, block_timestamp
            from ugraph_sync_checkpoints
            where deployment = $1
            order by block_number desc
            limit $2 offset $3
            "#,
            &[&deployment, &query_limit, &query_offset],
        )?
    } else {
        client.query(
            r#"
            select
              changed.block_number,
              coalesce(c.block_hash, changed.block_hash) as block_hash,
              coalesce(c.block_timestamp, changed.block_timestamp) as block_timestamp
            from (
              select block_number, min(block_hash) as block_hash, min(block_timestamp) as block_timestamp
              from ugraph_entity_changes
              where deployment = $1
              group by block_number
            ) changed
            left join ugraph_sync_checkpoints c
              on c.deployment = $1
             and c.block_number = changed.block_number
            order by changed.block_number desc
            limit $2 offset $3
            "#,
            &[&deployment, &query_limit, &query_offset],
        )?
    };
    let has_next = block_rows.len() > limit;
    if has_next {
        block_rows.truncate(limit);
    }
    let block_numbers = block_rows
        .iter()
        .map(|row| {
            let block_number: i64 = row.get("block_number");
            u64::try_from(block_number).context("activity block is negative")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let postgres_blocks = block_numbers
        .iter()
        .map(|block| i64::try_from(*block).context("activity block overflows postgres"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut blocks = BTreeMap::<u64, SyncBlockActivity>::new();
    for row in block_rows {
        let block_number: i64 = row.get("block_number");
        let block_number = u64::try_from(block_number).context("activity block is negative")?;
        let block_timestamp: Option<i64> = row.get("block_timestamp");
        blocks.insert(
            block_number,
            SyncBlockActivity {
                block_number,
                block_hash: row.get("block_hash"),
                block_timestamp: block_timestamp
                    .map(|timestamp| {
                        u64::try_from(timestamp).context("block timestamp is negative")
                    })
                    .transpose()?,
                created: 0,
                updated: 0,
                removed: 0,
                changes: Vec::new(),
            },
        );
    }
    if postgres_blocks.is_empty() {
        return Ok(SyncActivityPage {
            activities: Vec::new(),
            stats,
            page,
            limit,
            has_previous: page > 1,
            has_next: false,
            show_empty,
        });
    }
    let rows = client.query(
        r#"
        select
          v.block_number,
          v.entity,
          v.id,
          v.action,
          v.data,
          v.previous_data
        from ugraph_entity_changes v
        where v.deployment = $1
          and v.block_number = any($2)
        order by v.block_number desc, v.entity, v.id
        "#,
        &[&deployment, &postgres_blocks],
    )?;
    for row in rows {
        let block_number: i64 = row.get("block_number");
        let block_number = u64::try_from(block_number).context("activity block is negative")?;
        let Some(activity) = blocks.get_mut(&block_number) else {
            continue;
        };
        let entity = row.get::<_, String>("entity");
        let id = row.get::<_, String>("id");
        let action = row_to_entity_change_action(row.get::<_, String>("action").as_str())?;
        let data_value: Value = row.get("data");
        let data = serde_json::from_value(data_value).context("decoding activity entity data")?;
        let previous_data_value: Option<Value> = row.get("previous_data");
        let previous_data = previous_data_value
            .map(serde_json::from_value)
            .transpose()
            .context("decoding previous activity entity data")?;
        match action {
            EntityChangeAction::Created => activity.created += 1,
            EntityChangeAction::Updated => activity.updated += 1,
            EntityChangeAction::Removed => activity.removed += 1,
        }
        if activity.changes.len() < entity_limit {
            activity.changes.push(EntityChangeRecord {
                entity,
                id,
                action,
                data,
                previous_data,
            });
        }
    }
    Ok(SyncActivityPage {
        activities: block_numbers
            .into_iter()
            .filter_map(|block| blocks.remove(&block))
            .collect(),
        stats,
        page,
        limit,
        has_previous: page > 1,
        has_next,
        show_empty,
    })
}

fn sync_activity_stats(client: &mut Client, deployment: &str) -> anyhow::Result<SyncActivityStats> {
    let row = client.query_one(
        r#"
        select
          count(distinct block_number)::bigint as change_blocks,
          count(*)::bigint as entity_changes,
          count(*) filter (where action = 'created')::bigint as created,
          count(*) filter (where action = 'updated')::bigint as updated,
          count(*) filter (where action = 'removed')::bigint as removed
        from ugraph_entity_changes
        where deployment = $1
        "#,
        &[&deployment],
    )?;
    let checkpoint_row = client.query_one(
        "select count(*)::bigint from ugraph_sync_checkpoints where deployment = $1",
        &[&deployment],
    )?;
    Ok(SyncActivityStats {
        change_blocks: count_usize(&row, 0)?,
        entity_changes: count_usize(&row, 1)?,
        created: count_usize(&row, 2)?,
        updated: count_usize(&row, 3)?,
        removed: count_usize(&row, 4)?,
        indexed_checkpoints: count_usize(&checkpoint_row, 0)?,
    })
}

pub fn deployment_metadata(
    url: &str,
    deployment: &str,
) -> anyhow::Result<Option<DeploymentMetadataRecord>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    load_deployment_metadata(&mut client, deployment)
}

pub fn deployment_visibility(url: &str, deployment: &str) -> anyhow::Result<Option<String>> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    Ok(client
        .query_opt(
            "select visibility from ugraph_deployment_metadata where deployment = $1",
            &[&deployment],
        )?
        .map(|row| row.get("visibility")))
}

pub fn set_deployment_visibility(
    url: &str,
    deployment: &str,
    visibility: &str,
) -> anyhow::Result<DeploymentMetadataRecord> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    client.execute(
        r#"
        insert into ugraph_deployment_metadata (deployment, visibility)
        values ($1, $2)
        on conflict (deployment) do update
        set visibility = excluded.visibility, updated_at = now()
        "#,
        &[&deployment, &visibility],
    )?;
    load_deployment_metadata(&mut client, deployment)?
        .with_context(|| format!("deployment metadata `{deployment}` was not stored"))
}

fn load_deployment_metadata(
    client: &mut Client,
    deployment: &str,
) -> anyhow::Result<Option<DeploymentMetadataRecord>> {
    Ok(client
        .query_opt(
            &format!("{DEPLOYMENT_METADATA_SELECT} where m.deployment = $1"),
            &[&deployment],
        )?
        .map(row_to_deployment_metadata))
}

fn record_deployment_version_with_client(
    client: &mut Client,
    input: &DeploymentVersionWrite<'_>,
) -> anyhow::Result<()> {
    client
        .query_opt(
            "select id from ugraph_deployments where id = $1",
            &[&input.storage_deployment],
        )?
        .with_context(|| {
            format!(
                "storage deployment `{}` does not exist",
                input.storage_deployment
            )
        })?;
    client.execute(
        r#"
        insert into ugraph_deployment_versions
          (deployment, version_label, storage_deployment, visibility, owner_user_id,
           created_by_key_id, promoted_at)
        values ($1, $2, $3, $4, $5, $6, case when $7 then now() else null end)
        on conflict (deployment, version_label) do update
        set storage_deployment = excluded.storage_deployment,
            visibility = excluded.visibility,
            owner_user_id = coalesce(excluded.owner_user_id, ugraph_deployment_versions.owner_user_id),
            created_by_key_id = coalesce(excluded.created_by_key_id, ugraph_deployment_versions.created_by_key_id),
            promoted_at = case
              when $7 then coalesce(ugraph_deployment_versions.promoted_at, now())
              else ugraph_deployment_versions.promoted_at
            end,
            updated_at = now()
        "#,
        &[
            &input.deployment,
            &input.version_label,
            &input.storage_deployment,
            &input.visibility,
            &input.owner_user_id,
            &input.created_by_key_id,
            &input.promote,
        ],
    )?;
    if input.promote {
        client.execute(
            r#"
            insert into ugraph_deployment_metadata
              (deployment, version_label, visibility, owner_user_id, created_by_key_id)
            values ($1, $2, $3, $4, $5)
            on conflict (deployment) do update
            set version_label = excluded.version_label,
                visibility = excluded.visibility,
                owner_user_id = coalesce(excluded.owner_user_id, ugraph_deployment_metadata.owner_user_id),
                created_by_key_id = coalesce(excluded.created_by_key_id, ugraph_deployment_metadata.created_by_key_id),
                updated_at = now()
            "#,
            &[
                &input.deployment,
                &input.version_label,
                &input.visibility,
                &input.owner_user_id,
                &input.created_by_key_id,
            ],
        )?;
    }
    Ok(())
}

fn load_deployment_version(
    client: &mut Client,
    deployment: &str,
    version_label: &str,
) -> anyhow::Result<Option<DeploymentVersionRecord>> {
    Ok(client
        .query_opt(
            &format!(
                "{DEPLOYMENT_VERSION_SELECT} where v.deployment = $1 and v.version_label = $2"
            ),
            &[&deployment, &version_label],
        )?
        .map(row_to_deployment_version))
}

const DEPLOYMENT_METADATA_SELECT: &str = r#"
        select
          m.deployment,
          m.version_label,
          m.visibility,
          m.owner_user_id,
          owner.email as owner_email,
          m.created_by_key_id,
          key.prefix as created_by_key_prefix,
          m.created_at::text,
          m.updated_at::text
        from ugraph_deployment_metadata m
        left join ugraph_users owner on owner.id = m.owner_user_id
        left join ugraph_api_keys key on key.id = m.created_by_key_id
"#;

const DEPLOYMENT_VERSION_SELECT: &str = r#"
        select
          v.deployment,
          v.version_label,
          v.storage_deployment,
          v.visibility,
          v.owner_user_id,
          owner.email as owner_email,
          v.created_by_key_id,
          key.prefix as created_by_key_prefix,
          v.promoted_at::text,
          v.created_at::text,
          v.updated_at::text
        from ugraph_deployment_versions v
        left join ugraph_users owner on owner.id = v.owner_user_id
        left join ugraph_api_keys key on key.id = v.created_by_key_id
"#;

fn resolve_metadata_actor(
    client: &mut Client,
    owner_email: Option<&str>,
    api_key: Option<&str>,
) -> anyhow::Result<(Option<String>, Option<String>)> {
    let explicit_owner_user_id = match owner_email {
        Some(email) => {
            let normalized_email = normalize_email(email)?;
            Some(
                client
                    .query_opt(
                        "select id from ugraph_users where email = $1",
                        &[&normalized_email],
                    )?
                    .map(|row| row.get::<_, String>("id"))
                    .with_context(|| format!("owner user `{normalized_email}` does not exist"))?,
            )
        }
        None => None,
    };
    let (created_by_key_id, key_owner_user_id) = match api_key {
        Some(key) if !key.trim().is_empty() => {
            let key_hash = hash_api_key(key);
            let row = client
                .query_opt(
                    "select id, user_id from ugraph_api_keys where key_hash = $1 and revoked_at is null",
                    &[&key_hash],
                )?
                .context("api key is invalid or revoked")?;
            (
                Some(row.get::<_, String>("id")),
                Some(row.get::<_, String>("user_id")),
            )
        }
        _ => (None, None),
    };
    Ok((
        explicit_owner_user_id.or(key_owner_user_id),
        created_by_key_id,
    ))
}

fn normalize_email(email: &str) -> anyhow::Result<String> {
    let email = email.trim().to_ascii_lowercase();
    if email.is_empty() || !email.contains('@') {
        anyhow::bail!("invalid email `{email}`");
    }
    Ok(email)
}

fn stable_id(prefix: &str, input: &str) -> String {
    format!("{prefix}_{}", &hash_api_key(input)[0..24])
}

fn generate_api_key() -> anyhow::Result<String> {
    let mut bytes = [0_u8; 32];
    File::open("/dev/urandom")
        .context("opening /dev/urandom")?
        .read_exact(&mut bytes)
        .context("reading random api key bytes")?;
    Ok(format!("ugraph_{}", hex::encode(bytes)))
}

fn key_prefix(key: &str) -> String {
    key.chars().take(18).collect()
}

fn hash_api_key(key: &str) -> String {
    let mut hasher = Keccak::v256();
    hasher.update(key.as_bytes());
    let mut out = [0_u8; 32];
    hasher.finalize(&mut out);
    hex::encode(out)
}

fn row_to_user(row: postgres::Row) -> UserRecord {
    UserRecord {
        id: row.get("id"),
        email: row.get("email"),
        display_name: row.get("display_name"),
        role: row.get("role"),
        created_at: row.get("created_at"),
    }
}

fn row_to_api_key(row: postgres::Row) -> anyhow::Result<ApiKeyRecord> {
    let scopes_value: Value = row.get("scopes");
    Ok(ApiKeyRecord {
        id: row.get("id"),
        user_id: row.get("user_id"),
        name: row.get("name"),
        prefix: row.get("prefix"),
        scopes: serde_json::from_value(scopes_value).context("decoding api key scopes")?,
        created_at: row.get("created_at"),
        last_used_at: row.get("last_used_at"),
        revoked_at: row.get("revoked_at"),
    })
}

fn row_to_deployment_metadata(row: postgres::Row) -> DeploymentMetadataRecord {
    DeploymentMetadataRecord {
        deployment: row.get("deployment"),
        version_label: row.get("version_label"),
        visibility: row.get("visibility"),
        owner_user_id: row.get("owner_user_id"),
        owner_email: row.get("owner_email"),
        created_by_key_id: row.get("created_by_key_id"),
        created_by_key_prefix: row.get("created_by_key_prefix"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_deployment_version(row: postgres::Row) -> DeploymentVersionRecord {
    DeploymentVersionRecord {
        deployment: row.get("deployment"),
        version_label: row.get("version_label"),
        storage_deployment: row.get("storage_deployment"),
        visibility: row.get("visibility"),
        owner_user_id: row.get("owner_user_id"),
        owner_email: row.get("owner_email"),
        created_by_key_id: row.get("created_by_key_id"),
        created_by_key_prefix: row.get("created_by_key_prefix"),
        promoted_at: row.get("promoted_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn migrate(client: &mut Client) -> anyhow::Result<()> {
    let key = "ugraph:schema";
    client.query_one("select pg_advisory_lock(hashtextextended($1, 0))", &[&key])?;
    let schema_result = client.batch_execute(POSTGRES_SCHEMA);
    let unlock_result = client.query_one(
        "select pg_advisory_unlock(hashtextextended($1, 0))",
        &[&key],
    );
    schema_result?;
    unlock_result?;
    Ok(())
}

fn load_postgres_snapshot(
    client: &mut Client,
    deployment: &str,
    include_history: bool,
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
    let history = if include_history {
        let legacy_history: Vec<HistoricalSnapshot> =
            serde_json::from_value(history_value).context("decoding postgres history")?;
        let stored_history = load_postgres_history(client, deployment)?;
        if stored_history.is_empty() {
            legacy_history
        } else {
            stored_history
        }
    } else {
        Vec::new()
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

    let processed_logs = if include_history {
        client
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
                    block_number: u64::try_from(block_number)
                        .context("processed log block is negative")?,
                    transaction_index: u64::try_from(transaction_index)
                        .context("processed log transaction index is negative")?,
                    log_index: u64::try_from(log_index)
                        .context("processed log index is negative")?,
                    topic0: row.get("topic0"),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

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

fn load_postgres_status(
    client: &mut Client,
    deployment: &str,
) -> anyhow::Result<Option<StoreStatus>> {
    let Some(row) = client.query_opt(
        "select checkpoint from ugraph_deployments where id = $1",
        &[&deployment],
    )?
    else {
        return Ok(None);
    };
    let checkpoint_value: Value = row.get("checkpoint");
    let checkpoint: SyncCheckpoint =
        serde_json::from_value(checkpoint_value).context("decoding postgres checkpoint")?;
    let entity_row = client.query_one(
        "select count(*)::bigint from ugraph_entities where deployment = $1",
        &[&deployment],
    )?;
    let entities = count_usize(&entity_row, 0)?;
    let dynamic_source_row = client.query_one(
        "select count(*)::bigint from ugraph_dynamic_sources where deployment = $1",
        &[&deployment],
    )?;
    let dynamic_sources = count_usize(&dynamic_source_row, 0)?;
    let history_row = client.query_one(
        "select count(*)::bigint, min(block_number), max(block_number) from ugraph_history_snapshots where deployment = $1",
        &[&deployment],
    )?;
    let history_snapshots = count_usize(&history_row, 0)?;
    let history_earliest_block = optional_u64(history_row.get::<_, Option<i64>>(1))?;
    let history_latest_block = optional_u64(history_row.get::<_, Option<i64>>(2))?;
    Ok(Some(StoreStatus {
        checkpoint,
        entities,
        dynamic_sources,
        history_snapshots,
        history_earliest_block,
        history_latest_block,
    }))
}

fn count_usize(row: &postgres::Row, column: usize) -> anyhow::Result<usize> {
    let count: i64 = row.get(column);
    usize::try_from(count).context("postgres count is negative")
}

fn optional_u64(value: Option<i64>) -> anyhow::Result<Option<u64>> {
    value
        .map(|value| u64::try_from(value).context("postgres block is negative"))
        .transpose()
}

fn write_postgres_snapshot(
    client: &mut Client,
    deployment: &str,
    snapshot: &StoreSnapshot,
    activity: &[HistoricalSnapshot],
    activity_baseline: Option<&StoreSnapshot>,
    replace_activity: bool,
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

    write_postgres_activity(
        &mut tx,
        deployment,
        activity,
        activity_baseline,
        replace_activity,
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
                previous_data: None,
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
    previous_data: Option<EntityData>,
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
        let previous_data = previous.get(&(entity.clone(), id.clone()));
        if previous_data != Some(data) {
            rows.push(EntityVersionRow {
                entity: entity.clone(),
                id: id.clone(),
                data: data.clone(),
                previous_data: previous_data.cloned(),
                removed: false,
            });
        }
    }
    for ((entity, id), data) in previous {
        if !current.contains_key(&(entity.clone(), id.clone())) {
            rows.push(EntityVersionRow {
                entity: entity.clone(),
                id: id.clone(),
                data: EntityData::new(),
                previous_data: Some(data.clone()),
                removed: true,
            });
        }
    }
    rows
}

fn write_postgres_activity(
    tx: &mut postgres::Transaction<'_>,
    deployment: &str,
    activity: &[HistoricalSnapshot],
    activity_baseline: Option<&StoreSnapshot>,
    replace_activity: bool,
) -> anyhow::Result<()> {
    if replace_activity {
        tx.execute(
            "delete from ugraph_entity_changes where deployment = $1",
            &[&deployment],
        )?;
        tx.execute(
            "delete from ugraph_sync_checkpoints where deployment = $1",
            &[&deployment],
        )?;
    } else if let Some(first_block) = activity
        .iter()
        .map(|snapshot| snapshot.checkpoint.to_block)
        .min()
    {
        let first_block =
            i64::try_from(first_block).context("activity pruning block overflows postgres")?;
        tx.execute(
            "delete from ugraph_entity_changes where deployment = $1 and block_number >= $2",
            &[&deployment, &first_block],
        )?;
        tx.execute(
            "delete from ugraph_sync_checkpoints where deployment = $1 and block_number >= $2",
            &[&deployment, &first_block],
        )?;
    }

    let mut previous_entities = if replace_activity {
        BTreeMap::new()
    } else if let Some(baseline) = activity_baseline {
        entity_map(&baseline.entities)
    } else {
        load_current_entity_map(tx, deployment)?
    };
    let mut ordered_activity = BTreeMap::new();
    for snapshot in activity {
        ordered_activity.insert(snapshot.checkpoint.to_block, snapshot.clone());
    }

    for snapshot in ordered_activity.into_values() {
        let block_number = i64::try_from(snapshot.checkpoint.to_block)
            .context("activity block overflows postgres")?;
        let from_block = snapshot
            .checkpoint
            .from_block
            .map(i64::try_from)
            .transpose()
            .context("activity from block overflows postgres")?;
        let block_hash = snapshot.checkpoint.block_hash.as_deref();
        let block_timestamp = snapshot
            .checkpoint
            .block_timestamp
            .map(i64::try_from)
            .transpose()
            .context("activity block timestamp overflows postgres")?;
        let scanned_logs = i64::try_from(snapshot.checkpoint.scanned_logs)
            .context("activity scanned log count overflows postgres")?;
        let executed_logs = i64::try_from(snapshot.checkpoint.executed_logs)
            .context("activity executed log count overflows postgres")?;
        let validation_errors = i64::try_from(snapshot.checkpoint.validation_errors)
            .context("activity validation error count overflows postgres")?;
        let complete = snapshot.checkpoint.complete;
        let current_entities = entity_map(&snapshot.entities);
        let changes = changed_entity_versions(&previous_entities, &current_entities);
        let entity_changes = i32::try_from(changes.len())
            .context("activity entity change count overflows postgres")?;

        tx.execute(
            r#"
            insert into ugraph_sync_checkpoints (
              deployment, block_number, block_hash, block_timestamp, from_block,
              scanned_logs, executed_logs, validation_errors, complete,
              entity_changes, synced_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
            on conflict (deployment, block_number) do update set
              block_hash = coalesce(excluded.block_hash, ugraph_sync_checkpoints.block_hash),
              block_timestamp = coalesce(excluded.block_timestamp, ugraph_sync_checkpoints.block_timestamp),
              from_block = coalesce(excluded.from_block, ugraph_sync_checkpoints.from_block),
              scanned_logs = excluded.scanned_logs,
              executed_logs = excluded.executed_logs,
              validation_errors = excluded.validation_errors,
              complete = excluded.complete,
              entity_changes = excluded.entity_changes,
              synced_at = now()
            "#,
            &[
                &deployment,
                &block_number,
                &block_hash,
                &block_timestamp,
                &from_block,
                &scanned_logs,
                &executed_logs,
                &validation_errors,
                &complete,
                &entity_changes,
            ],
        )?;

        for row in changes {
            let key = (row.entity.clone(), row.id.clone());
            let action = if row.removed {
                EntityChangeAction::Removed
            } else if previous_entities.contains_key(&key) {
                EntityChangeAction::Updated
            } else {
                EntityChangeAction::Created
            };
            let action = action.as_str();
            let data = serde_json::to_value(&row.data)?;
            let previous_data = row
                .previous_data
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?;
            tx.execute(
                r#"
                insert into ugraph_entity_changes (
                  deployment, block_number, block_hash, block_timestamp,
                  entity, id, action, data, previous_data, updated_at
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
                on conflict (deployment, block_number, entity, id) do update set
                  block_hash = coalesce(excluded.block_hash, ugraph_entity_changes.block_hash),
                  block_timestamp = coalesce(excluded.block_timestamp, ugraph_entity_changes.block_timestamp),
                  action = excluded.action,
                  data = excluded.data,
                  previous_data = excluded.previous_data,
                  updated_at = now()
                "#,
                &[
                    &deployment,
                    &block_number,
                    &block_hash,
                    &block_timestamp,
                    &row.entity,
                    &row.id,
                    &action,
                    &data,
                    &previous_data,
                ],
            )?;
        }

        previous_entities = current_entities;
    }
    Ok(())
}

fn load_current_entity_map(
    tx: &mut postgres::Transaction<'_>,
    deployment: &str,
) -> anyhow::Result<BTreeMap<(String, String), EntityData>> {
    let mut entities = BTreeMap::new();
    for row in tx.query(
        "select entity, id, data from ugraph_entities where deployment = $1 order by entity, id",
        &[&deployment],
    )? {
        let data_value: Value = row.get("data");
        entities.insert(
            (row.get("entity"), row.get("id")),
            serde_json::from_value(data_value).context("decoding postgres entity data")?,
        );
    }
    Ok(entities)
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
              cursor_block, cursor_hash, topic0s
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
        cursor_hash: row.get("cursor_hash"),
        topic0s: serde_json::from_value(topic0s_value).context("decoding subscription topic0s")?,
    })
}

pub fn rollback_feed_chain(
    url: &str,
    chain_id: u64,
    from_block: u64,
) -> anyhow::Result<FeedRollbackReport> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    let mut tx = client.transaction()?;
    let chain_id_i64 = i64::try_from(chain_id).context("chain id overflows postgres")?;
    let from_block_i64 = i64::try_from(from_block).context("rollback block overflows postgres")?;
    let max_raw_block = tx
        .query_one(
            r#"
            select greatest(
              coalesce((select max(block_number) from ugraph_raw_blocks where chain_id = $1 and block_number >= $2), -1),
              coalesce((select max(block_number) from ugraph_raw_logs where chain_id = $1 and block_number >= $2), -1),
              coalesce((select max(cursor_block) from ugraph_feed_subscriptions where chain_id = $1 and cursor_block >= $2), -1)
            )
            "#,
            &[&chain_id_i64, &from_block_i64],
        )?
        .get::<_, i64>(0);
    let to_block = if max_raw_block >= 0 {
        Some(u64::try_from(max_raw_block).context("rollback max block is negative")?)
    } else {
        None
    };
    let rollback_cursor_block = from_block
        .checked_sub(1)
        .map(|block| i64::try_from(block).context("rollback cursor block overflows postgres"))
        .transpose()?;
    let rollback_cursor_hash: Option<String> = match rollback_cursor_block {
        Some(block) => tx
            .query_opt(
                "select block_hash from ugraph_raw_blocks where chain_id = $1 and block_number = $2",
                &[&chain_id_i64, &block],
            )?
            .and_then(|row| row.get::<_, Option<String>>("block_hash")),
        None => None,
    };
    let deleted_logs = tx.execute(
        "delete from ugraph_raw_logs where chain_id = $1 and block_number >= $2",
        &[&chain_id_i64, &from_block_i64],
    )?;
    let deleted_blocks = tx.execute(
        "delete from ugraph_raw_blocks where chain_id = $1 and block_number >= $2",
        &[&chain_id_i64, &from_block_i64],
    )?;
    let updated_subscriptions = match rollback_cursor_block {
        Some(block) => tx.execute(
            r#"
            update ugraph_feed_subscriptions
            set cursor_block = $1, cursor_hash = $2, updated_at = now()
            where chain_id = $3 and cursor_block >= $4
            "#,
            &[
                &block,
                &rollback_cursor_hash,
                &chain_id_i64,
                &from_block_i64,
            ],
        )?,
        None => tx.execute(
            r#"
            update ugraph_feed_subscriptions
            set cursor_block = null, cursor_hash = null, updated_at = now()
            where chain_id = $1 and cursor_block >= $2
            "#,
            &[&chain_id_i64, &from_block_i64],
        )?,
    };
    tx.commit()?;
    Ok(FeedRollbackReport {
        chain_id,
        from_block,
        to_block,
        deleted_blocks,
        deleted_logs,
        updated_subscriptions,
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
            block_timestamp: None,
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

create table if not exists ugraph_users (
  id text primary key,
  email text not null unique,
  display_name text,
  role text not null default 'member',
  created_at timestamptz not null default now()
);

create table if not exists ugraph_api_keys (
  id text primary key,
  user_id text not null references ugraph_users(id) on delete cascade,
  name text not null,
  prefix text not null unique,
  key_hash text not null unique,
  scopes jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  last_used_at timestamptz,
  revoked_at timestamptz
);

create index if not exists ugraph_api_keys_user
  on ugraph_api_keys (user_id, revoked_at);

create table if not exists ugraph_settings (
  key text primary key,
  value jsonb not null,
  updated_at timestamptz not null default now()
);

insert into ugraph_settings (key, value)
values ('public_user_signup', 'false'::jsonb)
on conflict (key) do nothing;

create table if not exists ugraph_deployment_metadata (
  deployment text primary key,
  version_label text,
  visibility text not null default 'private',
  owner_user_id text references ugraph_users(id),
  created_by_key_id text references ugraph_api_keys(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (visibility in ('private', 'public'))
);

alter table ugraph_deployment_metadata
  drop constraint if exists ugraph_deployment_metadata_deployment_fkey;

create table if not exists ugraph_deployment_versions (
  deployment text not null,
  version_label text not null,
  storage_deployment text not null references ugraph_deployments(id) on delete cascade,
  visibility text not null default 'private',
  owner_user_id text references ugraph_users(id),
  created_by_key_id text references ugraph_api_keys(id),
  promoted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (deployment, version_label),
  unique (deployment, storage_deployment),
  check (visibility in ('private', 'public'))
);

create index if not exists ugraph_deployment_versions_storage
  on ugraph_deployment_versions (storage_deployment);

insert into ugraph_deployment_versions (
  deployment, version_label, storage_deployment, visibility, owner_user_id,
  created_by_key_id, promoted_at, created_at, updated_at
)
select
  deployment, version_label, deployment, visibility, owner_user_id,
  created_by_key_id, updated_at, created_at, updated_at
from ugraph_deployment_metadata
where version_label is not null
on conflict (deployment, version_label) do nothing;

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

create table if not exists ugraph_sync_checkpoints (
  deployment text not null references ugraph_deployments(id) on delete cascade,
  block_number bigint not null,
  block_hash text,
  block_timestamp bigint,
  from_block bigint,
  scanned_logs bigint not null default 0,
  executed_logs bigint not null default 0,
  validation_errors bigint not null default 0,
  complete boolean not null default false,
  entity_changes integer not null default 0,
  synced_at timestamptz not null default now(),
  primary key (deployment, block_number)
);

create index if not exists ugraph_sync_checkpoints_synced_at
  on ugraph_sync_checkpoints (deployment, synced_at desc);

create table if not exists ugraph_entity_changes (
  deployment text not null references ugraph_deployments(id) on delete cascade,
  block_number bigint not null,
  block_hash text,
  block_timestamp bigint,
  entity text not null,
  id text not null,
  action text not null,
  data jsonb not null,
  previous_data jsonb,
  updated_at timestamptz not null default now(),
  primary key (deployment, block_number, entity, id),
  check (action in ('created', 'updated', 'removed'))
);

alter table ugraph_entity_changes
  add column if not exists previous_data jsonb;

create index if not exists ugraph_entity_changes_block
  on ugraph_entity_changes (deployment, block_number desc);

create index if not exists ugraph_entity_changes_entity_id
  on ugraph_entity_changes (deployment, entity, id, block_number);

create index if not exists ugraph_entity_changes_data_gin
  on ugraph_entity_changes using gin (data);

insert into ugraph_sync_checkpoints (
  deployment, block_number, block_hash, block_timestamp, from_block,
  scanned_logs, executed_logs, validation_errors, complete, entity_changes, synced_at
)
select
  h.deployment,
  h.block_number,
  h.block_hash,
  nullif(h.checkpoint->>'block_timestamp', '')::bigint,
  nullif(h.checkpoint->>'from_block', '')::bigint,
  coalesce(nullif(h.checkpoint->>'scanned_logs', '')::bigint, 0),
  coalesce(nullif(h.checkpoint->>'executed_logs', '')::bigint, 0),
  coalesce(nullif(h.checkpoint->>'validation_errors', '')::bigint, 0),
  coalesce(nullif(h.checkpoint->>'complete', '')::boolean, false),
  coalesce(v.entity_changes, 0),
  h.updated_at
from ugraph_history_snapshots h
left join (
  select deployment, block_number, count(*)::integer as entity_changes
  from ugraph_entity_versions
  group by deployment, block_number
) v on v.deployment = h.deployment and v.block_number = h.block_number
on conflict (deployment, block_number) do nothing;

delete from ugraph_sync_checkpoints
where from_block is not null
  and from_block > block_number;

insert into ugraph_entity_changes (
  deployment, block_number, block_hash, block_timestamp,
  entity, id, action, data, previous_data, updated_at
)
select
  v.deployment,
  v.block_number,
  h.block_hash,
  nullif(h.checkpoint->>'block_timestamp', '')::bigint,
  v.entity,
  v.id,
  case
    when v.removed then 'removed'
    when prev.block_number is null or prev.removed then 'created'
    else 'updated'
  end,
  v.data,
  case when prev.removed then null else prev.data end,
  h.updated_at
from ugraph_entity_versions v
join ugraph_history_snapshots h
  on h.deployment = v.deployment
 and h.block_number = v.block_number
left join lateral (
  select p.block_number, p.removed, p.data
  from ugraph_entity_versions p
  where p.deployment = v.deployment
    and p.entity = v.entity
    and p.id = v.id
    and p.block_number < v.block_number
  order by p.block_number desc
  limit 1
) prev on true
where
  (v.removed and prev.block_number is not null and not prev.removed)
  or (
    not v.removed
    and (
      prev.block_number is null
      or prev.removed
      or prev.data is distinct from v.data
    )
  )
on conflict (deployment, block_number, entity, id) do nothing;

update ugraph_entity_changes current_change
set
  previous_data = case
    when previous_change.action = 'removed' then null
    else previous_change.data
  end,
  action = case
    when current_change.action = 'created'
     and previous_change.action <> 'removed'
      then 'updated'
    else current_change.action
  end
from ugraph_entity_changes previous_change
where previous_change.deployment = current_change.deployment
  and previous_change.entity = current_change.entity
  and previous_change.id = current_change.id
  and previous_change.block_number = (
    select max(previous_block.block_number)
    from ugraph_entity_changes previous_block
    where previous_block.deployment = current_change.deployment
      and previous_block.entity = current_change.entity
      and previous_block.id = current_change.id
      and previous_block.block_number < current_change.block_number
  )
  and current_change.previous_data is null
  and previous_change.data is not null;

delete from ugraph_entity_changes
where action = 'updated'
  and previous_data is not null
  and data = previous_data;

update ugraph_sync_checkpoints checkpoint
set entity_changes = (
  select count(*)::integer
  from ugraph_entity_changes change
  where change.deployment = checkpoint.deployment
    and change.block_number = checkpoint.block_number
);

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
    use std::{
        collections::BTreeMap,
        env,
        sync::{Mutex, MutexGuard, OnceLock},
    };

    use ugraph_core::{
        EntityField, EntitySchema, EntityType, EventTriggerPlan, RawEthereumLog, SourcePlan,
    };
    use ugraph_runtime::{EntityData, StoreValue};

    use super::*;

    static POSTGRES_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn postgres_test_guard() -> MutexGuard<'static, ()> {
        POSTGRES_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("postgres test lock poisoned")
    }

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
            "ugraph_users",
            "ugraph_api_keys",
            "ugraph_settings",
            "ugraph_deployment_metadata",
        ] {
            assert!(POSTGRES_SCHEMA.contains(table));
        }
        assert!(POSTGRES_SCHEMA.contains("storage_mode"));
        assert!(POSTGRES_SCHEMA.contains("removed boolean"));
        assert!(POSTGRES_SCHEMA.contains("public_user_signup"));
        assert!(POSTGRES_SCHEMA.contains("visibility in ('private', 'public')"));
        assert!(POSTGRES_SCHEMA.contains("ugraph_deployment_versions"));
        assert!(POSTGRES_SCHEMA.contains("storage_deployment"));
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
        let _guard = postgres_test_guard();
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

    #[test]
    fn recent_sync_activity_reports_entity_changes_when_url_is_set() -> anyhow::Result<()> {
        let Ok(url) = env::var("UGRAPH_TEST_POSTGRES_URL") else {
            return Ok(());
        };
        let _guard = postgres_test_guard();
        let deployment = format!("ugraph_activity_test_{}", std::process::id());
        let store = SnapshotStore::Postgres {
            url: url.clone(),
            deployment: deployment.clone(),
        };
        let mut snapshot = fixture_snapshot();
        snapshot.history = vec![
            HistoricalSnapshot {
                checkpoint: fixture_checkpoint(1),
                entities: vec![fixture_entity("Protocol", "main", "0x01")],
                dynamic_sources: Vec::new(),
            },
            HistoricalSnapshot {
                checkpoint: fixture_checkpoint(2),
                entities: vec![
                    fixture_entity("Protocol", "main", "0x02"),
                    fixture_entity("Protocol", "side", "0x10"),
                ],
                dynamic_sources: Vec::new(),
            },
            HistoricalSnapshot {
                checkpoint: fixture_checkpoint(3),
                entities: vec![fixture_entity("Protocol", "side", "0x11")],
                dynamic_sources: Vec::new(),
            },
            HistoricalSnapshot {
                checkpoint: fixture_checkpoint(4),
                entities: vec![fixture_entity("Protocol", "side", "0x11")],
                dynamic_sources: Vec::new(),
            },
        ];
        store.write(&snapshot)?;

        let page = recent_sync_activity(&url, &deployment, 1, 3, 10, false)?;
        let activity = page.activities;

        assert_eq!(page.page, 1);
        assert_eq!(page.limit, 3);
        assert!(!page.show_empty);
        assert_eq!(page.stats.change_blocks, 3);
        assert_eq!(page.stats.indexed_checkpoints, 4);
        assert_eq!(page.stats.entity_changes, 5);
        assert_eq!(page.stats.created, 2);
        assert_eq!(page.stats.updated, 2);
        assert_eq!(page.stats.removed, 1);
        assert_eq!(activity.len(), 3);
        assert_eq!(activity[0].block_number, 3);
        assert_eq!(activity[0].block_hash.as_deref(), Some("0xblock3"));
        assert_eq!(activity[0].block_timestamp, Some(1_700_000_003));
        assert_eq!(activity[0].updated, 1);
        assert_eq!(activity[0].removed, 1);
        let removed_main = activity[0]
            .changes
            .iter()
            .find(|change| change.id == "main" && change.action == EntityChangeAction::Removed)
            .expect("removed main change");
        assert_eq!(
            removed_main
                .previous_data
                .as_ref()
                .and_then(|data| data.get("id")),
            Some(&StoreValue::Bytes("0x02".to_string()))
        );
        assert_eq!(activity[1].block_number, 2);
        assert_eq!(activity[1].created, 1);
        assert_eq!(activity[1].updated, 1);
        let updated_main = activity[1]
            .changes
            .iter()
            .find(|change| change.id == "main" && change.action == EntityChangeAction::Updated)
            .expect("updated main change");
        assert_eq!(
            updated_main
                .previous_data
                .as_ref()
                .and_then(|data| data.get("id")),
            Some(&StoreValue::Bytes("0x01".to_string()))
        );
        assert_eq!(
            updated_main.data.get("id"),
            Some(&StoreValue::Bytes("0x02".to_string()))
        );
        assert_eq!(activity[2].block_number, 1);
        assert_eq!(activity[2].created, 1);

        let page = recent_sync_activity(&url, &deployment, 1, 1, 10, true)?;
        assert!(page.show_empty);
        assert!(page.has_next);
        assert_eq!(page.activities.len(), 1);
        assert_eq!(page.activities[0].block_number, 4);
        assert!(page.activities[0].changes.is_empty());

        let mut next_snapshot = snapshot.clone();
        next_snapshot.entities = vec![
            fixture_entity("Protocol", "side", "0x11"),
            fixture_entity("Protocol", "late", "0x20"),
        ];
        let baseline = snapshot_from_history(&snapshot.history[3]);
        let new_activity = vec![HistoricalSnapshot {
            checkpoint: fixture_checkpoint(5),
            entities: next_snapshot.entities.clone(),
            dynamic_sources: Vec::new(),
        }];
        store.write_with_activity(&next_snapshot, &new_activity, Some(&baseline), false)?;

        let page = recent_sync_activity(&url, &deployment, 1, 10, 10, false)?;
        assert_eq!(page.stats.change_blocks, 4);
        assert_eq!(page.stats.entity_changes, 6);
        assert!(page.activities.iter().any(|activity| {
            activity.block_number == 5
                && activity.created == 1
                && activity.changes.iter().any(|change| {
                    change.id == "late" && change.action == EntityChangeAction::Created
                })
        }));

        cleanup(&url, &deployment)?;
        Ok(())
    }

    #[test]
    fn migration_prunes_legacy_noop_entity_changes() -> anyhow::Result<()> {
        let Ok(url) = env::var("UGRAPH_TEST_POSTGRES_URL") else {
            return Ok(());
        };
        let _guard = postgres_test_guard();
        let deployment = format!("ugraph_noop_activity_test_{}", std::process::id());
        let store = SnapshotStore::Postgres {
            url: url.clone(),
            deployment: deployment.clone(),
        };
        store.write(&fixture_snapshot())?;

        let mut client = connect(&url)?;
        migrate(&mut client)?;
        let data = serde_json::to_value(fixture_entity("Noop", "same", "0x01").data)?;
        for block in [10_i64, 11_i64] {
            client.execute(
                r#"
                insert into ugraph_sync_checkpoints (
                  deployment, block_number, block_hash, block_timestamp,
                  scanned_logs, executed_logs, validation_errors, complete, entity_changes
                )
                values ($1, $2, $3, $4, 1, 1, 0, true, 1)
                "#,
                &[
                    &deployment,
                    &block,
                    &format!("0xnoop{block}"),
                    &(1_700_000_000_i64 + block),
                ],
            )?;
            client.execute(
                r#"
                insert into ugraph_entity_changes (
                  deployment, block_number, block_hash, block_timestamp,
                  entity, id, action, data
                )
                values ($1, $2, $3, $4, 'Noop', 'same', 'created', $5)
                "#,
                &[
                    &deployment,
                    &block,
                    &format!("0xnoop{block}"),
                    &(1_700_000_000_i64 + block),
                    &data,
                ],
            )?;
        }
        drop(client);

        let page = recent_sync_activity(&url, &deployment, 1, 10, 10, false)?;
        assert!(page
            .activities
            .iter()
            .any(|activity| activity.block_number == 10));
        assert!(!page
            .activities
            .iter()
            .any(|activity| activity.block_number == 11));
        let mut client = connect(&url)?;
        let checkpoint = client.query_one(
            "select entity_changes from ugraph_sync_checkpoints where deployment = $1 and block_number = 11",
            &[&deployment],
        )?;
        assert_eq!(checkpoint.get::<_, i32>("entity_changes"), 0);

        cleanup(&url, &deployment)?;
        Ok(())
    }

    #[test]
    fn feed_rollback_prunes_raw_rows_and_rewinds_cursors() -> anyhow::Result<()> {
        let Ok(url) = env::var("UGRAPH_TEST_POSTGRES_URL") else {
            return Ok(());
        };
        let _guard = postgres_test_guard();
        let chain_id = 9_000_000_000_u64 + u64::from(std::process::id());
        let deployment = format!("ugraph_feed_test_{}", std::process::id());
        let source = fixture_source();
        register_feed_source_subscriptions(
            &url,
            &deployment,
            chain_id,
            std::slice::from_ref(&source),
        )?;
        let subscriptions = list_feed_subscriptions(&url, chain_id)?;
        assert_eq!(subscriptions.len(), 1);
        write_feed_logs(
            &url,
            &subscriptions[0],
            &[fixture_raw_log()],
            10,
            Some("0xhash10"),
        )?;

        assert_eq!(
            feed_block_hash(&url, chain_id, 10)?,
            Some("0xhash10".to_string())
        );
        let report = rollback_feed_chain(&url, chain_id, 10)?;

        assert_eq!(report.from_block, 10);
        assert_eq!(report.to_block, Some(10));
        assert_eq!(report.deleted_blocks, 1);
        assert_eq!(report.deleted_logs, 1);
        assert_eq!(report.updated_subscriptions, 1);
        assert_eq!(feed_block_hash(&url, chain_id, 10)?, None);
        let subscriptions = list_feed_subscriptions(&url, chain_id)?;
        assert_eq!(subscriptions[0].cursor_block, Some(9));
        assert_eq!(subscriptions[0].cursor_hash, None);
        cleanup_feed(&url, chain_id)?;
        Ok(())
    }

    #[test]
    fn users_api_keys_and_deployment_metadata_roundtrip_when_url_is_set() -> anyhow::Result<()> {
        let Ok(url) = env::var("UGRAPH_TEST_POSTGRES_URL") else {
            return Ok(());
        };
        let _guard = postgres_test_guard();
        let suffix = std::process::id();
        let email = format!("identity-{suffix}@ugraph.local");
        let deployment = format!("ugraph_identity_test_{suffix}");
        let store = SnapshotStore::Postgres {
            url: url.clone(),
            deployment: deployment.clone(),
        };
        store.write(&fixture_snapshot())?;

        let user = create_or_update_user(&url, &email, Some("Identity Test"), "admin")?;
        assert_eq!(user.email, email);
        assert_eq!(user.role, "admin");

        let created = create_api_key(&url, &email, "ci", &["deploy".to_string()])?;
        assert!(created.key.starts_with("ugraph_"));
        assert_eq!(created.record.scopes, vec!["deploy".to_string()]);

        let verified = verify_api_key(&url, &created.key)?.context("key should verify")?;
        assert_eq!(verified.email, email);
        assert!(verify_api_key_scope(&url, &created.key, "deploy")?.is_some());
        assert!(verify_api_key_scope(&url, &created.key, "query")?.is_none());

        set_public_signup(&url, true)?;
        assert!(public_signup_enabled(&url)?);
        set_public_signup(&url, false)?;
        assert!(!public_signup_enabled(&url)?);

        let metadata = record_deployment_metadata(
            &url,
            &deployment,
            Some("v1"),
            "private",
            None,
            Some(&created.key),
        )?;
        assert_eq!(metadata.deployment, deployment);
        assert_eq!(metadata.version_label.as_deref(), Some("v1"));
        assert_eq!(metadata.visibility, "private");
        assert_eq!(metadata.owner_email.as_deref(), Some(email.as_str()));
        assert_eq!(
            metadata.created_by_key_prefix.as_deref(),
            Some(created.record.prefix.as_str())
        );

        let metadata = set_deployment_visibility(&url, &deployment, "public")?;
        assert_eq!(metadata.visibility, "public");

        let versioned_deployment = format!("{deployment}@v2");
        let versioned_store = SnapshotStore::Postgres {
            url: url.clone(),
            deployment: versioned_deployment.clone(),
        };
        versioned_store.write(&fixture_snapshot())?;
        let version = record_deployment_version(
            &url,
            DeploymentVersionInput {
                deployment: &deployment,
                version_label: "v2",
                storage_deployment: &versioned_deployment,
                visibility: "public",
                owner_email: Some(&email),
                api_key: None,
                promote: false,
            },
        )?;
        assert_eq!(version.storage_deployment, versioned_deployment);
        assert_eq!(
            resolve_deployment_storage(&url, &deployment, "v2")?.as_deref(),
            Some(versioned_deployment.as_str())
        );
        assert_eq!(
            resolve_deployment_storage(&url, &deployment, "latest")?.as_deref(),
            Some(deployment.as_str())
        );
        promote_deployment_version(&url, &deployment, "v2")?;
        assert_eq!(
            resolve_deployment_storage(&url, &deployment, "latest")?.as_deref(),
            Some(versioned_deployment.as_str())
        );
        assert!(list_public_deployment_versions(&url)?
            .iter()
            .any(|version| version.deployment == deployment
                && version.version_label == "v2"
                && version.storage_deployment == versioned_deployment));

        let deployments = list_deployment_metadata(&url)?;
        assert!(deployments
            .iter()
            .any(|metadata| metadata.deployment == deployment));

        assert_eq!(revoke_api_key(&url, &created.record.prefix)?, 1);
        assert!(verify_api_key(&url, &created.key)?.is_none());

        cleanup(&url, &deployment)?;
        cleanup(&url, &versioned_deployment)?;
        cleanup_user(&url, &email)?;
        Ok(())
    }

    fn cleanup(url: &str, deployment: &str) -> anyhow::Result<()> {
        let mut client = connect(url)?;
        client.execute(
            "delete from ugraph_deployment_versions where deployment = $1 or storage_deployment = $1",
            &[&deployment],
        )?;
        client.execute(
            "delete from ugraph_deployment_metadata where deployment = $1",
            &[&deployment],
        )?;
        client.execute(
            "delete from ugraph_deployments where id = $1",
            &[&deployment],
        )?;
        Ok(())
    }

    fn cleanup_user(url: &str, email: &str) -> anyhow::Result<()> {
        let mut client = connect(url)?;
        migrate(&mut client)?;
        client.execute("delete from ugraph_users where email = $1", &[&email])?;
        Ok(())
    }

    fn cleanup_feed(url: &str, chain_id: u64) -> anyhow::Result<()> {
        let mut client = connect(url)?;
        migrate(&mut client)?;
        let chain_id = i64::try_from(chain_id).context("chain id overflows postgres")?;
        client.execute(
            "delete from ugraph_raw_logs where chain_id = $1",
            &[&chain_id],
        )?;
        client.execute(
            "delete from ugraph_raw_blocks where chain_id = $1",
            &[&chain_id],
        )?;
        client.execute(
            "delete from ugraph_feed_subscriptions where chain_id = $1",
            &[&chain_id],
        )?;
        Ok(())
    }

    fn fixture_source() -> SourcePlan {
        SourcePlan {
            name: "Source".to_string(),
            template: false,
            dynamic: false,
            template_name: None,
            params: Vec::new(),
            kind: "ethereum/contract".to_string(),
            network: Some("test".to_string()),
            address: Some("0x0000000000000000000000000000000000000001".to_string()),
            abi: Some("Source".to_string()),
            start_block: Some(10),
            end_block: None,
            triggers: vec![EventTriggerPlan {
                event: "Event()".to_string(),
                handler: "handleEvent".to_string(),
                signature: "Event()".to_string(),
                topic0: "0x0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
                inputs: Vec::new(),
            }],
        }
    }

    fn fixture_raw_log() -> RawEthereumLog {
        RawEthereumLog {
            address: "0x0000000000000000000000000000000000000001".to_string(),
            topics: vec![
                "0x0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            ],
            data: "0x".to_string(),
            block_number: Some("0xa".to_string()),
            block_hash: Some("0xhash10".to_string()),
            transaction_hash: Some("0xtx".to_string()),
            transaction_index: Some("0x0".to_string()),
            log_index: Some("0x0".to_string()),
        }
    }

    fn fixture_checkpoint(block: u64) -> SyncCheckpoint {
        SyncCheckpoint {
            from_block: Some(block),
            to_block: block,
            block_hash: Some(format!("0xblock{block}")),
            block_timestamp: Some(1_700_000_000 + block),
            scanned_logs: 1,
            executed_logs: 1,
            validation_errors: 0,
            complete: true,
        }
    }

    fn fixture_entity(entity: &str, id: &str, value: &str) -> EntitySnapshot {
        let mut data = EntityData::new();
        data.insert("id".to_string(), StoreValue::Bytes(value.to_string()));
        EntitySnapshot {
            entity: entity.to_string(),
            id: id.to_string(),
            data,
        }
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
                block_timestamp: None,
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
                    block_timestamp: None,
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

    fn snapshot_from_history(history: &HistoricalSnapshot) -> StoreSnapshot {
        let mut snapshot = fixture_snapshot();
        snapshot.checkpoint = history.checkpoint.clone();
        snapshot.entities = history.entities.clone();
        snapshot.dynamic_sources = history.dynamic_sources.clone();
        snapshot.history = Vec::new();
        snapshot
    }
}
