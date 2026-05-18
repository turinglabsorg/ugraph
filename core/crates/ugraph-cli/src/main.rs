mod query;
mod server;
mod state;
mod storage;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    thread,
    time::Duration,
};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use ugraph_core::{
    build_indexing_plan, check_handler_exports, check_manifest_abi_events, compatibility_report,
    exports_by_file, imports_by_file, inspect_wasm_tree, instantiate_dynamic_source,
    latest_block_number, resolve_rpc_urls, scan_planned_source, scan_raw_logs, scan_static_sources,
    EntitySchema, Manifest, MatchedLog, RpcResolverOptions, ScanOptions, ScanReport,
    ScanSourceReport, SourcePlan,
};

use crate::state::{
    entity_store_from_snapshot, historical_snapshot_from_store, materialize_historical_snapshot,
    snapshot_from_store, DynamicSourceSnapshot, HistoricalSnapshot, ProcessedLogSnapshot,
    SyncCheckpoint,
};
use crate::storage::SnapshotStore;

type LogIdentity = (String, bool, String, u64, u64, u64, String);
const DEFAULT_RPC_TIMEOUT_SECS: u64 = 15;

#[derive(Debug, Parser)]
#[command(name = "ugraph")]
#[command(about = "A fast Rust runtime for standard Graph Protocol subgraphs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
enum StorageKind {
    Json,
    Postgres,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, ValueEnum)]
enum LogSourceKind {
    Rpc,
    PostgresFeed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, ValueEnum)]
enum DeployProvider {
    Local,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum DeploymentVisibility {
    Private,
    Public,
}

impl DeploymentVisibility {
    fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Public => "public",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
enum ReorgPolicy {
    Fail,
    Rollback,
    Reset,
}

#[derive(Debug, Subcommand)]
enum UserCommand {
    /// Create or update a user.
    Create {
        #[arg(long)]
        email: String,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long, default_value = "member")]
        role: String,
        #[arg(long)]
        json: bool,
    },
    /// List users.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Manage whether public user signup is enabled.
    Signup {
        #[command(subcommand)]
        command: SignupCommand,
    },
    /// Manage API keys.
    Key {
        #[command(subcommand)]
        command: ApiKeyCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SignupCommand {
    /// Print current public signup setting.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Enable public user signup.
    Enable {
        #[arg(long)]
        json: bool,
    },
    /// Disable public user signup.
    Disable {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ApiKeyCommand {
    /// Create an API key for a user. The secret is printed once.
    Create {
        #[arg(long)]
        email: String,
        #[arg(long, default_value = "default")]
        name: String,
        #[arg(long = "scope")]
        scopes: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Verify an API key and print its user.
    Verify {
        #[arg(long, env = "UGRAPH_API_KEY")]
        api_key: String,
        #[arg(long)]
        json: bool,
    },
    /// Revoke an API key by prefix.
    Revoke {
        #[arg(long)]
        prefix: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum DeploymentCommand {
    /// List deployment ownership, versions, and visibility.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Register or update deployment metadata without running a sync.
    Register {
        #[arg(long)]
        deployment: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, value_enum, default_value = "private")]
        visibility: DeploymentVisibility,
        #[arg(long)]
        owner_email: Option<String>,
        #[arg(long, env = "UGRAPH_API_KEY")]
        api_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Change deployment query visibility.
    SetVisibility {
        #[arg(long)]
        deployment: String,
        #[arg(long, value_enum)]
        visibility: DeploymentVisibility,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate that a standard subgraph.yaml and all referenced files exist.
    Validate {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
    },
    /// Print a compact summary of a standard subgraph manifest.
    Inspect {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Resolve RPC URLs. User/env RPC wins; otherwise fetch Chainlist registry fresh.
    Rpc {
        #[arg(long, env = "UGRAPH_CHAIN_ID", default_value_t = 11155111)]
        chain_id: u64,
        #[arg(long, env = "UGRAPH_RPC_URL")]
        rpc_url: Option<String>,
        #[arg(long, default_value = ugraph_core::rpc::DEFAULT_CHAINLIST_REGISTRY_URL)]
        registry_url: String,
        #[arg(long, default_value_t = 5)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
    /// Inspect compiled Graph mapping WASM imports.
    WasmImports {
        #[arg(long, default_value = "build")]
        build_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Inspect compiled Graph mapping WASM function exports.
    WasmExports {
        #[arg(long, default_value = "build")]
        build_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Check that every manifest event handler is exported by its compiled WASM module.
    HandlerExports {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        build_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Check manifest event signatures against the ABI selected by each data source.
    AbiEvents {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Build the deterministic source/event scan plan from manifest and ABI files.
    Plan {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Fetch matching Ethereum logs for static data sources via JSON-RPC.
    Scan {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long, env = "UGRAPH_CHAIN_ID", default_value_t = 11155111)]
        chain_id: u64,
        #[arg(long, env = "UGRAPH_RPC_URL")]
        rpc_url: Option<String>,
        #[arg(long)]
        from_block: Option<u64>,
        #[arg(long)]
        to_block: Option<u64>,
        #[arg(long, env = "UGRAPH_MAX_BLOCK_RANGE", default_value_t = 2_000)]
        max_block_range: u64,
        #[arg(long, env = "UGRAPH_RPC_RETRIES", default_value_t = 3)]
        rpc_retries: u32,
        #[arg(long)]
        json: bool,
    },
    /// Fetch matching logs and execute their mapping handlers against compiled WASM.
    Replay {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        build_dir: Option<PathBuf>,
        #[arg(long, env = "UGRAPH_CHAIN_ID", default_value_t = 11155111)]
        chain_id: u64,
        #[arg(long, env = "UGRAPH_RPC_URL")]
        rpc_url: Option<String>,
        #[arg(long)]
        from_block: Option<u64>,
        #[arg(long)]
        to_block: Option<u64>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
        #[arg(long, env = "UGRAPH_MAX_BLOCK_RANGE", default_value_t = 2_000)]
        max_block_range: u64,
        #[arg(long, env = "UGRAPH_RPC_RETRIES", default_value_t = 3)]
        rpc_retries: u32,
        #[arg(long)]
        strict_schema: bool,
        #[arg(long)]
        json: bool,
    },
    /// Sync a subgraph into a current-state snapshot with checkpoint metadata.
    Sync {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        build_dir: Option<PathBuf>,
        #[arg(long, env = "UGRAPH_STATE_FILE", default_value = ".ugraph/state.json")]
        state_file: PathBuf,
        #[arg(long, env = "UGRAPH_STORAGE", value_enum, default_value = "json")]
        storage: StorageKind,
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: Option<String>,
        #[arg(long, env = "UGRAPH_DEPLOYMENT", default_value = "default")]
        deployment: String,
        #[arg(long, env = "UGRAPH_CHAIN_ID", default_value_t = 11155111)]
        chain_id: u64,
        #[arg(long, env = "UGRAPH_RPC_URL")]
        rpc_url: Option<String>,
        #[arg(long, env = "UGRAPH_LOG_SOURCE", value_enum, default_value = "rpc")]
        log_source: LogSourceKind,
        #[arg(long)]
        from_block: Option<u64>,
        #[arg(long)]
        to_block: Option<u64>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        #[arg(long)]
        reset: bool,
        #[arg(long)]
        watch: bool,
        #[arg(long, env = "UGRAPH_POLL_INTERVAL_MS", default_value_t = 1_000)]
        poll_interval_ms: u64,
        #[arg(long, env = "UGRAPH_RETRY_MAX_MS", default_value_t = 60_000)]
        retry_max_ms: u64,
        #[arg(
            long,
            env = "UGRAPH_REORG_POLICY",
            value_enum,
            default_value = "rollback"
        )]
        reorg_policy: ReorgPolicy,
        #[arg(long, env = "UGRAPH_REORG_CHECK_DEPTH", default_value_t = 64)]
        reorg_check_depth: usize,
        #[arg(long, env = "UGRAPH_HISTORY_LIMIT", default_value_t = 1_024)]
        history_limit: usize,
        #[arg(long, env = "UGRAPH_MAX_BLOCK_RANGE", default_value_t = 2_000)]
        max_block_range: u64,
        #[arg(long, env = "UGRAPH_RPC_RETRIES", default_value_t = 3)]
        rpc_retries: u32,
        #[arg(long)]
        strict_schema: bool,
        #[arg(long)]
        json: bool,
    },
    /// Read one chain once and write raw logs into the shared Postgres feed.
    ChainReader {
        #[arg(short, long)]
        manifest: Option<PathBuf>,
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: String,
        #[arg(long, env = "UGRAPH_DEPLOYMENT", default_value = "default")]
        deployment: String,
        #[arg(long, env = "UGRAPH_CHAIN_ID", default_value_t = 11155111)]
        chain_id: u64,
        #[arg(long, env = "UGRAPH_RPC_URL")]
        rpc_url: Option<String>,
        #[arg(long)]
        from_block: Option<u64>,
        #[arg(long)]
        to_block: Option<u64>,
        #[arg(long)]
        watch: bool,
        #[arg(long, env = "UGRAPH_POLL_INTERVAL_MS", default_value_t = 1_000)]
        poll_interval_ms: u64,
        #[arg(long, env = "UGRAPH_RETRY_MAX_MS", default_value_t = 60_000)]
        retry_max_ms: u64,
        #[arg(long, env = "UGRAPH_MAX_BLOCK_RANGE", default_value_t = 2_000)]
        max_block_range: u64,
        #[arg(long, env = "UGRAPH_RPC_RETRIES", default_value_t = 3)]
        rpc_retries: u32,
        #[arg(long)]
        json: bool,
    },
    /// Register and sync a deployment against local infrastructure.
    Deploy {
        #[arg(long, value_enum, default_value = "local")]
        provider: DeployProvider,
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        build_dir: Option<PathBuf>,
        #[arg(long, env = "UGRAPH_STORAGE", value_enum, default_value = "postgres")]
        storage: StorageKind,
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: Option<String>,
        #[arg(long, env = "UGRAPH_DEPLOYMENT", default_value = "default")]
        deployment: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, value_enum, default_value = "private")]
        visibility: DeploymentVisibility,
        #[arg(long)]
        owner_email: Option<String>,
        #[arg(long, env = "UGRAPH_API_KEY")]
        api_key: Option<String>,
        #[arg(long, env = "UGRAPH_CHAIN_ID", default_value_t = 11155111)]
        chain_id: u64,
        #[arg(long, env = "UGRAPH_RPC_URL")]
        rpc_url: Option<String>,
        #[arg(
            long,
            env = "UGRAPH_LOG_SOURCE",
            value_enum,
            default_value = "postgres-feed"
        )]
        log_source: LogSourceKind,
        #[arg(long)]
        from_block: Option<u64>,
        #[arg(long)]
        to_block: Option<u64>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        #[arg(long)]
        reset: bool,
        #[arg(long, env = "UGRAPH_DEPLOY_MAX_PASSES", default_value_t = 8)]
        max_passes: usize,
        #[arg(long, env = "UGRAPH_MAX_BLOCK_RANGE", default_value_t = 2_000)]
        max_block_range: u64,
        #[arg(long, env = "UGRAPH_RPC_RETRIES", default_value_t = 3)]
        rpc_retries: u32,
        #[arg(long)]
        json: bool,
    },
    /// Manage users, public signup, and API keys.
    Users {
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: String,
        #[command(subcommand)]
        command: UserCommand,
    },
    /// Inspect or update deployment ownership metadata.
    Deployments {
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: String,
        #[command(subcommand)]
        command: DeploymentCommand,
    },
    /// Serve GraphQL and GraphiQL from a current-state snapshot.
    Serve {
        #[arg(long, env = "UGRAPH_STATE_FILE", default_value = ".ugraph/state.json")]
        state_file: PathBuf,
        #[arg(long, env = "UGRAPH_STORAGE", value_enum, default_value = "json")]
        storage: StorageKind,
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: Option<String>,
        #[arg(long, env = "UGRAPH_DEPLOYMENT", default_value = "default")]
        deployment: String,
        #[arg(long, env = "UGRAPH_HOST", default_value = "127.0.0.1")]
        host: String,
        #[arg(long, env = "UGRAPH_PORT", default_value_t = 8030)]
        port: u16,
        #[arg(long)]
        once: bool,
    },
    /// Compare local current-state GraphQL output with a hosted GraphQL endpoint.
    Compare {
        #[arg(long, env = "UGRAPH_STATE_FILE", default_value = ".ugraph/state.json")]
        state_file: PathBuf,
        #[arg(long, env = "UGRAPH_STORAGE", value_enum, default_value = "json")]
        storage: StorageKind,
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: Option<String>,
        #[arg(long, env = "UGRAPH_DEPLOYMENT", default_value = "default")]
        deployment: String,
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        query: String,
        #[arg(long)]
        variables: Option<String>,
        #[arg(long)]
        operation_name: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Run a batch GraphQL conformance suite against a hosted Graph Node endpoint.
    Conformance {
        #[arg(long, env = "UGRAPH_STATE_FILE", default_value = ".ugraph/state.json")]
        state_file: PathBuf,
        #[arg(long, env = "UGRAPH_STORAGE", value_enum, default_value = "json")]
        storage: StorageKind,
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: Option<String>,
        #[arg(long, env = "UGRAPH_DEPLOYMENT", default_value = "default")]
        deployment: String,
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        cases_file: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Parse the GraphQL schema used for runtime store validation.
    Schema {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Check that every event handler has Graph Node's `(i32) -> ()` WASM ABI.
    HandlerSignatures {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        build_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Check compiled mapping WASM imports against the Graph Node host export surface.
    Compat {
        #[arg(long, default_value = "build")]
        build_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Compile and instantiate mapping WASM modules with a Graph-host-shaped linker.
    RuntimeCheck {
        #[arg(long, default_value = "build")]
        build_dir: PathBuf,
    },
    /// Execute mapping WASM id_of_type exports for graph-ts runtime type IDs.
    TypeIds {
        #[arg(long, default_value = "build")]
        build_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Run the full Graph Node/Goldsky compatibility gate for a compiled subgraph.
    Doctor {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        build_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Run structural, optional sync, and optional GraphQL conformance checks.
    Matrix {
        #[arg(short, long, default_value = "subgraph.yaml")]
        manifest: PathBuf,
        #[arg(long)]
        build_dir: Option<PathBuf>,
        #[arg(
            long,
            env = "UGRAPH_STATE_FILE",
            default_value = ".ugraph/matrix-state.json"
        )]
        state_file: PathBuf,
        #[arg(long, env = "UGRAPH_STORAGE", value_enum, default_value = "json")]
        storage: StorageKind,
        #[arg(long, env = "UGRAPH_POSTGRES_URL")]
        postgres_url: Option<String>,
        #[arg(long, env = "UGRAPH_DEPLOYMENT", default_value = "matrix")]
        deployment: String,
        #[arg(long, env = "UGRAPH_CHAIN_ID", default_value_t = 11155111)]
        chain_id: u64,
        #[arg(long, env = "UGRAPH_RPC_URL")]
        rpc_url: Option<String>,
        #[arg(long)]
        from_block: Option<u64>,
        #[arg(long)]
        to_block: Option<u64>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long, env = "UGRAPH_MAX_BLOCK_RANGE", default_value_t = 2_000)]
        max_block_range: u64,
        #[arg(long, env = "UGRAPH_RPC_RETRIES", default_value_t = 3)]
        rpc_retries: u32,
        #[arg(long)]
        endpoint: Option<String>,
        #[arg(long)]
        cases_file: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    ok: bool,
    manifest: String,
    build_dir: String,
    data_sources: usize,
    templates: usize,
    event_handlers: usize,
    handlers: usize,
    wasm_files: usize,
    required_host_imports: usize,
    missing_host_exports: Vec<String>,
    abi_events_ok: bool,
    handler_exports_ok: bool,
    handler_signatures_ok: bool,
    runtime_modules: usize,
}

#[derive(Debug, Serialize)]
struct MatrixReport {
    ok: bool,
    manifest: String,
    build_dir: String,
    structural: DoctorReport,
    sync: Option<MatrixSyncReport>,
    conformance: Option<ConformanceReport>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MatrixSyncReport {
    ok: bool,
    store: String,
    from_block: Option<u64>,
    to_block: u64,
    block_hash: Option<String>,
    scanned_logs: usize,
    executed_logs: usize,
    validation_errors: usize,
    complete: bool,
    entities: usize,
    dynamic_sources: usize,
}

#[derive(Debug, Serialize)]
struct ReplayReport {
    rpc_url: String,
    from_block: Option<u64>,
    to_block: u64,
    scanned_logs: usize,
    executed_logs: usize,
    validation_errors: usize,
    dynamic_sources: Vec<ScanSourceReport>,
    executions: Vec<ugraph_runtime::HandlerExecutionReport>,
}

#[derive(Debug, Serialize)]
struct CompareReport {
    ok: bool,
    endpoint: String,
    state_file: String,
    local: serde_json::Value,
    remote: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct ConformanceCase {
    name: String,
    query: String,
    #[serde(default)]
    variables: serde_json::Value,
    #[serde(default, rename = "operationName")]
    operation_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct ConformanceReport {
    ok: bool,
    endpoint: String,
    store: String,
    passed: usize,
    failed: usize,
    cases: Vec<ConformanceCaseReport>,
}

#[derive(Debug, Serialize)]
struct ConformanceCaseReport {
    name: String,
    ok: bool,
    local: serde_json::Value,
    remote: serde_json::Value,
}

#[derive(Debug)]
struct ReplayInput {
    manifest: PathBuf,
    build_dir: PathBuf,
    chain_id: u64,
    rpc_url: Option<String>,
    log_source: LogSource,
    from_block: Option<u64>,
    to_block: Option<u64>,
    limit: usize,
    max_block_range: u64,
    rpc_retries: u32,
    initial_store: ugraph_runtime::EntityStore,
    known_dynamic_sources: Vec<DynamicSourceSnapshot>,
    processed_logs: BTreeSet<LogIdentity>,
}

#[derive(Debug, Clone)]
enum LogSource {
    Rpc,
    PostgresFeed {
        postgres_url: String,
        deployment: String,
    },
}

#[derive(Debug)]
struct ChainReaderInput {
    manifest: Option<PathBuf>,
    postgres_url: String,
    deployment: String,
    chain_id: u64,
    rpc_url: Option<String>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    max_block_range: u64,
    rpc_retries: u32,
}

#[derive(Debug, Serialize)]
struct DeployReport {
    provider: DeployProvider,
    deployment: String,
    version_label: Option<String>,
    visibility: DeploymentVisibility,
    owner_email: Option<String>,
    metadata: Option<storage::DeploymentMetadataRecord>,
    log_source: LogSourceKind,
    passes: usize,
    feeds: Vec<storage::FeedIngestReport>,
    syncs: Vec<MatrixSyncReport>,
    sync: MatrixSyncReport,
}

struct SourceScanInput<'a> {
    log_source: &'a LogSource,
    chain_id: u64,
    rpc_url: &'a str,
    source: &'a SourcePlan,
    from_block: Option<u64>,
    to_block: u64,
    max_block_range: u64,
    rpc_retries: u32,
}

#[derive(Debug)]
struct ReplayRun {
    report: ReplayReport,
    schema: EntitySchema,
    entity_store: ugraph_runtime::EntityStore,
    dynamic_sources: Vec<DynamicSourceSnapshot>,
    processed_logs: BTreeSet<LogIdentity>,
    complete: bool,
    block_hash: Option<String>,
    history: Vec<HistoricalSnapshot>,
}

struct MatrixInput {
    manifest: PathBuf,
    build_dir: PathBuf,
    state_file: PathBuf,
    storage: StorageKind,
    postgres_url: Option<String>,
    deployment: String,
    chain_id: u64,
    rpc_url: Option<String>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    limit: usize,
    max_block_range: u64,
    rpc_retries: u32,
    endpoint: Option<String>,
    cases_file: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Validate { manifest } => {
            let parsed = Manifest::load(&manifest)
                .with_context(|| format!("loading {}", manifest.display()))?;
            parsed
                .validate_files(&manifest)
                .with_context(|| format!("validating {}", manifest.display()))?;
            println!("ok: {}", manifest.display());
        }
        Command::Inspect { manifest, json } => {
            let parsed = Manifest::load(&manifest)
                .with_context(|| format!("loading {}", manifest.display()))?;
            parsed
                .validate_files(&manifest)
                .with_context(|| format!("validating {}", manifest.display()))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&parsed)?);
            } else {
                println!("specVersion: {}", parsed.spec_version);
                println!("schema: {}", parsed.schema.file);
                println!("dataSources: {}", parsed.static_source_count());
                println!("templates: {}", parsed.template_count());
                println!("eventHandlers: {}", parsed.event_handler_count());
            }
        }
        Command::Rpc {
            chain_id,
            rpc_url,
            registry_url,
            limit,
            json,
        } => {
            let mut opts = RpcResolverOptions::for_chain(chain_id);
            opts.explicit_rpc_url = rpc_url;
            opts.registry_url = registry_url;
            let mut resolved = resolve_rpc_urls(opts)?;
            if limit > 0 && resolved.urls.len() > limit {
                resolved.urls.truncate(limit);
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&resolved)?);
            } else {
                println!("chainId: {}", resolved.chain_id);
                println!("source: {}", resolved.source);
                for url in resolved.urls {
                    println!("{url}");
                }
            }
        }
        Command::WasmImports { build_dir, json } => {
            let tree = inspect_wasm_tree(&build_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&tree)?);
            } else {
                println!("wasmFiles: {}", tree.files.len());
                println!("requiredImports: {}", tree.required_imports.len());
                for (file, imports) in imports_by_file(&tree.files) {
                    println!("\n{file}");
                    for import in imports {
                        println!("  {import}");
                    }
                }
            }
        }
        Command::WasmExports { build_dir, json } => {
            let tree = inspect_wasm_tree(&build_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&tree)?);
            } else {
                println!("wasmFiles: {}", tree.files.len());
                for (file, exports) in exports_by_file(&tree.files) {
                    println!("\n{file}");
                    for export in exports {
                        println!("  {export}");
                    }
                }
            }
        }
        Command::HandlerExports {
            manifest,
            build_dir,
            json,
        } => {
            let build_dir = default_build_dir(&manifest, build_dir);
            let report = check_handler_exports(&manifest, &build_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("ok: {}", report.ok);
                println!("handlers: {}", report.handlers.len());
                if !report.missing_handlers.is_empty() {
                    println!("missingHandlers:");
                    for missing in &report.missing_handlers {
                        println!(
                            "  {} {} {}",
                            missing.data_source,
                            missing.handler,
                            missing.wasm_path.display()
                        );
                    }
                }
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
        Command::AbiEvents { manifest, json } => {
            let report = check_manifest_abi_events(&manifest)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("ok: {}", report.ok);
                println!("events: {}", report.events.len());
                if !report.missing_events.is_empty() {
                    println!("missingEvents:");
                    for missing in &report.missing_events {
                        println!(
                            "  {} {} abi={}",
                            missing.data_source,
                            missing.normalized_signature,
                            missing.abi.as_deref().unwrap_or("<none>")
                        );
                    }
                }
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
        Command::Plan { manifest, json } => {
            let plan = build_indexing_plan(&manifest)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                println!("sources: {}", plan.sources.len());
                println!(
                    "triggers: {}",
                    plan.sources
                        .iter()
                        .map(|source| source.triggers.len())
                        .sum::<usize>()
                );
                for source in plan.sources {
                    println!(
                        "{}{} address={} startBlock={}",
                        if source.template { "template:" } else { "" },
                        source.name,
                        source.address.as_deref().unwrap_or("<dynamic>"),
                        source
                            .start_block
                            .map(|block| block.to_string())
                            .unwrap_or_else(|| "<dynamic>".to_string())
                    );
                    for trigger in source.triggers {
                        println!(
                            "  {} -> {} {}",
                            trigger.topic0, trigger.handler, trigger.signature
                        );
                    }
                }
            }
        }
        Command::Scan {
            manifest,
            chain_id,
            rpc_url,
            from_block,
            to_block,
            max_block_range,
            rpc_retries,
            json,
        } => {
            let mut rpc_opts = RpcResolverOptions::for_chain(chain_id);
            rpc_opts.explicit_rpc_url = rpc_url;
            let resolved = resolve_rpc_urls(rpc_opts)?;
            let rpc_url = resolved
                .urls
                .first()
                .cloned()
                .context("no RPC URLs resolved")?;
            let report = scan_static_sources(ScanOptions {
                manifest,
                rpc_url,
                from_block,
                to_block,
                max_block_range,
                rpc_retries,
            })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("rpc: {}", report.rpc_url);
                println!("toBlock: {}", report.to_block);
                println!("sources: {}", report.sources.len());
                println!("logs: {}", report.log_count);
                if !report.ordered_logs.is_empty() {
                    println!("orderedLogs:");
                    for log in &report.ordered_logs {
                        println!(
                            "  block={} log={} source={} handler={} tx={}",
                            log.block_number
                                .map(|block| block.to_string())
                                .unwrap_or_else(|| "<pending>".to_string()),
                            log.log_index
                                .map(|index| index.to_string())
                                .unwrap_or_else(|| "<pending>".to_string()),
                            log.source,
                            log.handler,
                            log.transaction_hash.as_deref().unwrap_or("<none>")
                        );
                    }
                }
                for source in report.sources {
                    println!(
                        "{} address={} range={}..{} triggers={} logs={}{}",
                        source.name,
                        source.address,
                        source.from_block,
                        source.to_block,
                        source.trigger_count,
                        source.log_count,
                        if source.skipped { " skipped" } else { "" }
                    );
                    for log in source.logs {
                        println!(
                            "  block={} log={} handler={} tx={}",
                            log.block_number
                                .map(|block| block.to_string())
                                .unwrap_or_else(|| "<pending>".to_string()),
                            log.log_index
                                .map(|index| index.to_string())
                                .unwrap_or_else(|| "<pending>".to_string()),
                            log.handler,
                            log.transaction_hash.as_deref().unwrap_or("<none>")
                        );
                    }
                }
            }
        }
        Command::Replay {
            manifest,
            build_dir,
            chain_id,
            rpc_url,
            from_block,
            to_block,
            limit,
            max_block_range,
            rpc_retries,
            strict_schema,
            json,
        } => {
            let build_dir = default_build_dir(&manifest, build_dir);
            let run = run_replay(ReplayInput {
                manifest,
                build_dir,
                chain_id,
                rpc_url,
                log_source: LogSource::Rpc,
                from_block,
                to_block,
                limit,
                max_block_range,
                rpc_retries,
                initial_store: ugraph_runtime::EntityStore::new(),
                known_dynamic_sources: Vec::new(),
                processed_logs: BTreeSet::new(),
            })?;
            let report = run.report;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("rpc: {}", report.rpc_url);
                println!("toBlock: {}", report.to_block);
                println!("scannedLogs: {}", report.scanned_logs);
                println!("executedLogs: {}", report.executed_logs);
                println!("validationErrors: {}", report.validation_errors);
                println!("dynamicSources: {}", report.dynamic_sources.len());
                for execution in &report.executions {
                    println!(
                        "  {} handler={} eventPtr={} storeSets={} dataSources={} ethereumCalls={}",
                        execution.wasm_path,
                        execution.handler,
                        execution.event_ptr,
                        execution.store_sets.len(),
                        execution.data_source_creates.len(),
                        execution.ethereum_calls.len()
                    );
                    for store_set in &execution.store_sets {
                        for error in &store_set.validation_errors {
                            println!("    schemaError: {error}");
                        }
                    }
                }
            }
            if strict_schema && report.validation_errors > 0 {
                std::process::exit(1);
            }
        }
        Command::Sync {
            manifest,
            build_dir,
            state_file,
            storage,
            postgres_url,
            deployment,
            chain_id,
            rpc_url,
            log_source,
            from_block,
            to_block,
            limit,
            reset,
            watch,
            poll_interval_ms,
            retry_max_ms,
            reorg_policy,
            reorg_check_depth,
            history_limit,
            max_block_range,
            rpc_retries,
            strict_schema,
            json,
        } => {
            let build_dir = default_build_dir(&manifest, build_dir);
            let store = snapshot_store(storage, state_file, postgres_url, deployment)?;
            let _indexer_lock = store.acquire_indexer_lock()?;
            let mut cycle_from_block = from_block;
            let mut cycle_reset = reset;
            let mut failures = 0_u32;
            loop {
                let result = sync_once(SyncOnceInput {
                    store: &store,
                    manifest: &manifest,
                    build_dir: &build_dir,
                    chain_id,
                    rpc_url: rpc_url.clone(),
                    log_source: log_source_for_sync(log_source, &store)?,
                    from_block: cycle_from_block,
                    to_block,
                    limit,
                    reset: cycle_reset,
                    reorg_policy,
                    reorg_check_depth,
                    history_limit,
                    max_block_range,
                    rpc_retries,
                });
                match result {
                    Ok(snapshot) => {
                        failures = 0;
                        if json {
                            println!("{}", serde_json::to_string_pretty(&snapshot)?);
                        } else {
                            println!("store: {}", store.label());
                            println!("toBlock: {}", snapshot.checkpoint.to_block);
                            println!("entities: {}", snapshot.entities.len());
                            println!("dynamicSources: {}", snapshot.dynamic_sources.len());
                            println!("executedLogs: {}", snapshot.checkpoint.executed_logs);
                            println!(
                                "validationErrors: {}",
                                snapshot.checkpoint.validation_errors
                            );
                            println!("complete: {}", snapshot.checkpoint.complete);
                        }
                        if strict_schema && snapshot.checkpoint.validation_errors > 0 {
                            std::process::exit(1);
                        }
                        if !watch {
                            break;
                        }
                        cycle_from_block = None;
                        cycle_reset = false;
                        thread::sleep(Duration::from_millis(poll_interval_ms));
                    }
                    Err(error) if watch => {
                        failures = failures.saturating_add(1);
                        let backoff_ms = watch_backoff_ms(poll_interval_ms, retry_max_ms, failures);
                        eprintln!(
                            "{}",
                            serde_json::json!({
                                "level": "error",
                                "event": "sync_error",
                                "store": store.label(),
                                "error": error.to_string(),
                                "failures": failures,
                                "retryInMs": backoff_ms
                            })
                        );
                        cycle_from_block = None;
                        cycle_reset = false;
                        thread::sleep(Duration::from_millis(backoff_ms));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Command::ChainReader {
            manifest,
            postgres_url,
            deployment,
            chain_id,
            rpc_url,
            from_block,
            to_block,
            watch,
            poll_interval_ms,
            retry_max_ms,
            max_block_range,
            rpc_retries,
            json,
        } => {
            let mut failures = 0_u32;
            loop {
                let result = run_chain_reader_once(ChainReaderInput {
                    manifest: manifest.clone(),
                    postgres_url: postgres_url.clone(),
                    deployment: deployment.clone(),
                    chain_id,
                    rpc_url: rpc_url.clone(),
                    from_block,
                    to_block,
                    max_block_range,
                    rpc_retries,
                });
                match result {
                    Ok(report) => {
                        failures = 0;
                        if json {
                            println!("{}", serde_json::to_string_pretty(&report)?);
                        } else {
                            println!("chainId: {}", report.chain_id);
                            println!("subscriptions: {}", report.subscriptions);
                            println!(
                                "toBlock: {}",
                                report
                                    .to_block
                                    .map(|block| block.to_string())
                                    .unwrap_or_else(|| "<none>".to_string())
                            );
                            println!("insertedLogs: {}", report.inserted_logs);
                        }
                        if !watch {
                            break;
                        }
                        thread::sleep(Duration::from_millis(poll_interval_ms));
                    }
                    Err(error) if watch => {
                        failures = failures.saturating_add(1);
                        let backoff_ms = watch_backoff_ms(poll_interval_ms, retry_max_ms, failures);
                        eprintln!(
                            "{}",
                            serde_json::json!({
                                "level": "error",
                                "event": "chain_reader_error",
                                "chainId": chain_id,
                                "error": error.to_string(),
                                "failures": failures,
                                "retryInMs": backoff_ms
                            })
                        );
                        thread::sleep(Duration::from_millis(backoff_ms));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Command::Deploy {
            provider,
            manifest,
            build_dir,
            storage,
            postgres_url,
            deployment,
            version,
            visibility,
            owner_email,
            api_key,
            chain_id,
            rpc_url,
            log_source,
            from_block,
            to_block,
            limit,
            reset,
            max_passes,
            max_block_range,
            rpc_retries,
            json,
        } => {
            let build_dir = default_build_dir(&manifest, build_dir);
            if let Some(key) = api_key.as_deref() {
                let postgres_url = postgres_url
                    .as_deref()
                    .context("--api-key requires --postgres-url")?;
                storage::verify_api_key_scope(postgres_url, key, "deploy")?
                    .context("api key is invalid, revoked, or missing deploy scope")?;
            }
            let store = snapshot_store(
                storage,
                PathBuf::from(".ugraph/state.json"),
                postgres_url.clone(),
                deployment.clone(),
            )?;
            let _indexer_lock = store.acquire_indexer_lock()?;
            let log_source_runtime = log_source_for_sync(log_source, &store)?;
            let max_passes = max_passes.max(1);
            let mut feeds = Vec::new();
            let mut syncs = Vec::new();
            let mut snapshot = None;
            let mut sync_from_block = from_block;
            let mut sync_reset = reset;
            let mut passes = 0;
            for pass in 0..max_passes {
                passes = pass + 1;
                if log_source == LogSourceKind::PostgresFeed {
                    let postgres_url = postgres_url
                        .clone()
                        .context("missing --postgres-url for postgres-feed deploy")?;
                    feeds.push(run_chain_reader_once(ChainReaderInput {
                        manifest: Some(manifest.clone()),
                        postgres_url,
                        deployment: deployment.clone(),
                        chain_id,
                        rpc_url: rpc_url.clone(),
                        from_block,
                        to_block,
                        max_block_range,
                        rpc_retries,
                    })?);
                }
                let current_snapshot = sync_once(SyncOnceInput {
                    store: &store,
                    manifest: &manifest,
                    build_dir: &build_dir,
                    chain_id,
                    rpc_url: rpc_url.clone(),
                    log_source: log_source_runtime.clone(),
                    from_block: sync_from_block,
                    to_block,
                    limit,
                    reset: sync_reset,
                    reorg_policy: ReorgPolicy::Rollback,
                    reorg_check_depth: 64,
                    history_limit: 1_024,
                    max_block_range,
                    rpc_retries,
                })?;
                let should_continue = deploy_should_continue(&current_snapshot, log_source);
                syncs.push(sync_report_from_snapshot(&store, &current_snapshot, false));
                snapshot = Some(current_snapshot);
                if !should_continue {
                    break;
                }
                sync_from_block = None;
                sync_reset = false;
            }
            let snapshot = snapshot.context("deploy sync did not run")?;
            let metadata = match &store {
                SnapshotStore::Postgres { url, .. } => Some(storage::record_deployment_metadata(
                    url,
                    &deployment,
                    version.as_deref(),
                    visibility.as_str(),
                    owner_email.as_deref(),
                    api_key.as_deref(),
                )?),
                SnapshotStore::Json { .. } => None,
            };
            let report = DeployReport {
                provider,
                deployment,
                version_label: version,
                visibility,
                owner_email,
                metadata,
                log_source,
                passes,
                feeds,
                syncs,
                sync: sync_report_from_snapshot(&store, &snapshot, true),
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("provider: {:?}", report.provider);
                println!("deployment: {}", report.deployment);
                println!(
                    "version: {}",
                    report.version_label.as_deref().unwrap_or("<none>")
                );
                println!("visibility: {}", report.visibility.as_str());
                println!(
                    "owner: {}",
                    report
                        .metadata
                        .as_ref()
                        .and_then(|metadata| metadata.owner_email.as_deref())
                        .or(report.owner_email.as_deref())
                        .unwrap_or("<none>")
                );
                println!("logSource: {:?}", report.log_source);
                println!("passes: {}", report.passes);
                let feed_inserted_logs: u64 =
                    report.feeds.iter().map(|feed| feed.inserted_logs).sum();
                let sync_executed_logs: usize =
                    report.syncs.iter().map(|sync| sync.executed_logs).sum();
                println!("feedPasses: {}", report.feeds.len());
                println!("feedInsertedLogs: {}", feed_inserted_logs);
                println!(
                    "feedToBlock: {}",
                    report
                        .feeds
                        .last()
                        .and_then(|feed| feed.to_block)
                        .unwrap_or_default()
                );
                println!("syncPasses: {}", report.syncs.len());
                println!("store: {}", report.sync.store);
                println!("toBlock: {}", report.sync.to_block);
                println!("executedLogs: {}", sync_executed_logs);
                println!("entities: {}", report.sync.entities);
                println!("complete: {}", report.sync.complete);
            }
            if !report.sync.ok {
                std::process::exit(1);
            }
        }
        Command::Users {
            postgres_url,
            command,
        } => match command {
            UserCommand::Create {
                email,
                display_name,
                role,
                json,
            } => {
                let user = storage::create_or_update_user(
                    &postgres_url,
                    &email,
                    display_name.as_deref(),
                    &role,
                )?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&user)?);
                } else {
                    println!("id: {}", user.id);
                    println!("email: {}", user.email);
                    println!(
                        "displayName: {}",
                        user.display_name.as_deref().unwrap_or("<none>")
                    );
                    println!("role: {}", user.role);
                    println!("createdAt: {}", user.created_at);
                }
            }
            UserCommand::List { json } => {
                let users = storage::list_users(&postgres_url)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&users)?);
                } else {
                    for user in users {
                        println!(
                            "{} email={} role={} createdAt={}",
                            user.id, user.email, user.role, user.created_at
                        );
                    }
                }
            }
            UserCommand::Signup { command } => match command {
                SignupCommand::Status { json } => {
                    let enabled = storage::public_signup_enabled(&postgres_url)?;
                    print_signup_status(enabled, json)?;
                }
                SignupCommand::Enable { json } => {
                    storage::set_public_signup(&postgres_url, true)?;
                    print_signup_status(true, json)?;
                }
                SignupCommand::Disable { json } => {
                    storage::set_public_signup(&postgres_url, false)?;
                    print_signup_status(false, json)?;
                }
            },
            UserCommand::Key { command } => match command {
                ApiKeyCommand::Create {
                    email,
                    name,
                    mut scopes,
                    json,
                } => {
                    if scopes.is_empty() {
                        scopes = vec!["deploy".to_string(), "query".to_string()];
                    }
                    let key = storage::create_api_key(&postgres_url, &email, &name, &scopes)?;
                    if json {
                        println!("{}", serde_json::to_string_pretty(&key)?);
                    } else {
                        println!("key: {}", key.key);
                        println!("id: {}", key.record.id);
                        println!("prefix: {}", key.record.prefix);
                        println!("userId: {}", key.record.user_id);
                        println!("scopes: {}", key.record.scopes.join(","));
                    }
                }
                ApiKeyCommand::Verify { api_key, json } => {
                    let user = storage::verify_api_key(&postgres_url, &api_key)?
                        .context("api key is invalid or revoked")?;
                    if json {
                        println!("{}", serde_json::to_string_pretty(&user)?);
                    } else {
                        println!("ok: true");
                        println!("userId: {}", user.id);
                        println!("email: {}", user.email);
                        println!("role: {}", user.role);
                    }
                }
                ApiKeyCommand::Revoke { prefix, json } => {
                    let revoked = storage::revoke_api_key(&postgres_url, &prefix)?;
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "revoked": revoked,
                                "prefix": prefix
                            }))?
                        );
                    } else {
                        println!("revoked: {revoked}");
                    }
                }
            },
        },
        Command::Deployments {
            postgres_url,
            command,
        } => match command {
            DeploymentCommand::List { json } => {
                let deployments = storage::list_deployment_metadata(&postgres_url)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&deployments)?);
                } else {
                    for deployment in deployments {
                        println!(
                            "{} version={} visibility={} owner={} key={} updatedAt={}",
                            deployment.deployment,
                            deployment.version_label.as_deref().unwrap_or("<none>"),
                            deployment.visibility,
                            deployment.owner_email.as_deref().unwrap_or("<none>"),
                            deployment
                                .created_by_key_prefix
                                .as_deref()
                                .unwrap_or("<none>"),
                            deployment.updated_at
                        );
                    }
                }
            }
            DeploymentCommand::Register {
                deployment,
                version,
                visibility,
                owner_email,
                api_key,
                json,
            } => {
                if let Some(key) = api_key.as_deref() {
                    storage::verify_api_key_scope(&postgres_url, key, "deploy")?
                        .context("api key is invalid, revoked, or missing deploy scope")?;
                }
                let metadata = storage::record_deployment_metadata(
                    &postgres_url,
                    &deployment,
                    version.as_deref(),
                    visibility.as_str(),
                    owner_email.as_deref(),
                    api_key.as_deref(),
                )?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&metadata)?);
                } else {
                    println!("deployment: {}", metadata.deployment);
                    println!(
                        "version: {}",
                        metadata.version_label.as_deref().unwrap_or("<none>")
                    );
                    println!("visibility: {}", metadata.visibility);
                    println!(
                        "owner: {}",
                        metadata.owner_email.as_deref().unwrap_or("<none>")
                    );
                    println!("updatedAt: {}", metadata.updated_at);
                }
            }
            DeploymentCommand::SetVisibility {
                deployment,
                visibility,
                json,
            } => {
                let metadata = storage::set_deployment_visibility(
                    &postgres_url,
                    &deployment,
                    visibility.as_str(),
                )?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&metadata)?);
                } else {
                    println!("deployment: {}", metadata.deployment);
                    println!("visibility: {}", metadata.visibility);
                    println!(
                        "owner: {}",
                        metadata.owner_email.as_deref().unwrap_or("<none>")
                    );
                    println!("updatedAt: {}", metadata.updated_at);
                }
            }
        },
        Command::Serve {
            state_file,
            storage,
            postgres_url,
            deployment,
            host,
            port,
            once,
        } => {
            let store = snapshot_store(storage, state_file, postgres_url, deployment)?;
            server::serve_store(store, &format!("{host}:{port}"), once)?;
        }
        Command::Compare {
            state_file,
            storage,
            postgres_url,
            deployment,
            endpoint,
            query,
            variables,
            operation_name,
            json,
        } => {
            let store = snapshot_store(storage, state_file, postgres_url, deployment)?;
            let snapshot = store.load()?;
            let variables = variables
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?
                .unwrap_or(serde_json::Value::Null);
            let local = query::normalize_json(&query::execute_graphql_with_operation(
                &snapshot,
                &query,
                &variables,
                operation_name.as_deref(),
            ));
            let remote = query::normalize_json(&post_graphql(
                &endpoint,
                &query,
                &variables,
                operation_name.as_deref(),
            )?);
            let report = CompareReport {
                ok: local == remote,
                endpoint,
                state_file: store.label(),
                local,
                remote,
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("ok: {}", report.ok);
                println!("endpoint: {}", report.endpoint);
                println!("stateFile: {}", report.state_file);
                if !report.ok {
                    println!("remote:");
                    println!("{}", serde_json::to_string_pretty(&report.remote)?);
                    println!("local:");
                    println!("{}", serde_json::to_string_pretty(&report.local)?);
                }
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
        Command::Conformance {
            state_file,
            storage,
            postgres_url,
            deployment,
            endpoint,
            cases_file,
            json,
        } => {
            let store = snapshot_store(storage, state_file, postgres_url, deployment)?;
            let snapshot = store.load()?;
            let cases = load_conformance_cases(&cases_file)?;
            let report = run_conformance_report(&snapshot, store.label(), endpoint, cases)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("ok: {}", report.ok);
                println!("endpoint: {}", report.endpoint);
                println!("store: {}", report.store);
                println!("passed: {}", report.passed);
                println!("failed: {}", report.failed);
                for case in report.cases.iter().filter(|case| !case.ok) {
                    println!("\ncase: {}", case.name);
                    println!("remote:");
                    println!("{}", serde_json::to_string_pretty(&case.remote)?);
                    println!("local:");
                    println!("{}", serde_json::to_string_pretty(&case.local)?);
                }
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
        Command::Schema { manifest, json } => {
            let schema = EntitySchema::load_for_manifest(&manifest)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&schema)?);
            } else {
                println!("entities: {}", schema.entities.len());
                for entity in schema.entities.values() {
                    println!("{} fields={}", entity.name, entity.fields.len());
                }
            }
        }
        Command::HandlerSignatures {
            manifest,
            build_dir,
            json,
        } => {
            let build_dir = default_build_dir(&manifest, build_dir);
            let report = ugraph_runtime::check_handler_signatures(&manifest, &build_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("ok: {}", report.ok);
                println!("handlers: {}", report.handlers.len());
                if !report.invalid_handlers.is_empty() {
                    println!("invalidHandlers:");
                    for invalid in &report.invalid_handlers {
                        println!(
                            "  {} {} params={:?} results={:?}",
                            invalid.data_source, invalid.handler, invalid.params, invalid.results
                        );
                    }
                }
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
        Command::Compat { build_dir, json } => {
            let report = compatibility_report(&build_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("ok: {}", report.ok);
                println!("wasmFiles: {}", report.files.len());
                println!("requiredImports: {}", report.required_imports.len());
                if !report.missing_host_exports.is_empty() {
                    println!("missingHostExports:");
                    for name in &report.missing_host_exports {
                        println!("  {name}");
                    }
                }
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
        Command::RuntimeCheck { build_dir } => {
            let check = ugraph_runtime::check_wasm_tree(&build_dir)?;
            println!("ok: true");
            println!("wasmModules: {}", check.modules.len());
            for module in check.modules {
                println!("{} imports={}", module.path.display(), module.import_count);
            }
        }
        Command::TypeIds { build_dir, json } => {
            let report = ugraph_runtime::inspect_graph_type_ids(&build_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("wasmModules: {}", report.modules.len());
                for module in report.modules {
                    println!("\n{}", module.path.display());
                    for (name, id) in module.type_ids {
                        println!("  {name}: {id}");
                    }
                }
            }
        }
        Command::Doctor {
            manifest,
            build_dir,
            json,
        } => {
            let build_dir = default_build_dir(&manifest, build_dir);
            let report = run_doctor_report(&manifest, &build_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("ok: {}", report.ok);
                println!("manifest: {}", report.manifest);
                println!("buildDir: {}", report.build_dir);
                println!("dataSources: {}", report.data_sources);
                println!("templates: {}", report.templates);
                println!("eventHandlers: {}", report.event_handlers);
                println!("handlers: {}", report.handlers);
                println!("wasmFiles: {}", report.wasm_files);
                println!("requiredHostImports: {}", report.required_host_imports);
                println!("abiEvents: {}", report.abi_events_ok);
                println!("handlerExports: {}", report.handler_exports_ok);
                println!("handlerSignatures: {}", report.handler_signatures_ok);
                println!("runtimeModules: {}", report.runtime_modules);
                if !report.missing_host_exports.is_empty() {
                    println!("missingHostExports:");
                    for missing in &report.missing_host_exports {
                        println!("  {missing}");
                    }
                }
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
        Command::Matrix {
            manifest,
            build_dir,
            state_file,
            storage,
            postgres_url,
            deployment,
            chain_id,
            rpc_url,
            from_block,
            to_block,
            limit,
            max_block_range,
            rpc_retries,
            endpoint,
            cases_file,
            json,
        } => {
            let build_dir = default_build_dir(&manifest, build_dir);
            let report = run_matrix(MatrixInput {
                manifest,
                build_dir,
                state_file,
                storage,
                postgres_url,
                deployment,
                chain_id,
                rpc_url,
                from_block,
                to_block,
                limit,
                max_block_range,
                rpc_retries,
                endpoint,
                cases_file,
            })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("ok: {}", report.ok);
                println!("manifest: {}", report.manifest);
                println!("buildDir: {}", report.build_dir);
                println!("structural: {}", report.structural.ok);
                if let Some(sync) = &report.sync {
                    println!(
                        "sync: {} executed={} validationErrors={} complete={} entities={} dynamicSources={}",
                        sync.ok,
                        sync.executed_logs,
                        sync.validation_errors,
                        sync.complete,
                        sync.entities,
                        sync.dynamic_sources
                    );
                }
                if let Some(conformance) = &report.conformance {
                    println!(
                        "conformance: {} passed={} failed={}",
                        conformance.ok, conformance.passed, conformance.failed
                    );
                }
                for note in &report.notes {
                    println!("note: {note}");
                }
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

fn run_replay(input: ReplayInput) -> anyhow::Result<ReplayRun> {
    let schema = EntitySchema::load_for_manifest(&input.manifest)?;
    let plan = build_indexing_plan(&input.manifest)?;
    let mut feed_backfill_pending = false;
    let scan = match &input.log_source {
        LogSource::Rpc => scan_static_sources_with_fallback(
            input.manifest.clone(),
            input.chain_id,
            input.rpc_url.clone(),
            input.from_block,
            input.to_block,
            input.max_block_range,
            input.rpc_retries,
        )?,
        LogSource::PostgresFeed {
            postgres_url,
            deployment,
        } => {
            let rpc_url = resolve_primary_rpc_url(input.chain_id, input.rpc_url.clone())?;
            let to_block = match input.to_block {
                Some(to_block) => to_block,
                None => storage::latest_feed_block(postgres_url, input.chain_id)?
                    .context("postgres feed has no cursor yet; start chain-reader first")?,
            };
            let static_sources = plan
                .sources
                .iter()
                .filter(|source| !source.template && source.address.is_some())
                .cloned()
                .collect::<Vec<_>>();
            storage::register_feed_source_subscriptions(
                postgres_url,
                deployment,
                input.chain_id,
                &static_sources,
            )?;
            let mut sources = Vec::new();
            for source in static_sources {
                if !storage::feed_source_caught_up(
                    postgres_url,
                    deployment,
                    input.chain_id,
                    &source,
                    to_block,
                )? {
                    feed_backfill_pending = true;
                }
                sources.push(storage::load_feed_source_report(
                    postgres_url,
                    input.chain_id,
                    &source,
                    input.from_block,
                    to_block,
                )?);
            }
            let mut ordered_logs = sources
                .iter()
                .flat_map(|source| source.logs.iter().cloned())
                .collect::<Vec<_>>();
            ordered_logs.sort_by_key(log_order_key);
            ScanReport {
                rpc_url,
                from_block: input.from_block,
                to_block,
                log_count: ordered_logs.len(),
                ordered_logs,
                sources,
            }
        }
    };

    let mut executions = Vec::new();
    let mut entity_store = input.initial_store;
    let mut pending_logs = scan.ordered_logs.clone();
    let mut processed_logs = input.processed_logs;
    let mut dynamic_source_keys = input
        .known_dynamic_sources
        .iter()
        .map(|source| dynamic_source_key(&source.name, &source.address))
        .collect::<BTreeSet<_>>();
    let mut dynamic_source_contexts = input
        .known_dynamic_sources
        .iter()
        .map(|source| {
            (
                dynamic_source_key(&source.name, &source.address),
                source.context.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut dynamic_sources = input.known_dynamic_sources.clone();
    let mut dynamic_scan_reports = Vec::new();
    let mut validation_errors = 0;
    let mut history = Vec::new();
    let mut ethereum_call_cache = ugraph_runtime::EthereumCallCache::new();
    let mut runtime_cache = ugraph_runtime::RuntimeModuleCache::new();

    for known in &input.known_dynamic_sources {
        let Some(source) =
            instantiate_dynamic_source(&plan, &known.name, &known.params, known.created_at_block)
        else {
            continue;
        };
        let dynamic_scan = scan_source_for_replay(SourceScanInput {
            log_source: &input.log_source,
            chain_id: input.chain_id,
            rpc_url: &scan.rpc_url,
            source: &source,
            from_block: scan.from_block,
            to_block: scan.to_block,
            max_block_range: input.max_block_range,
            rpc_retries: input.rpc_retries,
        })?;
        if feed_source_backfill_pending(&input.log_source, input.chain_id, &source, scan.to_block)?
        {
            feed_backfill_pending = true;
        }
        pending_logs.extend(dynamic_scan.logs.clone());
        dynamic_scan_reports.push(dynamic_scan);
    }

    let mut last_executed_block = None;
    loop {
        let Some(log) = next_unprocessed_log(&pending_logs, &processed_logs) else {
            break;
        };
        if input.limit == 0
            || (executions.len() >= input.limit && log.block_number != last_executed_block)
        {
            break;
        }
        let wasm_path = wasm_path_for_log(&input.build_dir, &log);
        let mut candidate_store = entity_store.clone();
        let mut candidate_call_cache = ethereum_call_cache.clone();
        let data_source_context =
            dynamic_source_contexts.get(&dynamic_source_key(&log.source, &log.address));
        let mut execution =
            ugraph_runtime::execute_matched_log_handler_with_runtime_cache_and_data_source_context(
                wasm_path,
                &log,
                &mut candidate_store,
                Some(&scan.rpc_url),
                &mut candidate_call_cache,
                &mut runtime_cache,
                data_source_context,
            )?;
        ugraph_runtime::validate_store_sets(&schema, &mut execution.store_sets);
        let execution_validation_errors = validation_error_count(&execution);
        validation_errors += execution_validation_errors;
        if execution_validation_errors > 0 {
            executions.push(execution);
            break;
        }

        entity_store = candidate_store;
        ethereum_call_cache = candidate_call_cache;
        processed_logs.insert(log_identity(&log));

        for create in &execution.data_source_creates {
            let Some(template_name) = create.name.as_deref() else {
                continue;
            };
            let creation_block = log.block_number.unwrap_or(scan.from_block.unwrap_or(0));
            let Some(source) =
                instantiate_dynamic_source(&plan, template_name, &create.params, creation_block)
            else {
                continue;
            };
            let address = source.address.as_deref().unwrap_or_default().to_string();
            if !dynamic_source_keys.insert(dynamic_source_key(template_name, &address)) {
                continue;
            }
            dynamic_source_contexts.insert(
                dynamic_source_key(template_name, &address),
                create.context.clone(),
            );
            let mut dynamic_scan = scan_source_for_replay(SourceScanInput {
                log_source: &input.log_source,
                chain_id: input.chain_id,
                rpc_url: &scan.rpc_url,
                source: &source,
                from_block: Some(creation_block),
                to_block: scan.to_block,
                max_block_range: input.max_block_range,
                rpc_retries: input.rpc_retries,
            })?;
            if feed_source_backfill_pending(
                &input.log_source,
                input.chain_id,
                &source,
                scan.to_block,
            )? {
                feed_backfill_pending = true;
            }
            let current_order = log_order_key(&log);
            dynamic_scan
                .logs
                .retain(|candidate| log_order_key(candidate) > current_order);
            dynamic_scan.log_count = dynamic_scan.logs.len();
            pending_logs.extend(dynamic_scan.logs.clone());
            dynamic_scan_reports.push(dynamic_scan);
            dynamic_sources.push(DynamicSourceSnapshot {
                name: template_name.to_string(),
                params: create.params.clone(),
                address,
                created_at_block: creation_block,
                context: create.context.clone(),
            });
        }
        let block_number = log.block_number;
        executions.push(execution);
        last_executed_block = block_number;
        if let Some(block_number) = block_number {
            if has_unprocessed_log_in_block(&pending_logs, &processed_logs, block_number) {
                continue;
            }
            push_history_snapshot(
                &mut history,
                historical_snapshot_from_store(
                    SyncCheckpoint {
                        from_block: scan.from_block,
                        to_block: block_number,
                        block_hash: log.block_hash.clone(),
                        scanned_logs: pending_logs.len(),
                        executed_logs: executions.len(),
                        validation_errors,
                        complete: false,
                    },
                    &entity_store,
                    dynamic_sources.clone(),
                ),
                usize::MAX,
            );
        }
    }
    let complete = !feed_backfill_pending
        && pending_logs
            .iter()
            .all(|log| processed_logs.contains(&log_identity(log)));

    let block_hash = match &input.log_source {
        LogSource::Rpc => fetch_block_hash(&scan.rpc_url, scan.to_block)
            .ok()
            .flatten(),
        LogSource::PostgresFeed { postgres_url, .. } => {
            storage::feed_block_hash(postgres_url, input.chain_id, scan.to_block)
                .ok()
                .flatten()
        }
    };
    let report = ReplayReport {
        rpc_url: scan.rpc_url,
        from_block: scan.from_block,
        to_block: scan.to_block,
        scanned_logs: pending_logs.len(),
        executed_logs: executions.len(),
        validation_errors,
        dynamic_sources: dynamic_scan_reports,
        executions,
    };

    Ok(ReplayRun {
        report,
        schema,
        entity_store,
        dynamic_sources,
        processed_logs,
        complete,
        block_hash,
        history,
    })
}

fn scan_static_sources_with_fallback(
    manifest: PathBuf,
    chain_id: u64,
    rpc_url: Option<String>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    max_block_range: u64,
    rpc_retries: u32,
) -> anyhow::Result<ScanReport> {
    let mut rpc_opts = RpcResolverOptions::for_chain(chain_id);
    rpc_opts.explicit_rpc_url = rpc_url;
    let resolved = resolve_rpc_urls(rpc_opts)?;
    let mut last_scan_error = None;
    for rpc_url in resolved.urls {
        match scan_static_sources(ScanOptions {
            manifest: manifest.clone(),
            rpc_url: rpc_url.clone(),
            from_block,
            to_block,
            max_block_range,
            rpc_retries,
        }) {
            Ok(report) => return Ok(report),
            Err(error) => {
                last_scan_error =
                    Some(anyhow::Error::new(error).context(format!("scanning RPC {rpc_url}")));
            }
        }
    }
    Err(last_scan_error.unwrap_or_else(|| anyhow::anyhow!("no RPC URLs resolved")))
}

fn scan_source_for_replay(input: SourceScanInput<'_>) -> anyhow::Result<ScanSourceReport> {
    match input.log_source {
        LogSource::Rpc => Ok(scan_planned_source(
            input.rpc_url,
            input.source,
            input.from_block,
            input.to_block,
            input.max_block_range,
            input.rpc_retries,
        )?),
        LogSource::PostgresFeed {
            postgres_url,
            deployment,
        } => {
            storage::register_feed_source_subscription(
                postgres_url,
                deployment,
                input.chain_id,
                input.source,
            )?;
            storage::load_feed_source_report(
                postgres_url,
                input.chain_id,
                input.source,
                input.from_block,
                input.to_block,
            )
        }
    }
}

fn feed_source_backfill_pending(
    log_source: &LogSource,
    chain_id: u64,
    source: &SourcePlan,
    to_block: u64,
) -> anyhow::Result<bool> {
    match log_source {
        LogSource::Rpc => Ok(false),
        LogSource::PostgresFeed {
            postgres_url,
            deployment,
        } => Ok(!storage::feed_source_caught_up(
            postgres_url,
            deployment,
            chain_id,
            source,
            to_block,
        )?),
    }
}

fn run_chain_reader_once(input: ChainReaderInput) -> anyhow::Result<storage::FeedIngestReport> {
    storage::migrate_postgres(&input.postgres_url)?;
    if let Some(manifest) = &input.manifest {
        let plan = build_indexing_plan(manifest)?;
        let sources = plan
            .sources
            .iter()
            .filter(|source| !source.template && source.address.is_some())
            .cloned()
            .collect::<Vec<_>>();
        storage::register_feed_source_subscriptions(
            &input.postgres_url,
            &input.deployment,
            input.chain_id,
            &sources,
        )?;
    }

    let rpc_urls = resolve_rpc_url_candidates(input.chain_id, input.rpc_url.clone())?;
    let mut last_error = None;
    for rpc_url in rpc_urls {
        match run_chain_reader_scan(&input, &rpc_url) {
            Ok(report) => return Ok(report),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no RPC URLs resolved")))
}

fn run_chain_reader_scan(
    input: &ChainReaderInput,
    rpc_url: &str,
) -> anyhow::Result<storage::FeedIngestReport> {
    let to_block = match input.to_block {
        Some(to_block) => to_block,
        None => latest_block_number(rpc_url)?,
    };
    let to_block_hash = fetch_block_hash(rpc_url, to_block)?;
    let mut subscriptions = storage::list_feed_subscriptions(&input.postgres_url, input.chain_id)?;
    let rollback = match feed_reorg_rollback_block(&subscriptions, rpc_url)? {
        Some(block) => {
            let rollback =
                storage::rollback_feed_chain(&input.postgres_url, input.chain_id, block)?;
            subscriptions = storage::list_feed_subscriptions(&input.postgres_url, input.chain_id)?;
            Some(rollback)
        }
        None => None,
    };
    let mut inserted_logs = 0_u64;
    for subscription in &subscriptions {
        let from_block = subscription
            .cursor_block
            .map(|block| block.saturating_add(1))
            .unwrap_or_else(|| input.from_block.unwrap_or(subscription.from_block));
        let from_block = from_block.max(subscription.from_block);
        if from_block > to_block {
            continue;
        }
        let logs = scan_raw_logs(
            rpc_url,
            &subscription.address,
            from_block,
            to_block,
            &subscription.topic0s,
            input.max_block_range,
            input.rpc_retries,
        )?;
        inserted_logs += storage::write_feed_logs(
            &input.postgres_url,
            subscription,
            &logs,
            to_block,
            to_block_hash.as_deref(),
        )?;
    }
    Ok(storage::FeedIngestReport {
        chain_id: input.chain_id,
        subscriptions: subscriptions.len(),
        to_block: Some(to_block),
        inserted_logs,
        rollback,
    })
}

fn feed_reorg_rollback_block(
    subscriptions: &[storage::FeedSubscription],
    rpc_url: &str,
) -> anyhow::Result<Option<u64>> {
    let mut cursors = BTreeMap::new();
    for subscription in subscriptions {
        let (Some(cursor_block), Some(cursor_hash)) = (
            subscription.cursor_block,
            subscription.cursor_hash.as_deref(),
        ) else {
            continue;
        };
        cursors
            .entry(cursor_block)
            .or_insert(cursor_hash.to_string());
    }
    let mut rollback_block = None;
    for (block, stored_hash) in cursors {
        let Some(rpc_hash) = fetch_block_hash(rpc_url, block)? else {
            continue;
        };
        if !stored_hash.eq_ignore_ascii_case(&rpc_hash) {
            rollback_block = Some(rollback_block.map_or(block, |current: u64| current.min(block)));
        }
    }
    Ok(rollback_block)
}

fn run_doctor_report(
    manifest: &std::path::Path,
    build_dir: &std::path::Path,
) -> anyhow::Result<DoctorReport> {
    let parsed =
        Manifest::load(manifest).with_context(|| format!("loading {}", manifest.display()))?;
    parsed
        .validate_files(manifest)
        .with_context(|| format!("validating {}", manifest.display()))?;
    let compat = compatibility_report(build_dir)?;
    let abi_events = check_manifest_abi_events(manifest)?;
    let handler_exports = check_handler_exports(manifest, build_dir)?;
    let handler_signatures = ugraph_runtime::check_handler_signatures(manifest, build_dir)?;
    let runtime = ugraph_runtime::check_wasm_tree(build_dir)?;
    let ok = compat.ok
        && abi_events.ok
        && handler_exports.ok
        && handler_signatures.ok
        && runtime.modules.len() == compat.files.len();
    Ok(DoctorReport {
        ok,
        manifest: manifest.display().to_string(),
        build_dir: build_dir.display().to_string(),
        data_sources: parsed.static_source_count(),
        templates: parsed.template_count(),
        event_handlers: parsed.event_handler_count(),
        handlers: parsed.handler_count(),
        wasm_files: compat.files.len(),
        required_host_imports: compat.required_imports.len(),
        missing_host_exports: compat.missing_host_exports,
        abi_events_ok: abi_events.ok,
        handler_exports_ok: handler_exports.ok,
        handler_signatures_ok: handler_signatures.ok,
        runtime_modules: runtime.modules.len(),
    })
}

fn sync_report_from_snapshot(
    store: &SnapshotStore,
    snapshot: &state::StoreSnapshot,
    require_complete: bool,
) -> MatrixSyncReport {
    MatrixSyncReport {
        ok: snapshot.checkpoint.validation_errors == 0
            && (!require_complete || snapshot.checkpoint.complete),
        store: store.label(),
        from_block: snapshot.checkpoint.from_block,
        to_block: snapshot.checkpoint.to_block,
        block_hash: snapshot.checkpoint.block_hash.clone(),
        scanned_logs: snapshot.checkpoint.scanned_logs,
        executed_logs: snapshot.checkpoint.executed_logs,
        validation_errors: snapshot.checkpoint.validation_errors,
        complete: snapshot.checkpoint.complete,
        entities: snapshot.entities.len(),
        dynamic_sources: snapshot.dynamic_sources.len(),
    }
}

fn deploy_should_continue(snapshot: &state::StoreSnapshot, log_source: LogSourceKind) -> bool {
    log_source == LogSourceKind::PostgresFeed
        && snapshot.checkpoint.validation_errors == 0
        && !snapshot.checkpoint.complete
}

fn run_matrix(input: MatrixInput) -> anyhow::Result<MatrixReport> {
    let structural = run_doctor_report(&input.manifest, &input.build_dir)?;
    let mut notes = Vec::new();
    let store = snapshot_store(
        input.storage,
        input.state_file,
        input.postgres_url,
        input.deployment,
    )?;

    let sync = match input.to_block {
        Some(to_block) => {
            let _indexer_lock = store.acquire_indexer_lock()?;
            let snapshot = sync_once(SyncOnceInput {
                store: &store,
                manifest: &input.manifest,
                build_dir: &input.build_dir,
                chain_id: input.chain_id,
                rpc_url: input.rpc_url,
                log_source: LogSource::Rpc,
                from_block: input.from_block,
                to_block: Some(to_block),
                limit: input.limit,
                reset: true,
                reorg_policy: ReorgPolicy::Rollback,
                reorg_check_depth: 64,
                history_limit: 1_024,
                max_block_range: input.max_block_range,
                rpc_retries: input.rpc_retries,
            })?;
            Some(sync_report_from_snapshot(&store, &snapshot, false))
        }
        None => {
            notes.push("sync skipped: provide --to-block to run a bounded sync slice".to_string());
            None
        }
    };

    let conformance = match (input.endpoint, input.cases_file) {
        (Some(endpoint), Some(cases_file)) => {
            let snapshot = store.load()?;
            let cases = load_conformance_cases(&cases_file)?;
            Some(run_conformance_report(
                &snapshot,
                store.label(),
                endpoint,
                cases,
            )?)
        }
        (None, None) => {
            notes.push(
                "conformance skipped: provide both --endpoint and --cases-file to diff GraphQL"
                    .to_string(),
            );
            None
        }
        _ => anyhow::bail!("matrix conformance requires both --endpoint and --cases-file"),
    };

    let ok = structural.ok
        && sync.as_ref().is_none_or(|sync| sync.ok)
        && conformance
            .as_ref()
            .is_none_or(|conformance| conformance.ok);
    Ok(MatrixReport {
        ok,
        manifest: input.manifest.display().to_string(),
        build_dir: input.build_dir.display().to_string(),
        structural,
        sync,
        conformance,
        notes,
    })
}

fn run_conformance_report(
    snapshot: &state::StoreSnapshot,
    store: String,
    endpoint: String,
    cases: Vec<ConformanceCase>,
) -> anyhow::Result<ConformanceReport> {
    let mut reports = Vec::with_capacity(cases.len());
    for case in cases {
        let local = query::normalize_json(&query::execute_graphql_with_operation(
            snapshot,
            &case.query,
            &case.variables,
            case.operation_name.as_deref(),
        ));
        let remote = query::normalize_json(&post_graphql(
            &endpoint,
            &case.query,
            &case.variables,
            case.operation_name.as_deref(),
        )?);
        reports.push(ConformanceCaseReport {
            name: case.name,
            ok: local == remote,
            local,
            remote,
        });
    }
    let failed = reports.iter().filter(|case| !case.ok).count();
    Ok(ConformanceReport {
        ok: failed == 0,
        endpoint,
        store,
        passed: reports.len().saturating_sub(failed),
        failed,
        cases: reports,
    })
}

struct SyncOnceInput<'a> {
    store: &'a SnapshotStore,
    manifest: &'a std::path::Path,
    build_dir: &'a std::path::Path,
    chain_id: u64,
    rpc_url: Option<String>,
    log_source: LogSource,
    from_block: Option<u64>,
    to_block: Option<u64>,
    limit: usize,
    reset: bool,
    reorg_policy: ReorgPolicy,
    reorg_check_depth: usize,
    history_limit: usize,
    max_block_range: u64,
    rpc_retries: u32,
}

fn sync_once(input: SyncOnceInput<'_>) -> anyhow::Result<state::StoreSnapshot> {
    let loaded = if input.reset {
        None
    } else {
        input.store.try_load()?
    };
    let previous = apply_reorg_policy(loaded, &input)?;
    let initial_store = previous
        .as_ref()
        .map(entity_store_from_snapshot)
        .unwrap_or_default();
    let known_dynamic_sources = previous
        .as_ref()
        .map(|snapshot| snapshot.dynamic_sources.clone())
        .unwrap_or_default();
    let processed_logs = previous
        .as_ref()
        .filter(|snapshot| !snapshot.checkpoint.complete)
        .map(processed_log_set)
        .unwrap_or_default();
    let from_block = input.from_block.or_else(|| {
        previous.as_ref().map(|snapshot| {
            if snapshot.checkpoint.complete {
                snapshot.checkpoint.to_block.saturating_add(1)
            } else {
                snapshot
                    .checkpoint
                    .from_block
                    .unwrap_or(snapshot.checkpoint.to_block)
            }
        })
    });
    let run = run_replay(ReplayInput {
        manifest: input.manifest.to_path_buf(),
        build_dir: input.build_dir.to_path_buf(),
        chain_id: input.chain_id,
        rpc_url: input.rpc_url,
        log_source: input.log_source.clone(),
        from_block,
        to_block: input.to_block,
        limit: input.limit,
        max_block_range: input.max_block_range,
        rpc_retries: input.rpc_retries,
        initial_store,
        known_dynamic_sources,
        processed_logs,
    })?;
    let checkpoint = SyncCheckpoint {
        from_block: run.report.from_block,
        to_block: run.report.to_block,
        block_hash: run.block_hash.clone(),
        scanned_logs: run.report.scanned_logs,
        executed_logs: run.report.executed_logs,
        validation_errors: run.report.validation_errors,
        complete: run.complete,
    };
    let processed_logs = if run.complete {
        Vec::new()
    } else {
        processed_log_snapshots(&run.processed_logs)
    };
    let mut snapshot = snapshot_from_store(
        input.manifest,
        checkpoint.clone(),
        run.schema,
        &run.entity_store,
        run.dynamic_sources.clone(),
        processed_logs,
    );
    snapshot.history = merge_history(
        previous.as_ref(),
        run.history,
        historical_snapshot_from_store(checkpoint, &run.entity_store, run.dynamic_sources),
        input.history_limit,
    );
    input.store.write(&snapshot)?;
    Ok(snapshot)
}

fn merge_history(
    previous: Option<&state::StoreSnapshot>,
    mut new_history: Vec<HistoricalSnapshot>,
    current: HistoricalSnapshot,
    limit: usize,
) -> Vec<HistoricalSnapshot> {
    let mut history = previous
        .map(|snapshot| snapshot.history.clone())
        .unwrap_or_default();
    if let Some(previous) = previous {
        push_history_snapshot(
            &mut history,
            HistoricalSnapshot {
                checkpoint: previous.checkpoint.clone(),
                entities: previous.entities.clone(),
                dynamic_sources: previous.dynamic_sources.clone(),
            },
            limit,
        );
    }
    for snapshot in new_history.drain(..) {
        push_history_snapshot(&mut history, snapshot, limit);
    }
    push_history_snapshot(&mut history, current, limit);
    history
}

fn push_history_snapshot(
    history: &mut Vec<HistoricalSnapshot>,
    snapshot: HistoricalSnapshot,
    limit: usize,
) {
    history.retain(|entry| entry.checkpoint.to_block != snapshot.checkpoint.to_block);
    history.push(snapshot);
    history.sort_by_key(|entry| entry.checkpoint.to_block);
    if limit > 0 && history.len() > limit {
        let remove = history.len() - limit;
        history.drain(0..remove);
    }
}

fn apply_reorg_policy(
    snapshot: Option<state::StoreSnapshot>,
    input: &SyncOnceInput<'_>,
) -> anyhow::Result<Option<state::StoreSnapshot>> {
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let Some(mismatch) =
        checkpoint_reorg_mismatch(&snapshot, input.chain_id, input.rpc_url.clone())?
    else {
        return Ok(Some(snapshot));
    };

    match input.reorg_policy {
        ReorgPolicy::Fail => anyhow::bail!(
            "checkpoint reorg detected at block {}: stored {}, rpc {} from {}",
            mismatch.block_number,
            mismatch.stored_hash,
            mismatch.rpc_hash,
            mismatch.rpc_url
        ),
        ReorgPolicy::Rollback => match rollback_reorg_snapshot(&snapshot, input, &mismatch.rpc_url)?
        {
            Some(rolled_back) => Ok(Some(rolled_back)),
            None => anyhow::bail!(
                "checkpoint reorg detected at block {}, but no retained safe checkpoint was found in the last {} historical snapshots; use --reorg-policy reset to rebuild from scratch",
                mismatch.block_number,
                input.reorg_check_depth
            ),
        },
        ReorgPolicy::Reset => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "level": "warn",
                    "event": "checkpoint_reorg",
                    "policy": "reset",
                    "block": mismatch.block_number,
                    "storedHash": mismatch.stored_hash,
                    "rpcHash": mismatch.rpc_hash,
                    "rpcUrl": mismatch.rpc_url
                })
            );
            Ok(None)
        }
    }
}

fn rollback_reorg_snapshot(
    snapshot: &state::StoreSnapshot,
    input: &SyncOnceInput<'_>,
    rpc_url: &str,
) -> anyhow::Result<Option<state::StoreSnapshot>> {
    for historical in snapshot.history.iter().rev().take(input.reorg_check_depth) {
        let Some(stored_hash) = historical.checkpoint.block_hash.as_deref() else {
            continue;
        };
        let block_number = historical.checkpoint.to_block;
        let rpc_hash = fetch_block_hash(rpc_url, block_number)?;
        if block_hash_mismatch(block_number, stored_hash, rpc_hash, rpc_url.to_string()).is_none() {
            let rolled_back = rollback_to_historical_snapshot(snapshot, historical);
            eprintln!(
                "{}",
                serde_json::json!({
                    "level": "warn",
                    "event": "checkpoint_reorg",
                    "policy": "rollback",
                    "fromBlock": snapshot.checkpoint.to_block,
                    "toBlock": rolled_back.checkpoint.to_block,
                    "blockHash": stored_hash,
                    "rpcUrl": rpc_url
                })
            );
            return Ok(Some(rolled_back));
        }
    }
    Ok(None)
}

fn rollback_to_historical_snapshot(
    snapshot: &state::StoreSnapshot,
    historical: &HistoricalSnapshot,
) -> state::StoreSnapshot {
    let block_number = historical.checkpoint.to_block;
    let mut rolled_back = materialize_historical_snapshot(snapshot, historical);
    rolled_back
        .history
        .retain(|entry| entry.checkpoint.to_block <= block_number);
    rolled_back.processed_logs = Vec::new();
    rolled_back
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CheckpointReorgMismatch {
    block_number: u64,
    stored_hash: String,
    rpc_hash: String,
    rpc_url: String,
}

fn checkpoint_reorg_mismatch(
    snapshot: &state::StoreSnapshot,
    chain_id: u64,
    rpc_url: Option<String>,
) -> anyhow::Result<Option<CheckpointReorgMismatch>> {
    let Some(stored_hash) = snapshot.checkpoint.block_hash.as_deref() else {
        return Ok(None);
    };
    let rpc_url = resolve_primary_rpc_url(chain_id, rpc_url)?;
    let block_number = snapshot.checkpoint.to_block;
    let rpc_hash = fetch_block_hash(&rpc_url, block_number)?;
    Ok(block_hash_mismatch(
        block_number,
        stored_hash,
        rpc_hash,
        rpc_url,
    ))
}

fn block_hash_mismatch(
    block_number: u64,
    stored_hash: &str,
    rpc_hash: Option<String>,
    rpc_url: String,
) -> Option<CheckpointReorgMismatch> {
    let rpc_hash = rpc_hash?;
    if stored_hash.eq_ignore_ascii_case(&rpc_hash) {
        None
    } else {
        Some(CheckpointReorgMismatch {
            block_number,
            stored_hash: stored_hash.to_string(),
            rpc_hash,
            rpc_url,
        })
    }
}

fn resolve_primary_rpc_url(chain_id: u64, rpc_url: Option<String>) -> anyhow::Result<String> {
    resolve_rpc_url_candidates(chain_id, rpc_url)?
        .into_iter()
        .next()
        .context("no RPC URLs resolved")
}

fn resolve_rpc_url_candidates(
    chain_id: u64,
    rpc_url: Option<String>,
) -> anyhow::Result<Vec<String>> {
    let mut rpc_opts = RpcResolverOptions::for_chain(chain_id);
    rpc_opts.explicit_rpc_url = rpc_url;
    let resolved = resolve_rpc_urls(rpc_opts)?;
    if resolved.urls.is_empty() {
        anyhow::bail!("no RPC URLs resolved");
    }
    Ok(resolved.urls)
}

fn watch_backoff_ms(base_ms: u64, max_ms: u64, failures: u32) -> u64 {
    let base_ms = base_ms.max(1_000);
    let max_ms = max_ms.max(base_ms);
    let shift = failures.saturating_sub(1).min(6);
    base_ms.saturating_mul(1_u64 << shift).min(max_ms)
}

fn rpc_timeout() -> Duration {
    let seconds = std::env::var("UGRAPH_RPC_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RPC_TIMEOUT_SECS)
        .max(1);
    Duration::from_secs(seconds)
}

fn snapshot_store(
    storage: StorageKind,
    state_file: PathBuf,
    postgres_url: Option<String>,
    deployment: String,
) -> anyhow::Result<SnapshotStore> {
    match storage {
        StorageKind::Json => Ok(SnapshotStore::Json { path: state_file }),
        StorageKind::Postgres => Ok(SnapshotStore::Postgres {
            url: postgres_url.context("missing --postgres-url for postgres storage")?,
            deployment,
        }),
    }
}

fn print_signup_status(enabled: bool, json: bool) -> anyhow::Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "publicUserSignup": enabled
            }))?
        );
    } else {
        println!("publicUserSignup: {enabled}");
    }
    Ok(())
}

fn log_source_for_sync(kind: LogSourceKind, store: &SnapshotStore) -> anyhow::Result<LogSource> {
    match kind {
        LogSourceKind::Rpc => Ok(LogSource::Rpc),
        LogSourceKind::PostgresFeed => match store {
            SnapshotStore::Postgres { url, deployment } => Ok(LogSource::PostgresFeed {
                postgres_url: url.clone(),
                deployment: deployment.clone(),
            }),
            SnapshotStore::Json { .. } => {
                anyhow::bail!("postgres-feed log source requires postgres storage")
            }
        },
    }
}

fn fetch_block_hash(rpc_url: &str, block_number: u64) -> anyhow::Result<Option<String>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(rpc_timeout())
        .build()?;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [format!("0x{block_number:x}"), false],
    });
    let response = client
        .post(rpc_url)
        .json(&body)
        .send()
        .with_context(|| format!("fetching block {block_number} from {rpc_url}"))?
        .error_for_status()
        .with_context(|| format!("RPC returned an error status for block {block_number}"))?
        .json::<serde_json::Value>()
        .with_context(|| format!("decoding block {block_number} response"))?;
    if response.get("error").is_some() {
        return Ok(None);
    }
    Ok(response
        .get("result")
        .and_then(|result| result.get("hash"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string))
}

fn post_graphql(
    endpoint: &str,
    query: &str,
    variables: &serde_json::Value,
    operation_name: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let client = reqwest::blocking::Client::new();
    let mut payload = serde_json::Map::new();
    payload.insert(
        "query".to_string(),
        serde_json::Value::String(query.to_string()),
    );
    if !variables.is_null() {
        payload.insert("variables".to_string(), variables.clone());
    }
    if let Some(operation_name) = operation_name {
        payload.insert(
            "operationName".to_string(),
            serde_json::Value::String(operation_name.to_string()),
        );
    }
    let response = client
        .post(endpoint)
        .json(&serde_json::Value::Object(payload))
        .send()
        .with_context(|| format!("posting GraphQL request to {endpoint}"))?
        .error_for_status()
        .with_context(|| format!("GraphQL endpoint returned an error status from {endpoint}"))?;
    response
        .json()
        .with_context(|| format!("decoding GraphQL response from {endpoint}"))
}

fn load_conformance_cases(path: &std::path::Path) -> anyhow::Result<Vec<ConformanceCase>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("reading conformance cases {}", path.display()))?;
    let cases = serde_json::from_str::<Vec<ConformanceCase>>(&raw)
        .with_context(|| format!("parsing conformance cases {}", path.display()))?;
    if cases.is_empty() {
        anyhow::bail!("conformance cases file {} is empty", path.display());
    }
    Ok(cases)
}

fn default_build_dir(manifest: &std::path::Path, build_dir: Option<PathBuf>) -> PathBuf {
    build_dir.unwrap_or_else(|| {
        manifest
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("build")
    })
}

fn wasm_path_for_log(build_dir: &std::path::Path, log: &MatchedLog) -> PathBuf {
    if let Ok(manifest) = Manifest::load(build_dir.join("subgraph.yaml")) {
        let source = if log.template {
            manifest
                .templates
                .iter()
                .find(|source| source.name == log.source)
        } else {
            manifest
                .data_sources
                .iter()
                .find(|source| source.name == log.source)
        };
        if let Some(source) = source {
            if source.mapping.file.ends_with(".wasm") {
                return build_dir.join(&source.mapping.file);
            }
        }
    }
    if log.template {
        build_dir
            .join("templates")
            .join(&log.source)
            .join(format!("{}.wasm", log.source))
    } else {
        build_dir
            .join(&log.source)
            .join(format!("{}.wasm", log.source))
    }
}

fn log_order_key(log: &MatchedLog) -> (u64, u64, u64) {
    (
        log.block_number.unwrap_or(u64::MAX),
        log.transaction_index.unwrap_or(u64::MAX),
        log.log_index.unwrap_or(u64::MAX),
    )
}

fn next_unprocessed_log(
    pending_logs: &[MatchedLog],
    processed_logs: &BTreeSet<LogIdentity>,
) -> Option<MatchedLog> {
    pending_logs
        .iter()
        .filter(|log| !processed_logs.contains(&log_identity(log)))
        .min_by_key(|log| log_order_key(log))
        .cloned()
}

fn has_unprocessed_log_in_block(
    pending_logs: &[MatchedLog],
    processed_logs: &BTreeSet<LogIdentity>,
    block_number: u64,
) -> bool {
    pending_logs.iter().any(|log| {
        log.block_number == Some(block_number) && !processed_logs.contains(&log_identity(log))
    })
}

fn validation_error_count(execution: &ugraph_runtime::HandlerExecutionReport) -> usize {
    execution
        .store_sets
        .iter()
        .map(|set| set.validation_errors.len())
        .sum()
}

fn dynamic_source_key(name: &str, address: &str) -> (String, String) {
    (name.to_string(), address.to_lowercase())
}

fn processed_log_set(snapshot: &state::StoreSnapshot) -> BTreeSet<LogIdentity> {
    snapshot
        .processed_logs
        .iter()
        .map(|log| {
            (
                log.source.clone(),
                log.template,
                log.address.to_lowercase(),
                log.block_number,
                log.transaction_index,
                log.log_index,
                log.topic0.clone(),
            )
        })
        .collect()
}

fn processed_log_snapshots(logs: &BTreeSet<LogIdentity>) -> Vec<ProcessedLogSnapshot> {
    logs.iter()
        .map(
            |(source, template, address, block_number, transaction_index, log_index, topic0)| {
                ProcessedLogSnapshot {
                    source: source.clone(),
                    template: *template,
                    address: address.clone(),
                    block_number: *block_number,
                    transaction_index: *transaction_index,
                    log_index: *log_index,
                    topic0: topic0.clone(),
                }
            },
        )
        .collect()
}

fn log_identity(log: &MatchedLog) -> LogIdentity {
    (
        log.source.clone(),
        log.template,
        log.address.to_lowercase(),
        log.block_number.unwrap_or(u64::MAX),
        log.transaction_index.unwrap_or(u64::MAX),
        log.log_index.unwrap_or(u64::MAX),
        log.topic0.clone(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::state::{EntitySnapshot, StoreSnapshot};
    use ugraph_core::EntitySchema;

    #[test]
    fn watch_backoff_is_exponential_and_capped() {
        assert_eq!(watch_backoff_ms(1_000, 60_000, 1), 1_000);
        assert_eq!(watch_backoff_ms(1_000, 60_000, 2), 2_000);
        assert_eq!(watch_backoff_ms(1_000, 60_000, 3), 4_000);
        assert_eq!(watch_backoff_ms(1_000, 5_000, 5), 5_000);
    }

    #[test]
    fn pending_log_helpers_respect_order_and_block_completion() {
        let first = test_log(10, 0);
        let second = test_log(10, 1);
        let third = test_log(11, 0);
        let pending = vec![third.clone(), second.clone(), first.clone()];
        let mut processed = BTreeSet::new();

        assert_eq!(
            next_unprocessed_log(&pending, &processed).and_then(|log| log.log_index),
            Some(0)
        );
        assert!(has_unprocessed_log_in_block(&pending, &processed, 10));

        processed.insert(log_identity(&first));
        assert!(has_unprocessed_log_in_block(&pending, &processed, 10));

        processed.insert(log_identity(&second));
        assert!(!has_unprocessed_log_in_block(&pending, &processed, 10));
        assert_eq!(
            next_unprocessed_log(&pending, &processed).map(|log| log.block_number),
            Some(Some(11))
        );
    }

    #[test]
    fn validation_error_count_sums_store_set_errors() {
        let execution = ugraph_runtime::HandlerExecutionReport {
            wasm_path: "mapping.wasm".to_string(),
            handler: "handleEvent".to_string(),
            event_ptr: 1,
            call_counts: BTreeMap::new(),
            store_sets: vec![
                ugraph_runtime::StoreSetCall {
                    entity: Some("Entity".to_string()),
                    id: Some("1".to_string()),
                    data: None,
                    validation_errors: vec!["missing data".to_string()],
                },
                ugraph_runtime::StoreSetCall {
                    entity: Some("Entity".to_string()),
                    id: Some("2".to_string()),
                    data: None,
                    validation_errors: vec!["missing field".to_string(), "wrong type".to_string()],
                },
            ],
            data_source_creates: Vec::new(),
            ethereum_calls: Vec::new(),
        };

        assert_eq!(validation_error_count(&execution), 3);
    }

    #[test]
    fn block_hash_mismatch_is_case_insensitive() {
        assert_eq!(
            block_hash_mismatch(
                42,
                "0xABC",
                Some("0xabc".to_string()),
                "https://rpc.example".to_string(),
            ),
            None
        );
    }

    #[test]
    fn block_hash_mismatch_reports_reorg_details() {
        let mismatch = block_hash_mismatch(
            42,
            "0xabc",
            Some("0xdef".to_string()),
            "https://rpc.example".to_string(),
        )
        .expect("mismatch");

        assert_eq!(mismatch.block_number, 42);
        assert_eq!(mismatch.stored_hash, "0xabc");
        assert_eq!(mismatch.rpc_hash, "0xdef");
        assert_eq!(mismatch.rpc_url, "https://rpc.example");
    }

    #[test]
    fn rollback_to_historical_snapshot_prunes_later_history() {
        let target = HistoricalSnapshot {
            checkpoint: SyncCheckpoint {
                from_block: Some(1),
                to_block: 90,
                block_hash: Some("0xsafe".to_string()),
                scanned_logs: 10,
                executed_logs: 10,
                validation_errors: 0,
                complete: true,
            },
            entities: vec![EntitySnapshot {
                entity: "Protocol".to_string(),
                id: "0xabc".to_string(),
                data: BTreeMap::new(),
            }],
            dynamic_sources: Vec::new(),
        };
        let later = HistoricalSnapshot {
            checkpoint: SyncCheckpoint {
                from_block: Some(1),
                to_block: 100,
                block_hash: Some("0xreorged".to_string()),
                scanned_logs: 11,
                executed_logs: 11,
                validation_errors: 0,
                complete: true,
            },
            entities: Vec::new(),
            dynamic_sources: Vec::new(),
        };
        let snapshot = StoreSnapshot {
            version: 1,
            manifest: "subgraph.yaml".to_string(),
            checkpoint: later.checkpoint.clone(),
            schema: EntitySchema::default(),
            entities: Vec::new(),
            dynamic_sources: Vec::new(),
            processed_logs: Vec::new(),
            history: vec![target.clone(), later],
        };

        let rolled_back = rollback_to_historical_snapshot(&snapshot, &target);

        assert_eq!(rolled_back.checkpoint.to_block, 90);
        assert_eq!(rolled_back.entities.len(), 1);
        assert_eq!(rolled_back.history.len(), 1);
        assert_eq!(rolled_back.history[0].checkpoint.to_block, 90);
    }

    #[test]
    fn deploy_continues_only_for_incomplete_postgres_feed_without_errors() {
        assert!(deploy_should_continue(
            &test_snapshot(false, 0),
            LogSourceKind::PostgresFeed
        ));
        assert!(!deploy_should_continue(
            &test_snapshot(true, 0),
            LogSourceKind::PostgresFeed
        ));
        assert!(!deploy_should_continue(
            &test_snapshot(false, 1),
            LogSourceKind::PostgresFeed
        ));
        assert!(!deploy_should_continue(
            &test_snapshot(false, 0),
            LogSourceKind::Rpc
        ));
    }

    fn test_snapshot(complete: bool, validation_errors: usize) -> StoreSnapshot {
        StoreSnapshot {
            version: 1,
            manifest: "subgraph.yaml".to_string(),
            checkpoint: SyncCheckpoint {
                from_block: Some(1),
                to_block: 2,
                block_hash: None,
                scanned_logs: 0,
                executed_logs: 0,
                validation_errors,
                complete,
            },
            schema: EntitySchema::default(),
            entities: Vec::new(),
            dynamic_sources: Vec::new(),
            processed_logs: Vec::new(),
            history: Vec::new(),
        }
    }

    fn test_log(block_number: u64, log_index: u64) -> MatchedLog {
        MatchedLog {
            source: "Source".to_string(),
            template: false,
            handler: "handleEvent".to_string(),
            signature: "Event()".to_string(),
            network: Some("mainnet".to_string()),
            topic0: "0x00".to_string(),
            address: "0x0000000000000000000000000000000000000001".to_string(),
            block_number: Some(block_number),
            block_hash: Some(format!("0x{block_number:064x}")),
            transaction_hash: Some(format!("0x{log_index:064x}")),
            transaction_index: Some(0),
            log_index: Some(log_index),
            topics: Vec::new(),
            data: "0x".to_string(),
            params: Vec::new(),
        }
    }
}
