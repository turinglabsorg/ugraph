use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    thread,
    time::Duration,
};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use ugraph_core::{
    build_indexing_plan, instantiate_dynamic_source, latest_block_number, resolve_rpc_urls,
    scan_planned_source, scan_raw_logs, scan_static_sources, throttle_rpc_request, EntitySchema,
    Manifest, MatchedLog, RpcResolverOptions, ScanOptions, ScanReport, ScanSourceReport,
    SourcePlan,
};
use ugraph_service::{server, state, storage};

use ugraph_service::state::{
    entity_store_from_snapshot, historical_snapshot_from_store, materialize_historical_snapshot,
    snapshot_from_store, DynamicSourceSnapshot, HistoricalSnapshot, ProcessedLogSnapshot,
    SyncCheckpoint,
};
use ugraph_service::storage::SnapshotStore;

type LogIdentity = (String, bool, String, u64, u64, u64, String);
const DEFAULT_RPC_TIMEOUT_SECS: u64 = 15;

#[derive(Debug, Parser)]
#[command(name = "ugraph-node")]
#[command(about = "UGraph node runtime for serving and indexing subgraphs")]
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
enum ReorgPolicy {
    Fail,
    Rollback,
    Reset,
}

#[derive(Debug, Subcommand)]
enum Command {
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
        #[arg(
            long,
            env = "UGRAPH_SYNC_MAX_BLOCKS_PER_PASS",
            default_value_t = 10_000
        )]
        max_blocks_per_pass: u64,
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
        #[arg(long, env = "UGRAPH_CHAIN_ID")]
        chain_id: Option<u64>,
        #[arg(long, env = "UGRAPH_BLOCK_EXPLORER_URL")]
        block_explorer_url: Option<String>,
        #[arg(long, env = "UGRAPH_HOST", default_value = "127.0.0.1")]
        host: String,
        #[arg(long, env = "UGRAPH_PORT", default_value_t = 8030)]
        port: u16,
        #[arg(long)]
        once: bool,
    },
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

#[derive(Debug)]
struct ReplayInput {
    manifest: PathBuf,
    build_dir: PathBuf,
    chain_id: u64,
    rpc_urls: Vec<String>,
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

struct SourceScanInput<'a> {
    log_source: &'a LogSource,
    chain_id: u64,
    rpc_urls: &'a [String],
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
    block_timestamp: Option<u64>,
    history: Vec<HistoricalSnapshot>,
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
    max_blocks_per_pass: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CheckpointReorgMismatch {
    block_number: u64,
    stored_hash: String,
    rpc_hash: String,
    rpc_url: String,
}

#[derive(Debug, Clone, Default)]
struct BlockMetadata {
    hash: Option<String>,
    timestamp: Option<u64>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
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
            max_blocks_per_pass,
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
                    max_blocks_per_pass,
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
        Command::Serve {
            state_file,
            storage,
            postgres_url,
            deployment,
            chain_id,
            block_explorer_url,
            host,
            port,
            once,
        } => {
            let store = snapshot_store(storage, state_file, postgres_url, deployment)?;
            server::serve_store(
                store,
                &format!("{host}:{port}"),
                once,
                server::ServeOptions {
                    chain_id,
                    block_explorer_url,
                },
            )?;
        }
    }
    Ok(())
}

fn run_replay(input: ReplayInput) -> anyhow::Result<ReplayRun> {
    let schema = EntitySchema::load_for_manifest(&input.manifest)?;
    let plan = build_indexing_plan(&input.manifest)?;
    let mut feed_backfill_pending = false;
    let scan = match &input.log_source {
        LogSource::Rpc => scan_static_sources_with_candidates(
            input.manifest.clone(),
            &input.rpc_urls,
            input.from_block,
            input.to_block,
            input.max_block_range,
            input.rpc_retries,
        )?,
        LogSource::PostgresFeed {
            postgres_url,
            deployment,
        } => {
            let rpc_url = primary_rpc_url(&input.rpc_urls)?.to_string();
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
    let replay_rpc_urls = preferred_rpc_candidates(&input.rpc_urls, &scan.rpc_url);

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
    let mut block_metadata_cache = BTreeMap::<u64, BlockMetadata>::new();

    for known in &input.known_dynamic_sources {
        let Some(source) =
            instantiate_dynamic_source(&plan, &known.name, &known.params, known.created_at_block)
        else {
            continue;
        };
        let dynamic_scan = scan_source_for_replay(SourceScanInput {
            log_source: &input.log_source,
            chain_id: input.chain_id,
            rpc_urls: &replay_rpc_urls,
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
    while let Some(log) = next_unprocessed_log(&pending_logs, &processed_logs) {
        if input.limit == 0
            || (executions.len() >= input.limit && log.block_number != last_executed_block)
        {
            break;
        }
        let mut runtime_log = log.clone();
        if let Some(block_number) = runtime_log.block_number {
            let block_metadata =
                cached_block_metadata(&mut block_metadata_cache, &replay_rpc_urls, block_number)
                    .unwrap_or_default();
            if runtime_log.block_hash.is_none() {
                runtime_log.block_hash = block_metadata.hash;
            }
            if runtime_log.block_timestamp.is_none() {
                runtime_log.block_timestamp = block_metadata.timestamp;
            }
        }
        let wasm_path = wasm_path_for_log(&input.build_dir, &runtime_log);
        let mut candidate_store = entity_store.clone();
        let mut candidate_call_cache = ethereum_call_cache.clone();
        let data_source_context =
            dynamic_source_contexts.get(&dynamic_source_key(&log.source, &log.address));
        let mut execution = ugraph_runtime::execute_matched_log_handler_with_runtime_cache_data_source_context_and_rpc_urls(
            wasm_path,
            &runtime_log,
            &mut candidate_store,
            &replay_rpc_urls,
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
                rpc_urls: &replay_rpc_urls,
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
            let block_metadata =
                cached_block_metadata(&mut block_metadata_cache, &replay_rpc_urls, block_number)
                    .unwrap_or_default();
            push_history_snapshot(
                &mut history,
                historical_snapshot_from_store(
                    SyncCheckpoint {
                        from_block: scan.from_block,
                        to_block: block_number,
                        block_hash: log.block_hash.clone().or(block_metadata.hash),
                        block_timestamp: block_metadata.timestamp,
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

    let current_block_metadata =
        cached_block_metadata(&mut block_metadata_cache, &replay_rpc_urls, scan.to_block)
            .unwrap_or_default();
    let block_hash = current_block_metadata
        .hash
        .or_else(|| match &input.log_source {
            LogSource::Rpc => None,
            LogSource::PostgresFeed { postgres_url, .. } => {
                storage::feed_block_hash(postgres_url, input.chain_id, scan.to_block)
                    .ok()
                    .flatten()
            }
        });
    let block_timestamp = current_block_metadata.timestamp;
    let report = ReplayReport {
        rpc_url: rpc_endpoint_label(&scan.rpc_url),
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
        block_timestamp,
        history,
    })
}

fn scan_static_sources_with_candidates(
    manifest: PathBuf,
    rpc_urls: &[String],
    from_block: Option<u64>,
    to_block: Option<u64>,
    max_block_range: u64,
    rpc_retries: u32,
) -> anyhow::Result<ScanReport> {
    let mut last_scan_error = None;
    for rpc_url in rpc_urls {
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
                last_scan_error = Some(anyhow::Error::new(error).context(format!(
                    "scanning RPC candidate {}",
                    rpc_endpoint_label(rpc_url)
                )));
            }
        }
    }
    Err(last_scan_error.unwrap_or_else(|| anyhow::anyhow!("no RPC URLs resolved")))
}

fn scan_source_for_replay(input: SourceScanInput<'_>) -> anyhow::Result<ScanSourceReport> {
    match input.log_source {
        LogSource::Rpc => {
            let mut last_scan_error = None;
            for rpc_url in input.rpc_urls {
                match scan_planned_source(
                    rpc_url,
                    input.source,
                    input.from_block,
                    input.to_block,
                    input.max_block_range,
                    input.rpc_retries,
                ) {
                    Ok(report) => return Ok(report),
                    Err(error) => {
                        last_scan_error = Some(anyhow::Error::new(error).context(format!(
                            "scanning dynamic source {} with RPC candidate {}",
                            input.source.name,
                            rpc_endpoint_label(rpc_url)
                        )));
                    }
                }
            }
            Err(last_scan_error.unwrap_or_else(|| anyhow::anyhow!("no RPC URLs resolved")))
        }
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

fn sync_once(input: SyncOnceInput<'_>) -> anyhow::Result<state::StoreSnapshot> {
    let loaded = if input.reset {
        None
    } else {
        input.store.try_load()?
    };
    let rpc_urls = resolve_rpc_url_candidates(input.chain_id, input.rpc_url.clone())?;
    let previous = apply_reorg_policy(loaded, &input, &rpc_urls)?;
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
    let from_block = sync_start_block(input.from_block, previous.as_ref(), input.reset);
    let to_block = match input.log_source {
        LogSource::Rpc => bounded_sync_to_block(
            &rpc_urls,
            from_block,
            input.to_block,
            input.max_blocks_per_pass,
        )?,
        LogSource::PostgresFeed { .. } => input.to_block,
    };
    let run = run_replay(ReplayInput {
        manifest: input.manifest.to_path_buf(),
        build_dir: input.build_dir.to_path_buf(),
        chain_id: input.chain_id,
        rpc_urls,
        log_source: input.log_source.clone(),
        from_block,
        to_block,
        limit: input.limit,
        max_block_range: input.max_block_range,
        rpc_retries: input.rpc_retries,
        initial_store,
        known_dynamic_sources,
        processed_logs,
    })?;
    if let Some(previous) = previous.as_ref() {
        if should_keep_previous_checkpoint(
            previous,
            from_block,
            run.report.to_block,
            run.complete,
            run.report.executed_logs,
            run.report.validation_errors,
            input.to_block,
        ) {
            return Ok(previous.clone());
        }
    }
    let checkpoint = SyncCheckpoint {
        from_block: run.report.from_block,
        to_block: run.report.to_block,
        block_hash: run.block_hash.clone(),
        block_timestamp: run.block_timestamp,
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
    let current_history =
        historical_snapshot_from_store(checkpoint, &run.entity_store, run.dynamic_sources);
    let mut activity_history = run.history.clone();
    activity_history.push(current_history.clone());
    snapshot.history = merge_history(
        previous.as_ref(),
        run.history,
        current_history,
        input.history_limit,
    );
    input.store.write_with_activity(
        &snapshot,
        &activity_history,
        previous.as_ref(),
        input.reset,
    )?;
    Ok(snapshot)
}

fn bounded_sync_to_block(
    rpc_urls: &[String],
    from_block: Option<u64>,
    explicit_to_block: Option<u64>,
    max_blocks_per_pass: u64,
) -> anyhow::Result<Option<u64>> {
    if explicit_to_block.is_some() || max_blocks_per_pass == 0 {
        return Ok(explicit_to_block);
    }
    let Some(from_block) = from_block else {
        return Ok(None);
    };
    let head = latest_block_number_with_fallback(rpc_urls)?;
    if from_block > head {
        return Ok(Some(head));
    }
    Ok(Some(bounded_to_block_from_head(
        from_block,
        head,
        max_blocks_per_pass,
    )))
}

fn bounded_to_block_from_head(from_block: u64, head: u64, max_blocks_per_pass: u64) -> u64 {
    if from_block > head {
        return head;
    }
    from_block
        .saturating_add(max_blocks_per_pass.saturating_sub(1))
        .min(head)
}

fn should_keep_previous_checkpoint(
    previous: &state::StoreSnapshot,
    started_from_block: Option<u64>,
    report_to_block: u64,
    complete: bool,
    executed_logs: usize,
    validation_errors: usize,
    explicit_to_block: Option<u64>,
) -> bool {
    explicit_to_block.is_none()
        && previous.checkpoint.complete
        && complete
        && executed_logs == 0
        && validation_errors == 0
        && report_to_block <= previous.checkpoint.to_block
        && started_from_block.is_some_and(|from_block| from_block > report_to_block)
}

fn sync_start_block(
    configured_from_block: Option<u64>,
    previous: Option<&state::StoreSnapshot>,
    reset: bool,
) -> Option<u64> {
    if reset || previous.is_none() {
        return configured_from_block;
    }
    previous.map(|snapshot| {
        if snapshot.checkpoint.complete {
            snapshot.checkpoint.to_block.saturating_add(1)
        } else {
            snapshot
                .checkpoint
                .from_block
                .unwrap_or(snapshot.checkpoint.to_block)
        }
    })
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
    rpc_urls: &[String],
) -> anyhow::Result<Option<state::StoreSnapshot>> {
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let Some(mismatch) = checkpoint_reorg_mismatch(&snapshot, rpc_urls)? else {
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
        ReorgPolicy::Rollback => match rollback_reorg_snapshot(&snapshot, input, rpc_urls)? {
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
    rpc_urls: &[String],
) -> anyhow::Result<Option<state::StoreSnapshot>> {
    for historical in snapshot.history.iter().rev().take(input.reorg_check_depth) {
        let Some(stored_hash) = historical.checkpoint.block_hash.as_deref() else {
            continue;
        };
        let block_number = historical.checkpoint.to_block;
        let rpc_hash = fetch_block_hash_with_fallback(rpc_urls, block_number)?;
        let rpc_label = rpc_candidates_label(rpc_urls);
        if block_hash_mismatch(block_number, stored_hash, rpc_hash, rpc_label.clone()).is_none() {
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
                    "rpcUrl": rpc_label
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

fn checkpoint_reorg_mismatch(
    snapshot: &state::StoreSnapshot,
    rpc_urls: &[String],
) -> anyhow::Result<Option<CheckpointReorgMismatch>> {
    let Some(stored_hash) = snapshot.checkpoint.block_hash.as_deref() else {
        return Ok(None);
    };
    let block_number = snapshot.checkpoint.to_block;
    let rpc_hash = fetch_block_hash_with_fallback(rpc_urls, block_number)?;
    Ok(block_hash_mismatch(
        block_number,
        stored_hash,
        rpc_hash,
        rpc_candidates_label(rpc_urls),
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

fn primary_rpc_url(rpc_urls: &[String]) -> anyhow::Result<&str> {
    rpc_urls
        .first()
        .map(String::as_str)
        .context("no RPC URLs resolved")
}

fn preferred_rpc_candidates(rpc_urls: &[String], preferred: &str) -> Vec<String> {
    let mut ordered = Vec::new();
    if !preferred.trim().is_empty() {
        ordered.push(preferred.to_string());
    }
    for rpc_url in rpc_urls {
        if !ordered.iter().any(|candidate| candidate == rpc_url) {
            ordered.push(rpc_url.clone());
        }
    }
    ordered
}

fn latest_block_number_with_fallback(rpc_urls: &[String]) -> anyhow::Result<u64> {
    let mut last_error = None;
    for rpc_url in rpc_urls {
        match latest_block_number(rpc_url) {
            Ok(block) => return Ok(block),
            Err(error) => {
                last_error = Some(anyhow::Error::new(error).context(format!(
                    "fetching latest block from RPC candidate {}",
                    rpc_endpoint_label(rpc_url)
                )));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no RPC URLs resolved")))
}

fn rpc_candidates_label(rpc_urls: &[String]) -> String {
    rpc_urls
        .iter()
        .map(|rpc_url| rpc_endpoint_label(rpc_url))
        .collect::<Vec<_>>()
        .join(",")
}

fn rpc_endpoint_label(rpc_url: &str) -> String {
    reqwest::Url::parse(rpc_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| "rpc-candidate".to_string())
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

fn cached_block_metadata(
    cache: &mut BTreeMap<u64, BlockMetadata>,
    rpc_urls: &[String],
    block_number: u64,
) -> anyhow::Result<BlockMetadata> {
    if let Some(metadata) = cache.get(&block_number) {
        return Ok(metadata.clone());
    }
    let metadata = fetch_block_metadata_with_fallback(rpc_urls, block_number)?;
    cache.insert(block_number, metadata.clone());
    Ok(metadata)
}

fn fetch_block_hash(rpc_url: &str, block_number: u64) -> anyhow::Result<Option<String>> {
    Ok(fetch_block_metadata(rpc_url, block_number)?.hash)
}

fn fetch_block_hash_with_fallback(
    rpc_urls: &[String],
    block_number: u64,
) -> anyhow::Result<Option<String>> {
    Ok(fetch_block_metadata_with_fallback(rpc_urls, block_number)?.hash)
}

fn fetch_block_metadata_with_fallback(
    rpc_urls: &[String],
    block_number: u64,
) -> anyhow::Result<BlockMetadata> {
    let mut last_error = None;
    for rpc_url in rpc_urls {
        match fetch_block_metadata(rpc_url, block_number) {
            Ok(metadata) => return Ok(metadata),
            Err(error) => {
                last_error = Some(error.context(format!(
                    "fetching block {block_number} from RPC candidate {}",
                    rpc_endpoint_label(rpc_url)
                )));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no RPC URLs resolved")))
}

fn fetch_block_metadata(rpc_url: &str, block_number: u64) -> anyhow::Result<BlockMetadata> {
    let client = reqwest::blocking::Client::builder()
        .timeout(rpc_timeout())
        .build()?;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [format!("0x{block_number:x}"), false],
    });
    throttle_rpc_request();
    let response = client
        .post(rpc_url)
        .json(&body)
        .send()
        .with_context(|| format!("fetching block {block_number}"))?;
    let status = response.status();
    let response_text = response
        .text()
        .with_context(|| format!("reading block {block_number} response"))?;
    if !status.is_success() {
        anyhow::bail!("RPC returned HTTP status {status} for block {block_number}");
    }
    let response = serde_json::from_str::<serde_json::Value>(&response_text)
        .with_context(|| format!("decoding block {block_number} response"))?;
    if response.get("error").is_some() {
        return Ok(BlockMetadata::default());
    }
    let result = response.get("result");
    let hash = result
        .and_then(|result| result.get("hash"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let timestamp = result
        .and_then(|result| result.get("timestamp"))
        .and_then(serde_json::Value::as_str)
        .and_then(ugraph_core::parse_rpc_u64);
    Ok(BlockMetadata { hash, timestamp })
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
    use ugraph_core::EntitySchema;
    use ugraph_service::state::{EntitySnapshot, StoreSnapshot};

    #[test]
    fn watch_backoff_is_exponential_and_capped() {
        assert_eq!(watch_backoff_ms(1_000, 60_000, 1), 1_000);
        assert_eq!(watch_backoff_ms(1_000, 60_000, 2), 2_000);
        assert_eq!(watch_backoff_ms(1_000, 60_000, 3), 4_000);
        assert_eq!(watch_backoff_ms(1_000, 5_000, 5), 5_000);
    }

    #[test]
    fn bounded_to_block_caps_watch_passes() {
        assert_eq!(bounded_to_block_from_head(100, 1_000, 10), 109);
        assert_eq!(bounded_to_block_from_head(100, 105, 10), 105);
        assert_eq!(bounded_to_block_from_head(110, 105, 10), 105);
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
                block_timestamp: None,
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
                block_timestamp: None,
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
    fn sync_start_block_uses_configured_block_only_for_initial_or_reset_sync() {
        let complete = test_snapshot(true, 0);
        assert_eq!(sync_start_block(Some(100), None, false), Some(100));
        assert_eq!(sync_start_block(Some(100), Some(&complete), false), Some(3));
        assert_eq!(
            sync_start_block(Some(100), Some(&complete), true),
            Some(100)
        );

        let mut incomplete = test_snapshot(false, 0);
        incomplete.checkpoint.from_block = Some(50);
        incomplete.checkpoint.to_block = 60;
        assert_eq!(
            sync_start_block(Some(100), Some(&incomplete), false),
            Some(50)
        );
    }

    #[test]
    fn sync_keeps_previous_checkpoint_when_rpc_head_is_not_newer() {
        let previous = test_snapshot(true, 0);
        assert!(should_keep_previous_checkpoint(
            &previous,
            Some(3),
            2,
            true,
            0,
            0,
            None
        ));
        assert!(!should_keep_previous_checkpoint(
            &previous,
            Some(3),
            4,
            true,
            0,
            0,
            None
        ));
        assert!(!should_keep_previous_checkpoint(
            &previous,
            Some(3),
            2,
            true,
            1,
            0,
            None
        ));
        assert!(!should_keep_previous_checkpoint(
            &previous,
            Some(3),
            2,
            true,
            0,
            0,
            Some(2)
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
                block_timestamp: None,
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
            block_timestamp: None,
            transaction_hash: Some(format!("0x{log_index:064x}")),
            transaction_index: Some(0),
            log_index: Some(log_index),
            topics: Vec::new(),
            data: "0x".to_string(),
            params: Vec::new(),
        }
    }
}
