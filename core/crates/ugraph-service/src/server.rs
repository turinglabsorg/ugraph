use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Component, Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use serde::Deserialize;
use serde_json::json;
use ugraph_runtime::{EntityData, StoreValue};

use crate::{
    query::{execute_graphql_with_context, query_needs_history, GraphqlHttpRequest},
    state::StoreSnapshot,
    storage::{self, SnapshotStore, StoreStatus},
};

const MAX_HTTP_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_SYNC_ACTIVITY_LIMIT: usize = 8;
const MAX_SYNC_ACTIVITY_LIMIT: usize = 50;

#[derive(Debug, Clone, Default)]
pub struct ServeOptions {
    pub chain_id: Option<u64>,
    pub block_explorer_url: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SyncActivityOptions {
    page: usize,
    limit: usize,
    show_empty: bool,
}

impl SyncActivityOptions {
    fn from_params(params: &BTreeMap<String, String>) -> Self {
        let page = params
            .get("sync_page")
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1);
        let limit = params
            .get("sync_limit")
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SYNC_ACTIVITY_LIMIT)
            .min(MAX_SYNC_ACTIVITY_LIMIT);
        let show_empty = matches!(
            params.get("show_empty").map(String::as_str),
            Some("1" | "true" | "yes" | "on")
        );
        Self {
            page,
            limit,
            show_empty,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct GraphqlEndpoint<'a> {
    path: &'a str,
    deployment: Option<&'a str>,
    version: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteDeployFile {
    path: String,
    content_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteDeployRequest {
    deployment: String,
    version: Option<String>,
    visibility: Option<String>,
    manifest_path: String,
    build_dir: String,
    chain_id: u64,
    rpc_url: Option<String>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    reset: bool,
    sync: bool,
    limit: usize,
    max_block_range: u64,
    rpc_retries: u32,
    files: Vec<RemoteDeployFile>,
}

struct RemoteDeployResponse {
    deployment: String,
    storage_deployment: String,
    version: String,
    bundle_dir: String,
    latest_endpoint: String,
    version_endpoint: String,
    sync: serde_json::Value,
    metadata: Option<storage::DeploymentMetadataRecord>,
    deployment_version: Option<storage::DeploymentVersionRecord>,
}

struct DeployKeyActor {
    user: serde_json::Value,
    database_api_key: bool,
}

pub fn serve_store(
    store: SnapshotStore,
    bind: &str,
    once: bool,
    options: ServeOptions,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind).with_context(|| format!("binding {bind}"))?;
    println!("UGraph status: http://{bind}/");
    println!("GraphiQL: http://{bind}/graphql");
    for stream in listener.incoming() {
        let stream = stream.with_context(|| format!("accepting connection on {bind}"))?;
        if once {
            log_connection_error(handle_store_connection(stream, &store, &options), &store);
            break;
        }
        let store = store.clone();
        let options = options.clone();
        thread::spawn(move || {
            log_connection_error(handle_store_connection(stream, &store, &options), &store);
        });
    }
    Ok(())
}

fn log_connection_error(result: anyhow::Result<()>, store: &SnapshotStore) {
    if let Err(error) = result {
        if is_client_disconnect(&error) {
            return;
        }
        eprintln!(
            "{}",
            json!({
                "level": "error",
                "event": "http_request_error",
                "store": store.label(),
                "error": error.to_string()
            })
        );
    }
}

fn is_client_disconnect(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| {
                matches!(
                    io_error.kind(),
                    ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                )
            })
    })
}

fn handle_store_connection(
    mut stream: TcpStream,
    store: &SnapshotStore,
    options: &ServeOptions,
) -> anyhow::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let request = match read_request(&mut stream) {
        Ok(request) => request,
        Err(error) => {
            return write_json_status(
                &mut stream,
                "400 Bad Request",
                &json!({ "errors": [{ "message": error.to_string() }] }),
            );
        }
    };
    let (head, body) = match split_http_request(&request) {
        Ok(parts) => parts,
        Err(error) => {
            return write_json_status(
                &mut stream,
                "400 Bad Request",
                &json!({ "errors": [{ "message": error.to_string() }] }),
            );
        }
    };
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let headers = parse_headers(lines);
    let (path, query_string) = target
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((target, None));

    if method == "OPTIONS" {
        return write_response(&mut stream, "204 No Content", "text/plain", b"");
    }
    if method == "GET" && (path == "/" || path == "/status") {
        let status_store = latest_store_for_status(store).unwrap_or_else(|_| store.clone());
        let (status, store_status) = match status_store.status() {
            Ok(store_status) => ("200 OK", Some(store_status)),
            Err(_) => ("503 Service Unavailable", None),
        };
        let metadata = deployment_metadata_for_store(store).ok().flatten();
        let query_params = query_string.map(parse_query_params).unwrap_or_default();
        let sync_options = SyncActivityOptions::from_params(&query_params);
        let public_deployments = public_deployments_for_store(store).unwrap_or_default();
        let sync_activity = sync_activity_for_store(&status_store, &sync_options).ok();
        return write_response(
            &mut stream,
            status,
            "text/html; charset=utf-8",
            home_html(HomeHtmlInput {
                store,
                runtime_store: &status_store,
                status: store_status.as_ref(),
                metadata: metadata.as_ref(),
                public_deployments: &public_deployments,
                sync_activity: sync_activity.as_ref(),
                sync_options: &sync_options,
                options,
            })
            .as_bytes(),
        );
    }
    if method == "GET" && path == "/healthz" {
        let status_store = latest_store_for_status(store).unwrap_or_else(|_| store.clone());
        return write_healthz(&mut stream, &status_store);
    }
    if method == "GET" && path == "/metrics" {
        let status_store = latest_store_for_status(store).unwrap_or_else(|_| store.clone());
        return write_metrics(&mut stream, &status_store);
    }
    if path == "/api/auth/verify" {
        return match method {
            "GET" => handle_auth_verify(&mut stream, store, &headers),
            _ => write_response(
                &mut stream,
                "405 Method Not Allowed",
                "application/json",
                br#"{"errors":[{"message":"method not allowed"}]}"#,
            ),
        };
    }
    if path == "/api/deployments" {
        return match method {
            "POST" => handle_remote_deploy(&mut stream, store, &headers, body),
            _ => write_response(
                &mut stream,
                "405 Method Not Allowed",
                "application/json",
                br#"{"errors":[{"message":"method not allowed"}]}"#,
            ),
        };
    }
    if let Some(endpoint) = graphql_endpoint(path) {
        let (query_store, meta_deployment) = match resolve_graphql_store(store, endpoint) {
            Ok(Some(resolved)) => resolved,
            Ok(None) => {
                return write_response(
                    &mut stream,
                    "404 Not Found",
                    "application/json",
                    br#"{"errors":[{"message":"subgraph version not found"}]}"#,
                );
            }
            Err(error) => {
                return write_json_status(
                    &mut stream,
                    "503 Service Unavailable",
                    &json!({ "errors": [{ "message": error.to_string() }] }),
                );
            }
        };
        return match method {
            "GET" => handle_graphql_get(
                &mut stream,
                &query_store,
                &meta_deployment,
                &headers,
                endpoint,
                query_string,
            ),
            "POST" => {
                handle_graphql_post(&mut stream, &query_store, &meta_deployment, &headers, body)
            }
            _ => write_response(
                &mut stream,
                "405 Method Not Allowed",
                "application/json",
                br#"{"errors":[{"message":"method not allowed"}]}"#,
            ),
        };
    }
    write_response(
        &mut stream,
        "404 Not Found",
        "application/json",
        br#"{"errors":[{"message":"not found"}]}"#,
    )
}

fn resolve_graphql_store(
    store: &SnapshotStore,
    endpoint: GraphqlEndpoint<'_>,
) -> anyhow::Result<Option<(SnapshotStore, String)>> {
    let Some(requested_deployment) = endpoint.deployment else {
        return Ok(Some((store.clone(), deployment_name(store).to_string())));
    };
    if requested_deployment != deployment_name(store) {
        return Ok(None);
    }
    let Some(requested_version) = endpoint.version else {
        return Ok(Some((store.clone(), requested_deployment.to_string())));
    };
    match store {
        SnapshotStore::Postgres { url, .. } => {
            Ok(
                storage::resolve_deployment_storage(url, requested_deployment, requested_version)?
                    .map(|storage_deployment| {
                        (
                            SnapshotStore::Postgres {
                                url: url.clone(),
                                deployment: storage_deployment,
                            },
                            requested_deployment.to_string(),
                        )
                    }),
            )
        }
        SnapshotStore::Json { .. } => {
            if requested_version == "latest" {
                Ok(Some((store.clone(), requested_deployment.to_string())))
            } else {
                Ok(None)
            }
        }
    }
}

fn graphql_endpoint(path: &str) -> Option<GraphqlEndpoint<'_>> {
    if path == "/graphql" {
        return Some(GraphqlEndpoint {
            path,
            deployment: None,
            version: None,
        });
    }
    let mut parts = path.trim_matches('/').split('/');
    let prefix = parts.next()?;
    let deployment = parts.next()?;
    let version = parts.next()?;
    let suffix = parts.next()?;
    if parts.next().is_some() || prefix != "subgraphs" || (suffix != "gn" && suffix != "graphql") {
        return None;
    }
    Some(GraphqlEndpoint {
        path,
        deployment: Some(deployment),
        version: Some(version),
    })
}

fn handle_auth_verify(
    stream: &mut TcpStream,
    store: &SnapshotStore,
    headers: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let SnapshotStore::Postgres { url, .. } = store else {
        return write_json_status(
            stream,
            "503 Service Unavailable",
            &json!({ "errors": [{ "message": "remote auth requires postgres storage" }] }),
        );
    };
    let Some(key) = request_api_key(headers) else {
        return write_json_status(
            stream,
            "401 Unauthorized",
            &json!({ "errors": [{ "message": "api key required" }] }),
        );
    };
    match verify_deploy_key(url, key) {
        Ok(Some(actor)) => write_json(stream, &json!({ "ok": true, "user": actor.user })),
        Ok(None) => write_json_status(
            stream,
            "401 Unauthorized",
            &json!({ "errors": [{ "message": "api key is invalid or missing deploy scope" }] }),
        ),
        Err(error) => write_json_status(
            stream,
            "503 Service Unavailable",
            &json!({ "errors": [{ "message": error.to_string() }] }),
        ),
    }
}

fn handle_remote_deploy(
    stream: &mut TcpStream,
    store: &SnapshotStore,
    headers: &BTreeMap<String, String>,
    body: &[u8],
) -> anyhow::Result<()> {
    let SnapshotStore::Postgres {
        url,
        deployment: served_deployment,
    } = store
    else {
        return write_json_status(
            stream,
            "503 Service Unavailable",
            &json!({ "errors": [{ "message": "remote deploy requires postgres storage" }] }),
        );
    };
    let Some(key) = request_api_key(headers) else {
        return write_json_status(
            stream,
            "401 Unauthorized",
            &json!({ "errors": [{ "message": "api key required" }] }),
        );
    };
    let actor = match verify_deploy_key(url, key) {
        Ok(Some(actor)) => actor,
        Ok(None) => {
            return write_json_status(
                stream,
                "401 Unauthorized",
                &json!({ "errors": [{ "message": "api key is invalid or missing deploy scope" }] }),
            );
        }
        Err(error) => {
            return write_json_status(
                stream,
                "503 Service Unavailable",
                &json!({ "errors": [{ "message": error.to_string() }] }),
            );
        }
    };
    let request = match serde_json::from_slice::<RemoteDeployRequest>(body) {
        Ok(request) => request,
        Err(error) => {
            return write_json_status(
                stream,
                "400 Bad Request",
                &json!({ "errors": [{ "message": format!("invalid deploy request: {error}") }] }),
            );
        }
    };
    let metadata_api_key = actor.database_api_key.then_some(key);
    match run_remote_deploy(url, served_deployment, metadata_api_key, &request) {
        Ok(response) => write_json(
            stream,
            &json!({
                "ok": true,
                "actor": actor.user,
                "deployment": response.deployment,
                "storageDeployment": response.storage_deployment,
                "version": response.version,
                "bundleDir": response.bundle_dir,
                "latestEndpoint": response.latest_endpoint,
                "versionEndpoint": response.version_endpoint,
                "sync": response.sync,
                "metadata": response.metadata,
                "deploymentVersion": response.deployment_version,
            }),
        ),
        Err(error) => write_json_status(
            stream,
            "500 Internal Server Error",
            &json!({ "errors": [{ "message": error.to_string() }] }),
        ),
    }
}

fn run_remote_deploy(
    postgres_url: &str,
    served_deployment: &str,
    metadata_api_key: Option<&str>,
    request: &RemoteDeployRequest,
) -> anyhow::Result<RemoteDeployResponse> {
    if request.deployment.trim().is_empty() {
        anyhow::bail!("deployment is required");
    }
    if request.deployment != served_deployment {
        anyhow::bail!(
            "this ugraph instance serves deployment `{served_deployment}`; remote deploy requested `{}`",
            request.deployment
        );
    }
    let visibility = request.visibility.as_deref().unwrap_or("private");
    if visibility != "private" && visibility != "public" {
        anyhow::bail!("visibility must be `private` or `public`");
    }
    if request.files.is_empty() {
        anyhow::bail!("remote deploy bundle is empty");
    }
    let version = request
        .version
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(default_remote_version);
    let storage_deployment = format!("{}@{}", request.deployment, version);
    let bundle_dir = remote_bundle_dir(&request.deployment, &version)?;
    if bundle_dir.exists() {
        fs::remove_dir_all(&bundle_dir)
            .with_context(|| format!("removing old bundle {}", bundle_dir.display()))?;
    }
    fs::create_dir_all(&bundle_dir)
        .with_context(|| format!("creating bundle dir {}", bundle_dir.display()))?;
    for file in &request.files {
        write_remote_bundle_file(&bundle_dir, file)?;
    }
    let manifest_path = bundle_dir.join(safe_relative_path(&request.manifest_path)?);
    let build_dir = bundle_dir.join(safe_relative_path(&request.build_dir)?);
    if !manifest_path.is_file() {
        anyhow::bail!("manifest was not uploaded: {}", manifest_path.display());
    }
    if !build_dir.is_dir() {
        anyhow::bail!("build dir was not uploaded: {}", build_dir.display());
    }
    let sync = if request.sync {
        run_remote_sync(
            postgres_url,
            &storage_deployment,
            &manifest_path,
            &build_dir,
            request,
        )?
    } else {
        json!({ "skipped": true })
    };
    let (metadata, deployment_version) = if request.sync {
        let deployment_version = storage::record_deployment_version(
            postgres_url,
            storage::DeploymentVersionInput {
                deployment: &request.deployment,
                version_label: &version,
                storage_deployment: &storage_deployment,
                visibility,
                owner_email: None,
                api_key: metadata_api_key,
                promote: true,
            },
        )?;
        let metadata = storage::deployment_metadata(postgres_url, &request.deployment)?;
        (metadata, Some(deployment_version))
    } else {
        (None, None)
    };
    Ok(RemoteDeployResponse {
        deployment: request.deployment.clone(),
        storage_deployment,
        version: version.clone(),
        bundle_dir: bundle_dir.display().to_string(),
        latest_endpoint: format!("/subgraphs/{}/latest/gn", request.deployment),
        version_endpoint: format!("/subgraphs/{}/{version}/gn", request.deployment),
        sync,
        metadata,
        deployment_version,
    })
}

fn verify_deploy_key(postgres_url: &str, key: &str) -> anyhow::Result<Option<DeployKeyActor>> {
    if let Some(user) = storage::verify_api_key_scope(postgres_url, key, "deploy")? {
        return Ok(Some(DeployKeyActor {
            user: serde_json::to_value(user)?,
            database_api_key: true,
        }));
    }
    let bootstrap_key = std::env::var("UGRAPH_BOOTSTRAP_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if bootstrap_key.as_deref() == Some(key) {
        return Ok(Some(DeployKeyActor {
            user: json!({
                "id": "bootstrap",
                "email": "bootstrap@ugraph.local",
                "role": "admin"
            }),
            database_api_key: false,
        }));
    }
    Ok(None)
}

fn run_remote_sync(
    postgres_url: &str,
    storage_deployment: &str,
    manifest_path: &Path,
    build_dir: &Path,
    request: &RemoteDeployRequest,
) -> anyhow::Result<serde_json::Value> {
    let bin = std::env::var("UGRAPH_NODE_BIN")
        .ok()
        .map(PathBuf::from)
        .unwrap_or(std::env::current_exe().context("resolving current ugraph-node executable")?);
    let mut command = Command::new(bin);
    command
        .arg("sync")
        .arg("--manifest")
        .arg(manifest_path)
        .arg("--build-dir")
        .arg(build_dir)
        .arg("--storage")
        .arg("postgres")
        .arg("--postgres-url")
        .arg(postgres_url)
        .arg("--deployment")
        .arg(storage_deployment)
        .arg("--chain-id")
        .arg(request.chain_id.to_string())
        .arg("--limit")
        .arg(request.limit.max(1).to_string())
        .arg("--max-block-range")
        .arg(request.max_block_range.max(1).to_string())
        .arg("--rpc-retries")
        .arg(request.rpc_retries.to_string())
        .arg("--json");
    let rpc_url = request
        .rpc_url
        .clone()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("UGRAPH_RPC_URL").ok())
        .filter(|value| !value.trim().is_empty());
    if let Some(rpc_url) = rpc_url {
        command.arg("--rpc-url").arg(rpc_url);
    }
    if let Some(from_block) = request.from_block {
        command.arg("--from-block").arg(from_block.to_string());
    }
    if let Some(to_block) = request.to_block {
        command.arg("--to-block").arg(to_block.to_string());
    }
    if request.reset {
        command.arg("--reset");
    }
    let output = command.output().context("running remote sync")?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        anyhow::bail!(
            "remote sync failed with status {}: {}{}",
            output.status,
            stdout,
            stderr
        );
    }
    serde_json::from_str::<serde_json::Value>(&stdout).or_else(|_| {
        Ok(json!({
            "stdout": stdout,
            "stderr": stderr
        }))
    })
}

fn write_remote_bundle_file(root: &Path, file: &RemoteDeployFile) -> anyhow::Result<()> {
    let relative = safe_relative_path(&file.path)?;
    let destination = root.join(relative);
    let bytes = hex::decode(&file.content_hex)
        .with_context(|| format!("decoding hex content for {}", file.path))?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating bundle dir {}", parent.display()))?;
    }
    fs::write(&destination, bytes)
        .with_context(|| format!("writing bundle file {}", destination.display()))
}

fn remote_bundle_dir(deployment: &str, version: &str) -> anyhow::Result<PathBuf> {
    let base = std::env::var("UGRAPH_REMOTE_DEPLOY_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/data/remote-deployments"));
    Ok(base
        .join(safe_name_component(deployment)?)
        .join(safe_name_component(version)?))
}

fn safe_name_component(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        anyhow::bail!("empty path component");
    }
    if value
        .chars()
        .any(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '@')))
    {
        anyhow::bail!("unsafe path component `{value}`");
    }
    Ok(value.to_string())
}

fn safe_relative_path(value: &str) -> anyhow::Result<PathBuf> {
    let path = Path::new(value);
    if path.is_absolute() {
        anyhow::bail!("absolute paths are not allowed in deploy bundles");
    }
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("unsafe deploy bundle path `{value}`")
            }
        }
    }
    if clean.as_os_str().is_empty() {
        anyhow::bail!("empty deploy bundle path");
    }
    Ok(clean)
}

fn default_remote_version() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    format!("v{seconds}")
}

fn handle_graphql_get(
    stream: &mut TcpStream,
    store: &SnapshotStore,
    meta_deployment: &str,
    headers: &BTreeMap<String, String>,
    endpoint: GraphqlEndpoint<'_>,
    query_string: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(query_string) = query_string {
        let params = parse_query_params(query_string);
        if let Some(query) = params.get("query") {
            if !write_if_graphql_unauthorized(stream, store, headers)? {
                return Ok(());
            }
            let variables = params
                .get("variables")
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or(serde_json::Value::Null);
            let operation_name = params.get("operationName").map(String::as_str);
            return match load_snapshot_for_graphql(store, query, &variables, operation_name) {
                Ok(snapshot) => {
                    let response = execute_graphql_with_context(
                        &snapshot,
                        query,
                        &variables,
                        operation_name,
                        Some(meta_deployment),
                    );
                    write_json(stream, &response)
                }
                Err(error) => write_json_status(
                    stream,
                    "503 Service Unavailable",
                    &json!({ "errors": [{ "message": error.to_string() }] }),
                ),
            };
        }
    }
    write_response(
        stream,
        "200 OK",
        "text/html; charset=utf-8",
        graphiql_html(endpoint.path).as_bytes(),
    )
}

fn handle_graphql_post(
    stream: &mut TcpStream,
    store: &SnapshotStore,
    meta_deployment: &str,
    headers: &BTreeMap<String, String>,
    body: &[u8],
) -> anyhow::Result<()> {
    if !write_if_graphql_unauthorized(stream, store, headers)? {
        return Ok(());
    }
    let payload =
        serde_json::from_slice::<GraphqlHttpRequest>(body).unwrap_or(GraphqlHttpRequest {
            query: String::new(),
            variables: serde_json::Value::Null,
            _operation_name: None,
        });
    match load_snapshot_for_graphql(
        store,
        &payload.query,
        &payload.variables,
        payload._operation_name.as_deref(),
    ) {
        Ok(snapshot) => {
            let response = execute_graphql_with_context(
                &snapshot,
                &payload.query,
                &payload.variables,
                payload._operation_name.as_deref(),
                Some(meta_deployment),
            );
            write_json(stream, &response)
        }
        Err(error) => write_json_status(
            stream,
            "503 Service Unavailable",
            &json!({ "errors": [{ "message": error.to_string() }] }),
        ),
    }
}

fn load_snapshot_for_graphql(
    store: &SnapshotStore,
    query: &str,
    variables: &serde_json::Value,
    operation_name: Option<&str>,
) -> anyhow::Result<StoreSnapshot> {
    if query_needs_history(query, variables, operation_name) {
        store.load()
    } else {
        store.load_current()
    }
}

fn write_if_graphql_unauthorized(
    stream: &mut TcpStream,
    store: &SnapshotStore,
    headers: &BTreeMap<String, String>,
) -> anyhow::Result<bool> {
    match graphql_authorized(store, headers) {
        Ok(true) => Ok(true),
        Ok(false) => {
            write_json_status(
                stream,
                "401 Unauthorized",
                &json!({ "errors": [{ "message": "api key required" }] }),
            )?;
            Ok(false)
        }
        Err(error) => {
            write_json_status(
                stream,
                "503 Service Unavailable",
                &json!({ "errors": [{ "message": error.to_string() }] }),
            )?;
            Ok(false)
        }
    }
}

fn write_healthz(stream: &mut TcpStream, store: &SnapshotStore) -> anyhow::Result<()> {
    match store.status() {
        Ok(status) => write_json(
            stream,
            &json!({
                "ok": true,
                "store": store.label(),
                "entities": status.entities,
                "dynamicSources": status.dynamic_sources,
                "historySnapshots": status.history_snapshots,
                "historyEarliestBlock": status.history_earliest_block,
                "historyLatestBlock": status.history_latest_block,
                "toBlock": status.checkpoint.to_block,
                "blockHash": status.checkpoint.block_hash,
                "blockTimestamp": status.checkpoint.block_timestamp,
                "complete": status.checkpoint.complete,
                "validationErrors": status.checkpoint.validation_errors,
            }),
        ),
        Err(error) => write_json_status(
            stream,
            "503 Service Unavailable",
            &json!({
                "ok": false,
                "store": store.label(),
                "error": error.to_string(),
            }),
        ),
    }
}

fn write_metrics(stream: &mut TcpStream, store: &SnapshotStore) -> anyhow::Result<()> {
    match store.status() {
        Ok(status) => write_response(
            stream,
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            metrics_text(store, &status).as_bytes(),
        ),
        Err(_) => write_response(
            stream,
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            unavailable_metrics_text(store).as_bytes(),
        ),
    }
}

fn read_request(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > MAX_HTTP_REQUEST_BYTES {
            anyhow::bail!("HTTP request exceeds {MAX_HTTP_REQUEST_BYTES} bytes");
        }
        if request_complete(&buffer) {
            break;
        }
    }
    Ok(buffer)
}

fn request_complete(buffer: &[u8]) -> bool {
    let Some(header_end) = find_header_end(buffer) else {
        return false;
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]);
    let content_length = head
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    buffer.len() >= header_end + 4 + content_length
}

fn split_http_request(request: &[u8]) -> anyhow::Result<(&str, &[u8])> {
    let header_end = find_header_end(request).context("invalid HTTP request")?;
    let head = std::str::from_utf8(&request[..header_end]).context("HTTP headers are not UTF-8")?;
    Ok((head, &request[header_end + 4..]))
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn write_json(stream: &mut TcpStream, value: &serde_json::Value) -> anyhow::Result<()> {
    let body = serde_json::to_vec(value)?;
    write_response(stream, "200 OK", "application/json", &body)
}

fn write_json_status(
    stream: &mut TcpStream,
    status: &str,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(value)?;
    write_response(stream, status, "application/json", &body)
}

fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> anyhow::Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: content-type, authorization, x-api-key\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

fn parse_headers<'a>(lines: impl Iterator<Item = &'a str>) -> BTreeMap<String, String> {
    lines
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect()
}

fn graphql_authorized(
    store: &SnapshotStore,
    headers: &BTreeMap<String, String>,
) -> anyhow::Result<bool> {
    let SnapshotStore::Postgres { url, deployment } = store else {
        return Ok(true);
    };
    let visibility =
        storage::deployment_visibility(url, deployment)?.unwrap_or_else(|| "public".to_string());
    if visibility != "private" {
        return Ok(true);
    }
    let Some(key) = request_api_key(headers) else {
        return Ok(false);
    };
    Ok(storage::verify_api_key_scope(url, key, "query")?.is_some())
}

fn request_api_key(headers: &BTreeMap<String, String>) -> Option<&str> {
    if let Some(value) = headers.get("x-api-key") {
        return Some(value.as_str());
    }
    let value = headers.get("authorization")?;
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
}

fn deployment_name(store: &SnapshotStore) -> &str {
    match store {
        SnapshotStore::Postgres { deployment, .. } => deployment,
        SnapshotStore::Json { .. } => "default",
    }
}

fn deployment_metadata_for_store(
    store: &SnapshotStore,
) -> anyhow::Result<Option<storage::DeploymentMetadataRecord>> {
    match store {
        SnapshotStore::Postgres { url, deployment } => {
            storage::deployment_metadata(url, deployment)
        }
        SnapshotStore::Json { .. } => Ok(None),
    }
}

fn latest_store_for_status(store: &SnapshotStore) -> anyhow::Result<SnapshotStore> {
    match store {
        SnapshotStore::Postgres { url, deployment } => Ok(storage::resolve_deployment_storage(
            url, deployment, "latest",
        )?
        .map(|storage_deployment| SnapshotStore::Postgres {
            url: url.clone(),
            deployment: storage_deployment,
        })
        .unwrap_or_else(|| store.clone())),
        SnapshotStore::Json { .. } => Ok(store.clone()),
    }
}

fn public_deployments_for_store(
    store: &SnapshotStore,
) -> anyhow::Result<Vec<storage::DeploymentVersionRecord>> {
    match store {
        SnapshotStore::Postgres { url, .. } => storage::list_public_deployment_versions(url),
        SnapshotStore::Json { .. } => Ok(Vec::new()),
    }
}

fn sync_activity_for_store(
    store: &SnapshotStore,
    options: &SyncActivityOptions,
) -> anyhow::Result<storage::SyncActivityPage> {
    match store {
        SnapshotStore::Postgres { url, deployment } => storage::recent_sync_activity(
            url,
            deployment,
            options.page,
            options.limit,
            25,
            options.show_empty,
        ),
        SnapshotStore::Json { .. } => Ok(storage::SyncActivityPage {
            activities: Vec::new(),
            stats: storage::SyncActivityStats::default(),
            page: options.page,
            limit: options.limit,
            has_previous: options.page > 1,
            has_next: false,
            show_empty: options.show_empty,
        }),
    }
}

fn metrics_text(store: &SnapshotStore, status: &StoreStatus) -> String {
    let store = prometheus_label_value(&store.label());
    let complete = usize::from(status.checkpoint.complete);
    format!(
        concat!(
            "# HELP ugraph_store_up Whether the selected store can be loaded.\n",
            "# TYPE ugraph_store_up gauge\n",
            "ugraph_store_up{{store=\"{store}\"}} 1\n",
            "# HELP ugraph_entities Number of current-state entities in the deployment.\n",
            "# TYPE ugraph_entities gauge\n",
            "ugraph_entities{{store=\"{store}\"}} {entities}\n",
            "# HELP ugraph_dynamic_sources Number of active dynamic data sources.\n",
            "# TYPE ugraph_dynamic_sources gauge\n",
            "ugraph_dynamic_sources{{store=\"{store}\"}} {dynamic_sources}\n",
            "# HELP ugraph_history_snapshots Number of retained historical current-state snapshots.\n",
            "# TYPE ugraph_history_snapshots gauge\n",
            "ugraph_history_snapshots{{store=\"{store}\"}} {history_snapshots}\n",
            "# HELP ugraph_history_earliest_block Earliest retained historical block, or 0 when empty.\n",
            "# TYPE ugraph_history_earliest_block gauge\n",
            "ugraph_history_earliest_block{{store=\"{store}\"}} {history_earliest_block}\n",
            "# HELP ugraph_history_latest_block Latest retained historical block, or 0 when empty.\n",
            "# TYPE ugraph_history_latest_block gauge\n",
            "ugraph_history_latest_block{{store=\"{store}\"}} {history_latest_block}\n",
            "# HELP ugraph_checkpoint_to_block Last checkpoint block number.\n",
            "# TYPE ugraph_checkpoint_to_block gauge\n",
            "ugraph_checkpoint_to_block{{store=\"{store}\"}} {to_block}\n",
            "# HELP ugraph_checkpoint_complete Whether the checkpoint completed all known logs.\n",
            "# TYPE ugraph_checkpoint_complete gauge\n",
            "ugraph_checkpoint_complete{{store=\"{store}\"}} {complete}\n",
            "# HELP ugraph_validation_errors Store validation errors observed in the last sync.\n",
            "# TYPE ugraph_validation_errors gauge\n",
            "ugraph_validation_errors{{store=\"{store}\"}} {validation_errors}\n"
        ),
        store = store,
        entities = status.entities,
        dynamic_sources = status.dynamic_sources,
        history_snapshots = status.history_snapshots,
        history_earliest_block = status.history_earliest_block.unwrap_or(0),
        history_latest_block = status.history_latest_block.unwrap_or(0),
        to_block = status.checkpoint.to_block,
        complete = complete,
        validation_errors = status.checkpoint.validation_errors
    )
}

fn unavailable_metrics_text(store: &SnapshotStore) -> String {
    let store = prometheus_label_value(&store.label());
    format!(
        concat!(
            "# HELP ugraph_store_up Whether the selected store can be loaded.\n",
            "# TYPE ugraph_store_up gauge\n",
            "ugraph_store_up{{store=\"{store}\"}} 0\n"
        ),
        store = store
    )
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\n")
}

struct HomeHtmlInput<'a> {
    store: &'a SnapshotStore,
    runtime_store: &'a SnapshotStore,
    status: Option<&'a StoreStatus>,
    metadata: Option<&'a storage::DeploymentMetadataRecord>,
    public_deployments: &'a [storage::DeploymentVersionRecord],
    sync_activity: Option<&'a storage::SyncActivityPage>,
    sync_options: &'a SyncActivityOptions,
    options: &'a ServeOptions,
}

fn home_html(input: HomeHtmlInput<'_>) -> String {
    let store = input.store;
    let runtime_store = input.runtime_store;
    let status = input.status;
    let metadata = input.metadata;
    let public_deployments = input.public_deployments;
    let sync_activity = input.sync_activity;
    let sync_options = input.sync_options;
    let options = input.options;
    let ok = status
        .map(|status| status.checkpoint.complete && status.checkpoint.validation_errors == 0)
        .unwrap_or(false);
    let display_status = if ok { "OPERATIONAL" } else { "DEGRADED" };
    let badge = if ok { "ok" } else { "down" };
    let deployment = deployment_name(store);
    let version = metadata
        .and_then(|metadata| metadata.version_label.as_deref())
        .unwrap_or("latest");
    let visibility = metadata
        .map(|metadata| metadata.visibility.as_str())
        .unwrap_or("public");
    let versioned_endpoint = format!("/subgraphs/{deployment}/{version}/gn");
    let latest_endpoint = format!("/subgraphs/{deployment}/latest/gn");
    let to_block = status
        .map(|status| status.checkpoint.to_block.to_string())
        .unwrap_or_else(|| "-".to_string());
    let block_hash = status
        .and_then(|status| status.checkpoint.block_hash.as_deref())
        .unwrap_or("-");
    let entities = status
        .map(|status| status.entities.to_string())
        .unwrap_or_else(|| "-".to_string());
    let dynamic_sources = status
        .map(|status| status.dynamic_sources.to_string())
        .unwrap_or_else(|| "-".to_string());
    let entity_changes = sync_activity
        .map(|page| page.stats.entity_changes.to_string())
        .unwrap_or_else(|| "-".to_string());
    let change_blocks = sync_activity
        .map(|page| page.stats.change_blocks.to_string())
        .unwrap_or_else(|| "-".to_string());
    let indexed_checkpoints = sync_activity
        .map(|page| page.stats.indexed_checkpoints.to_string())
        .unwrap_or_else(|| "-".to_string());
    let state_cache = status
        .map(|status| status.history_snapshots.to_string())
        .unwrap_or_else(|| "-".to_string());
    let validation_errors = status
        .map(|status| status.checkpoint.validation_errors.to_string())
        .unwrap_or_else(|| "-".to_string());
    let public_subgraphs = public_subgraphs_html(public_deployments);
    let sync_controls = sync_controls_html(sync_activity, sync_options);
    let sync_blocks = sync_blocks_html(sync_activity, options);
    let chain_id = options
        .chain_id
        .map(|chain_id| chain_id.to_string())
        .unwrap_or_else(|| "-".to_string());
    let explorer = explorer_status_html(options);
    let sync_page = sync_activity
        .map(|page| page.page)
        .unwrap_or(sync_options.page);
    let sync_mode = sync_activity
        .map(|page| page.show_empty)
        .unwrap_or(sync_options.show_empty);
    let sync_page_label = format!(
        "page {sync_page} / {}",
        if sync_mode {
            "sync checkpoints"
        } else {
            "entity changes"
        }
    );
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>UGraph Status</title>
  <style>
    :root {{ color-scheme: dark; --bg:#050505; --ink:#f3f0e6; --paper:#f3f0e6; --void:#050505; --muted:#8d887d; --line:#f3f0e6; --acid:#c8ff00; --hot:#ff3b30; --blue:#38a3ff; --warn:#ffb000; }}
    * {{ box-sizing: border-box; }}
    html {{ min-height:100%; background:var(--bg); }}
    body {{ margin:0; min-height:100vh; background:var(--bg); color:var(--ink); font-family:"Courier New", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; letter-spacing:0; }}
    body::before {{ content:""; position:fixed; inset:0; pointer-events:none; opacity:.22; background-image:repeating-linear-gradient(0deg, rgba(243,240,230,.18) 0 1px, transparent 1px 24px), repeating-linear-gradient(90deg, rgba(243,240,230,.12) 0 1px, transparent 1px 24px); }}
    body::after {{ content:"UGRAPH // LIVE"; position:fixed; right:-58px; top:46%; transform:rotate(90deg); color:rgba(243,240,230,.22); font-size:12px; font-weight:700; pointer-events:none; }}
    main {{ width:min(1280px, calc(100% - 28px)); margin:0 auto; padding:18px 0 28px; position:relative; }}
    .shell {{ border:3px solid var(--line); background:var(--void); box-shadow:12px 12px 0 var(--acid); }}
    .topbar {{ display:flex; justify-content:space-between; gap:12px; padding:8px 12px; border-bottom:3px solid var(--line); background:var(--paper); color:var(--void); font-size:12px; font-weight:700; text-transform:uppercase; }}
    .maker {{ color:var(--void); text-decoration:none; border-bottom:2px solid var(--void); white-space:nowrap; }}
    header {{ display:grid; grid-template-columns:130px minmax(0, 1fr) 190px; align-items:stretch; border-bottom:3px solid var(--line); }}
    .mark {{ display:grid; place-items:center; min-height:132px; border-right:3px solid var(--line); background:var(--hot); color:var(--void); font-size:34px; font-weight:700; line-height:1; }}
    .brand {{ min-width:0; padding:16px 18px 14px; }}
    h1 {{ margin:0; font-size:72px; line-height:.9; font-weight:700; letter-spacing:0; text-transform:uppercase; }}
    .subtitle {{ margin-top:10px; color:var(--ink); font-size:14px; line-height:1.35; word-break:break-word; }}
    .status {{ display:grid; place-items:center; border-left:3px solid var(--line); padding:12px; color:var(--warn); background:var(--void); font-size:18px; font-weight:700; text-align:center; text-transform:uppercase; }}
    .status.ok {{ color:var(--void); background:var(--acid); }}
    .grid {{ display:grid; grid-template-columns:1.6fr repeat(4, minmax(0, 1fr)); border-bottom:3px solid var(--line); }}
    .metric {{ min-width:0; padding:13px 12px 12px; border-right:3px solid var(--line); background:var(--void); }}
    .metric:nth-child(odd) {{ background:#101010; }}
    .metric:last-child {{ border-right:0; }}
    .label {{ color:var(--muted); font-size:11px; font-weight:700; text-transform:uppercase; }}
    .value {{ margin-top:8px; font-size:28px; line-height:1; font-weight:700; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .metric:first-child .value {{ color:var(--acid); font-size:34px; }}
    .content {{ display:grid; grid-template-columns:1.35fr .65fr; }}
    .panel {{ padding:16px; border-right:3px solid var(--line); min-width:0; }}
    .panel:last-child {{ border-right:0; }}
    .terminal {{ margin:0; padding:0; list-style:none; display:grid; gap:0; font-size:14px; line-height:1.35; }}
    .terminal li {{ display:grid; grid-template-columns:140px minmax(0,1fr); gap:12px; align-items:start; padding:9px 0; border-bottom:1px solid rgba(243,240,230,.28); }}
    .terminal li:last-child {{ border-bottom:0; }}
    .key {{ color:var(--acid); font-weight:700; text-transform:uppercase; }}
    code, a {{ color:var(--ink); word-break:break-all; }}
    a {{ text-decoration-thickness:2px; text-underline-offset:3px; }}
    a:hover {{ color:var(--acid); }}
    .ok-text {{ color:var(--acid); font-weight:700; }}
    .warn-text {{ color:var(--warn); font-weight:700; }}
    .hash {{ color:#c8c1ad; }}
    .section {{ border-top:3px solid var(--line); }}
    .section-head {{ display:flex; flex-wrap:wrap; align-items:stretch; justify-content:space-between; gap:0; background:var(--paper); color:var(--void); }}
    .section-title {{ padding:8px 12px; color:var(--void); font-size:12px; font-weight:700; text-transform:uppercase; }}
    .sync-controls {{ display:flex; flex-wrap:wrap; margin-left:auto; border-left:3px solid var(--void); }}
    .control {{ display:inline-flex; align-items:center; min-height:31px; padding:7px 10px; border-right:3px solid var(--void); color:var(--void); background:var(--paper); font-size:12px; font-weight:700; text-decoration:none; text-transform:uppercase; }}
    .control:last-child {{ border-right:0; }}
    .control:hover {{ background:var(--void); color:var(--acid); }}
    .control.disabled {{ color:#6b665d; pointer-events:none; text-decoration:none; }}
    .subgraph-row {{ display:grid; grid-template-columns:1.1fr .7fr 1fr 1.7fr; gap:0; border-top:3px solid var(--line); }}
    .subgraph-cell {{ min-width:0; padding:12px; border-right:3px solid var(--line); }}
    .subgraph-cell:last-child {{ border-right:0; }}
    .subgraph-name {{ color:var(--acid); font-size:18px; font-weight:700; }}
    .subgraph-label {{ display:block; margin-bottom:6px; color:var(--muted); font-size:11px; font-weight:700; text-transform:uppercase; }}
    .sync-summary {{ display:grid; grid-template-columns:repeat(6, minmax(0, 1fr)); border-top:3px solid var(--line); }}
    .sync-summary span {{ min-width:0; padding:9px 10px; border-right:3px solid var(--line); font-size:12px; font-weight:700; text-transform:uppercase; }}
    .sync-summary span:last-child {{ border-right:0; }}
    .sync-summary b {{ color:var(--acid); }}
    .sync-row {{ display:grid; grid-template-columns:170px minmax(0, 1fr); border-top:3px solid var(--line); }}
    .sync-block {{ padding:12px; border-right:3px solid var(--line); background:#101010; color:var(--acid); font-size:20px; font-weight:700; }}
    .sync-block a {{ color:var(--acid); }}
    .block-meta {{ margin-top:8px; color:var(--ink); font-size:12px; font-weight:700; line-height:1.35; word-break:break-word; }}
    .block-meta a {{ color:var(--ink); }}
    .block-hash {{ color:var(--muted); }}
    .sync-detail {{ min-width:0; padding:12px; }}
    .sync-counts {{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:10px; }}
    .sync-count {{ border:2px solid var(--line); padding:5px 7px; font-size:12px; font-weight:700; text-transform:uppercase; }}
    .sync-count.created {{ background:var(--acid); color:var(--void); }}
    .sync-count.updated {{ background:var(--blue); color:var(--void); }}
    .sync-count.removed {{ background:var(--hot); color:var(--void); }}
    .changes {{ display:flex; flex-wrap:wrap; gap:7px; }}
    .change {{ max-width:100%; display:grid; grid-template-columns:auto minmax(120px, max-content); grid-template-areas:"action target" "summary summary"; column-gap:8px; row-gap:3px; border:1px solid rgba(243,240,230,.36); padding:6px 8px; font-size:12px; }}
    .change b {{ grid-area:action; color:var(--muted); text-transform:uppercase; }}
    .change-target {{ grid-area:target; color:var(--ink); font-weight:700; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
    .change code {{ grid-area:summary; min-width:0; color:#c8c1ad; white-space:normal; word-break:break-word; }}
    .change.created b {{ color:var(--acid); }}
    .change.updated b {{ color:var(--blue); }}
    .change.removed b {{ color:var(--hot); }}
    .empty-row {{ padding:14px 12px; border-top:3px solid var(--line); color:var(--muted); font-size:13px; font-weight:700; text-transform:uppercase; }}
    .footer {{ display:flex; flex-wrap:wrap; align-items:center; gap:0; border-top:3px solid var(--line); background:var(--void); }}
    .button {{ display:inline-flex; align-items:center; justify-content:center; min-height:46px; padding:10px 14px; border-right:3px solid var(--line); background:var(--void); color:var(--ink); text-decoration:none; font-size:13px; font-weight:700; text-transform:uppercase; }}
    .button:hover {{ background:var(--paper); color:var(--void); }}
    @media (max-width: 900px) {{ body::after {{ display:none; }} .shell {{ box-shadow:7px 7px 0 var(--acid); }} .topbar {{ flex-direction:column; }} header {{ grid-template-columns:86px 1fr; }} .mark {{ min-height:96px; font-size:25px; }} .brand {{ padding:13px; }} h1 {{ font-size:42px; }} .status {{ grid-column:1 / -1; border-left:0; border-top:3px solid var(--line); min-height:54px; }} .grid {{ grid-template-columns:1fr 1fr; }} .metric {{ border-bottom:3px solid var(--line); }} .metric:nth-child(2n) {{ border-right:0; }} .metric:first-child .value, .value {{ font-size:24px; }} .content {{ grid-template-columns:1fr; }} .panel {{ border-right:0; border-bottom:3px solid var(--line); }} .terminal li {{ grid-template-columns:1fr; gap:4px; }} .section-head {{ display:grid; grid-template-columns:1fr; }} .sync-controls {{ margin-left:0; border-left:0; border-top:3px solid var(--void); }} .control {{ flex:1 1 auto; justify-content:center; }} .subgraph-row {{ grid-template-columns:1fr; }} .subgraph-cell {{ border-right:0; border-bottom:3px solid var(--line); }} .subgraph-cell:last-child {{ border-bottom:0; }} .sync-summary {{ grid-template-columns:1fr 1fr; }} .sync-summary span:nth-child(2n) {{ border-right:0; }} .sync-summary span {{ border-bottom:3px solid var(--line); }} .sync-row {{ grid-template-columns:1fr; }} .sync-block {{ border-right:0; border-bottom:3px solid var(--line); }} .footer {{ display:grid; grid-template-columns:1fr; }} .button {{ margin-left:0; border-right:0; border-left:0; border-bottom:3px solid var(--line); width:100%; justify-content:flex-start; }} }}
  </style>
</head>
<body>
  <main>
    <section class="shell" aria-label="UGraph service status">
      <div class="topbar">
        <span>self-hosted subgraph runtime // public status</span>
        <a class="maker" href="https://turinglabs.org" rel="noopener">made by turinglabs_</a>
      </div>
      <header>
        <div class="mark" aria-label="UGraph logo">UG</div>
        <div class="brand">
          <h1>UGraph</h1>
          <div class="subtitle">open subgraph runtime / {store}</div>
        </div>
        <div class="status {badge}">{status}</div>
      </header>
      <section class="grid" aria-label="Service metrics">
        <div class="metric"><div class="label">Block</div><div class="value">{to_block}</div></div>
        <div class="metric"><div class="label">Entities</div><div class="value">{entities}</div></div>
        <div class="metric"><div class="label">Sources</div><div class="value">{dynamic_sources}</div></div>
        <div class="metric"><div class="label">Changes</div><div class="value">{entity_changes}</div></div>
        <div class="metric"><div class="label">Errors</div><div class="value">{validation_errors}</div></div>
      </section>
      <section class="content">
        <div class="panel">
          <ul class="terminal" aria-label="Deployment terminal">
            <li><span class="key">$ name</span><code>{deployment}</code></li>
            <li><span class="key">$ version</span><code>{version}</code></li>
            <li><span class="key">$ visibility</span><code>{visibility}</code></li>
            <li><span class="key">$ endpoint</span><a href="{versioned_endpoint}">{versioned_endpoint}</a></li>
            <li><span class="key">$ latest</span><a href="{latest_endpoint}">{latest_endpoint}</a></li>
            <li><span class="key">$ block_hash</span><code class="hash">{block_hash}</code></li>
          </ul>
        </div>
        <div class="panel">
          <ul class="terminal" aria-label="Runtime terminal">
            <li><span class="key">$ runtime</span><span class="{health_class}">{health_text}</span></li>
            <li><span class="key">$ chain</span><code>{chain_id}</code></li>
            <li><span class="key">$ explorer</span>{explorer}</li>
            <li><span class="key">$ change_blocks</span><span>{change_blocks}</span></li>
            <li><span class="key">$ checkpoints</span><span>{indexed_checkpoints}</span></li>
            <li><span class="key">$ state_cache</span><span>{state_cache}</span></li>
            <li><span class="key">$ sync_page</span><span>{sync_page_label}</span></li>
            <li><span class="key">$ refresh</span><span>10 seconds</span></li>
          </ul>
        </div>
      </section>
      <section class="section" aria-label="Public subgraphs">
        <div class="section-head"><div class="section-title">PUBLIC SUBGRAPHS</div></div>
        {public_subgraphs}
      </section>
      <section class="section" aria-label="Entity changes">
        <div class="section-head"><div class="section-title">ENTITY CHANGES</div>{sync_controls}</div>
        {sync_summary}
        {sync_blocks}
      </section>
      <nav class="footer" aria-label="Service links">
        <a class="button" href="/graphql">GraphiQL</a>
        <a class="button" href="{latest_endpoint}">Latest endpoint</a>
        <a class="button" href="/healthz">Health JSON</a>
        <a class="button" href="/metrics">Metrics</a>
      </nav>
    </section>
  </main>
  <script>
    document.querySelectorAll('time[data-timestamp]').forEach((node) => {{
      const timestamp = Number(node.getAttribute('data-timestamp'));
      if (Number.isFinite(timestamp)) {{
        node.textContent = new Date(timestamp * 1000).toISOString().replace('T', ' ').replace('.000Z', ' UTC');
      }}
    }});
  </script>
</body>
</html>"#,
        store = html_escape(&runtime_store.label()),
        badge = badge,
        status = display_status,
        deployment = html_escape(deployment),
        version = html_escape(version),
        visibility = html_escape(visibility),
        versioned_endpoint = html_escape(&versioned_endpoint),
        latest_endpoint = html_escape(&latest_endpoint),
        to_block = to_block,
        block_hash = html_escape(block_hash),
        entities = entities,
        dynamic_sources = dynamic_sources,
        entity_changes = entity_changes,
        validation_errors = validation_errors,
        public_subgraphs = public_subgraphs,
        sync_controls = sync_controls,
        sync_summary = sync_summary_html(sync_activity),
        sync_blocks = sync_blocks,
        chain_id = html_escape(&chain_id),
        explorer = explorer,
        change_blocks = html_escape(&change_blocks),
        indexed_checkpoints = html_escape(&indexed_checkpoints),
        state_cache = html_escape(&state_cache),
        sync_page_label = html_escape(&sync_page_label),
        health_class = if ok { "ok-text" } else { "warn-text" },
        health_text = if ok {
            "sync complete"
        } else {
            "attention required"
        }
    )
}

fn public_subgraphs_html(rows: &[storage::DeploymentVersionRecord]) -> String {
    if rows.is_empty() {
        return r#"<div class="empty-row">no public subgraphs</div>"#.to_string();
    }
    rows.iter()
        .map(|version| {
            let endpoint = format!(
                "/subgraphs/{}/{}/gn",
                version.deployment, version.version_label
            );
            let deployed_by = version
                .owner_email
                .clone()
                .or_else(|| {
                    version
                        .created_by_key_prefix
                        .as_ref()
                        .map(|prefix| format!("key:{prefix}"))
                })
                .unwrap_or_else(|| "unassigned".to_string());
            format!(
                concat!(
                    r#"<article class="subgraph-row">"#,
                    r#"<div class="subgraph-cell"><span class="subgraph-label">name</span><div class="subgraph-name">{name}</div></div>"#,
                    r#"<div class="subgraph-cell"><span class="subgraph-label">version</span><code>{version}</code></div>"#,
                    r#"<div class="subgraph-cell"><span class="subgraph-label">deployed by</span><code>{deployed_by}</code></div>"#,
                    r#"<div class="subgraph-cell"><span class="subgraph-label">endpoint</span><a href="{endpoint}">{endpoint}</a></div>"#,
                    r#"</article>"#
                ),
                name = html_escape(&version.deployment),
                version = html_escape(&version.version_label),
                deployed_by = html_escape(&deployed_by),
                endpoint = html_escape(&endpoint)
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn sync_summary_html(page: Option<&storage::SyncActivityPage>) -> String {
    let stats = page.map(|page| page.stats.clone()).unwrap_or_default();
    format!(
        concat!(
            r#"<div class="sync-summary" aria-label="Entity change summary">"#,
            r#"<span>change blocks <b>{change_blocks}</b></span>"#,
            r#"<span>entity changes <b>{entity_changes}</b></span>"#,
            r#"<span>created <b>{created}</b></span>"#,
            r#"<span>updated <b>{updated}</b></span>"#,
            r#"<span>removed <b>{removed}</b></span>"#,
            r#"<span>checkpoints <b>{indexed_checkpoints}</b></span>"#,
            r#"</div>"#
        ),
        change_blocks = stats.change_blocks,
        entity_changes = stats.entity_changes,
        created = stats.created,
        updated = stats.updated,
        removed = stats.removed,
        indexed_checkpoints = stats.indexed_checkpoints
    )
}

fn sync_controls_html(
    page: Option<&storage::SyncActivityPage>,
    requested: &SyncActivityOptions,
) -> String {
    let page_number = page.map(|page| page.page).unwrap_or(requested.page);
    let limit = page.map(|page| page.limit).unwrap_or(requested.limit);
    let show_empty = page
        .map(|page| page.show_empty)
        .unwrap_or(requested.show_empty);
    let previous = page
        .filter(|page| page.has_previous)
        .map(|_| page_number.saturating_sub(1));
    let next = page.filter(|page| page.has_next).map(|_| page_number + 1);
    let mode_url = sync_activity_url(1, limit, !show_empty);
    let mode_label = if show_empty {
        "changes only"
    } else {
        "show checkpoints"
    };
    let previous_html = previous
        .map(|page| {
            format!(
                r#"<a class="control" href="{url}">prev</a>"#,
                url = html_escape(&sync_activity_url(page, limit, show_empty))
            )
        })
        .unwrap_or_else(|| r#"<span class="control disabled">prev</span>"#.to_string());
    let next_html = next
        .map(|page| {
            format!(
                r#"<a class="control" href="{url}">next</a>"#,
                url = html_escape(&sync_activity_url(page, limit, show_empty))
            )
        })
        .unwrap_or_else(|| r#"<span class="control disabled">next</span>"#.to_string());
    format!(
        concat!(
            r#"<div class="sync-controls" aria-label="Sync log controls">"#,
            "{previous}",
            r#"<span class="control disabled">page {page}</span>"#,
            "{next}",
            r#"<a class="control" href="{mode_url}">{mode_label}</a>"#,
            r#"</div>"#
        ),
        previous = previous_html,
        page = page_number,
        next = next_html,
        mode_url = html_escape(&mode_url),
        mode_label = mode_label
    )
}

fn sync_activity_url(page: usize, limit: usize, show_empty: bool) -> String {
    format!(
        "/status?sync_page={page}&sync_limit={limit}&show_empty={}",
        usize::from(show_empty)
    )
}

fn sync_blocks_html(page: Option<&storage::SyncActivityPage>, options: &ServeOptions) -> String {
    let Some(page) = page else {
        return r#"<div class="empty-row">entity change activity unavailable</div>"#.to_string();
    };
    if page.activities.is_empty() {
        return if page.show_empty {
            r#"<div class="empty-row">no sync checkpoints recorded</div>"#.to_string()
        } else {
            r#"<div class="empty-row">no entity changes recorded</div>"#.to_string()
        };
    }
    page.activities
        .iter()
        .map(|activity| {
            let total_changes = activity.created + activity.updated + activity.removed;
            let changes = if activity.changes.is_empty() {
                r#"<span class="change"><b>idle</b><code>no entity changes</code></span>"#
                    .to_string()
            } else {
                let mut rows = activity
                    .changes
                    .iter()
                    .map(change_activity_html)
                    .collect::<Vec<_>>()
                    .join("");
                if total_changes > activity.changes.len() {
                    rows.push_str(&format!(
                        r#"<span class="change"><b>more</b><code>{} hidden</code></span>"#,
                        total_changes - activity.changes.len()
                    ));
                }
                rows
            };
            let block = block_link_html(activity.block_number, options);
            let explorer = block_explorer_url(options, activity.block_number)
                .map(|url| {
                    format!(
                        r#"<a href="{url}" rel="noopener">explorer</a>"#,
                        url = html_escape(&url)
                    )
                })
                .unwrap_or_else(|| "-".to_string());
            let block_hash = activity
                .block_hash
                .as_deref()
                .map(short_hash)
                .unwrap_or_else(|| "-".to_string());
            let emitted = block_timestamp_html(activity.block_timestamp);
            format!(
                concat!(
                    r#"<article class="sync-row">"#,
                    r#"<div class="sync-block">"#,
                    r#"<div>{block}</div>"#,
                    r#"<div class="block-meta">emitted {emitted}</div>"#,
                    r#"<div class="block-meta">hash <span class="block-hash">{block_hash}</span></div>"#,
                    r#"<div class="block-meta">{explorer}</div>"#,
                    r#"</div>"#,
                    r#"<div class="sync-detail">"#,
                    r#"<div class="sync-counts">"#,
                    r#"<span class="sync-count created">created {created}</span>"#,
                    r#"<span class="sync-count updated">updated {updated}</span>"#,
                    r#"<span class="sync-count removed">removed {removed}</span>"#,
                    r#"</div>"#,
                    r#"<div class="changes">{changes}</div>"#,
                    r#"</div>"#,
                    r#"</article>"#
                ),
                block = block,
                emitted = emitted,
                block_hash = html_escape(&block_hash),
                explorer = explorer,
                created = activity.created,
                updated = activity.updated,
                removed = activity.removed,
                changes = changes
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn change_activity_html(change: &storage::EntityChangeRecord) -> String {
    let action = change.action.as_str();
    let full_target = format!("{}:{}", change.entity, change.id);
    let target = format!("{} {}", change.entity, short_identifier(&change.id));
    let summary = change_summary(change);
    format!(
        concat!(
            r#"<span class="change {action}" title="{title}">"#,
            r#"<b>{action}</b>"#,
            r#"<span class="change-target">{target}</span>"#,
            r#"<code>{summary}</code>"#,
            r#"</span>"#
        ),
        action = html_escape(action),
        title = html_escape(&full_target),
        target = html_escape(&target),
        summary = html_escape(&summary)
    )
}

fn change_summary(change: &storage::EntityChangeRecord) -> String {
    match change.action {
        storage::EntityChangeAction::Created => {
            format!("created: {}", field_summary(&change.data, 4))
        }
        storage::EntityChangeAction::Updated => {
            if let Some(previous_data) = change.previous_data.as_ref() {
                let summary = diff_summary(previous_data, &change.data, 4);
                if summary.is_empty() {
                    "no field-level change".to_string()
                } else {
                    summary
                }
            } else {
                format!("updated: {}", field_summary(&change.data, 4))
            }
        }
        storage::EntityChangeAction::Removed => {
            let data = change.previous_data.as_ref().unwrap_or(&change.data);
            format!("removed: {}", field_summary(data, 4))
        }
    }
}

fn diff_summary(previous: &EntityData, current: &EntityData, limit: usize) -> String {
    let mut keys = previous
        .keys()
        .chain(current.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| previous.get(key) != current.get(key))
        .collect::<Vec<_>>();
    sort_fields_by_readability(&mut keys);
    let hidden = keys.len().saturating_sub(limit);
    let mut fields = keys
        .into_iter()
        .take(limit)
        .map(|key| {
            let before = previous
                .get(&key)
                .map(format_store_value)
                .unwrap_or_else(|| "null".to_string());
            let after = current
                .get(&key)
                .map(format_store_value)
                .unwrap_or_else(|| "null".to_string());
            format!("{key}: {before} -> {after}")
        })
        .collect::<Vec<_>>();
    if hidden > 0 {
        fields.push(format!("+{hidden} fields"));
    }
    fields.join("; ")
}

fn field_summary(data: &EntityData, limit: usize) -> String {
    if data.is_empty() {
        return "no fields".to_string();
    }
    let mut keys = data.keys().cloned().collect::<Vec<_>>();
    sort_fields_by_readability(&mut keys);
    let hidden = keys.len().saturating_sub(limit);
    let mut fields = keys
        .into_iter()
        .take(limit)
        .filter_map(|key| {
            data.get(&key)
                .map(|value| format!("{key}={}", format_store_value(value)))
        })
        .collect::<Vec<_>>();
    if hidden > 0 {
        fields.push(format!("+{hidden} fields"));
    }
    fields.join(", ")
}

fn sort_fields_by_readability(fields: &mut [String]) {
    fields.sort_by(|left, right| {
        field_priority(left)
            .cmp(&field_priority(right))
            .then_with(|| left.cmp(right))
    });
}

fn field_priority(field: &str) -> usize {
    const PRIORITY: [&str; 33] = [
        "name",
        "symbol",
        "status",
        "state",
        "user",
        "owner",
        "account",
        "holder",
        "buyer",
        "seller",
        "creator",
        "recipient",
        "campaign",
        "pool",
        "market",
        "token",
        "asset",
        "amount",
        "value",
        "balance",
        "total",
        "price",
        "rate",
        "shares",
        "count",
        "timestamp",
        "createdAt",
        "updatedAt",
        "txHash",
        "transactionHash",
        "hash",
        "address",
        "id",
    ];
    let normalized = field.to_ascii_lowercase();
    PRIORITY
        .iter()
        .position(|candidate| candidate.eq_ignore_ascii_case(field))
        .or_else(|| {
            PRIORITY.iter().position(|candidate| {
                *candidate != "id" && normalized.contains(&candidate.to_ascii_lowercase())
            })
        })
        .unwrap_or(PRIORITY.len())
}

fn format_store_value(value: &StoreValue) -> String {
    match value {
        StoreValue::String(value) => short_string_value(value),
        StoreValue::Bytes(value) => short_identifier(value),
        StoreValue::Int(value) => value.to_string(),
        StoreValue::BigDecimal { digits, exp } => {
            let exponent = exp.parse::<i32>().unwrap_or_default();
            if exponent == 0 {
                compact_integer(digits)
            } else {
                format!("{}e{}", compact_integer(digits), exponent)
            }
        }
        StoreValue::Bool(value) => value.to_string(),
        StoreValue::Array(values) => {
            let visible = values
                .iter()
                .take(3)
                .map(format_store_value)
                .collect::<Vec<_>>();
            if values.len() > visible.len() {
                format!(
                    "[{}, +{}]",
                    visible.join(", "),
                    values.len() - visible.len()
                )
            } else {
                format!("[{}]", visible.join(", "))
            }
        }
        StoreValue::Null => "null".to_string(),
        StoreValue::BigInt(value) => compact_integer(value),
        StoreValue::Int8(value) | StoreValue::Timestamp(value) => value.to_string(),
    }
}

fn short_string_value(value: &str) -> String {
    if value.starts_with("0x") {
        return short_identifier(value);
    }
    if value.len() <= 48 {
        return value.to_string();
    }
    format!("{}...", value.chars().take(45).collect::<String>())
}

fn short_identifier(value: &str) -> String {
    if value.len() <= 24 {
        return value.to_string();
    }
    if value.starts_with("0x") && value.len() > 18 {
        return format!("{}...{}", &value[..10], &value[value.len() - 6..]);
    }
    format!("{}...{}", &value[..14], &value[value.len() - 6..])
}

fn compact_integer(value: &str) -> String {
    let trimmed = value.trim();
    let negative = trimmed.starts_with('-');
    let digits = trimmed.trim_start_matches('-').trim_start_matches('0');
    if digits.is_empty() {
        return "0".to_string();
    }
    if digits.len() <= 18 {
        let mut grouped = String::new();
        for (index, ch) in digits.chars().rev().enumerate() {
            if index > 0 && index % 3 == 0 {
                grouped.push(',');
            }
            grouped.push(ch);
        }
        let grouped = grouped.chars().rev().collect::<String>();
        return if negative {
            format!("-{grouped}")
        } else {
            grouped
        };
    }
    let mut chars = digits.chars();
    let first = chars.next().unwrap_or('0');
    let decimals = chars.take(3).collect::<String>();
    let prefix = if decimals.is_empty() {
        first.to_string()
    } else {
        format!("{first}.{decimals}")
    };
    let sign = if negative { "-" } else { "" };
    format!("{sign}{prefix}e{}", digits.len() - 1)
}

fn block_link_html(block: u64, options: &ServeOptions) -> String {
    block_explorer_url(options, block)
        .map(|url| {
            format!(
                r#"<a href="{url}" rel="noopener">#{block}</a>"#,
                url = html_escape(&url)
            )
        })
        .unwrap_or_else(|| format!("#{block}"))
}

fn block_timestamp_html(timestamp: Option<u64>) -> String {
    timestamp
        .map(|timestamp| {
            format!(
                r#"<time data-timestamp="{timestamp}">{timestamp}</time>"#,
                timestamp = timestamp
            )
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn short_hash(value: &str) -> String {
    if value.len() <= 18 {
        return value.to_string();
    }
    format!("{}...{}", &value[..10], &value[value.len() - 6..])
}

fn explorer_status_html(options: &ServeOptions) -> String {
    explorer_home_url(options)
        .map(|url| {
            format!(
                r#"<a href="{url}" rel="noopener">{label}</a>"#,
                url = html_escape(&url),
                label = html_escape(&explorer_label(&url))
            )
        })
        .unwrap_or_else(|| "<span>-</span>".to_string())
}

fn explorer_label(url: &str) -> String {
    url.trim()
        .strip_prefix("https://")
        .or_else(|| url.trim().strip_prefix("http://"))
        .unwrap_or(url.trim())
        .trim_end_matches('/')
        .to_string()
}

fn explorer_home_url(options: &ServeOptions) -> Option<String> {
    let template = block_explorer_template(options)?;
    if let Some((root, _)) = template.split_once("/block/{block}") {
        return Some(root.to_string());
    }
    if let Some((root, _)) = template.split_once("{block}") {
        return Some(root.trim_end_matches('/').to_string());
    }
    Some(template.trim_end_matches('/').to_string())
}

fn block_explorer_url(options: &ServeOptions, block: u64) -> Option<String> {
    let template = block_explorer_template(options)?;
    if template.contains("{block}") {
        Some(template.replace("{block}", &block.to_string()))
    } else {
        Some(format!("{}/{}", template.trim_end_matches('/'), block))
    }
}

fn block_explorer_template(options: &ServeOptions) -> Option<String> {
    if let Some(configured) = options
        .block_explorer_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(configured.to_string());
    }
    let template = match options.chain_id? {
        1 => "https://etherscan.io/block/{block}",
        10 => "https://optimistic.etherscan.io/block/{block}",
        56 => "https://bscscan.com/block/{block}",
        100 => "https://gnosisscan.io/block/{block}",
        137 => "https://polygonscan.com/block/{block}",
        8453 => "https://basescan.org/block/{block}",
        84532 => "https://sepolia.basescan.org/block/{block}",
        42161 => "https://arbiscan.io/block/{block}",
        43114 => "https://snowtrace.io/block/{block}",
        11155111 => "https://sepolia.etherscan.io/block/{block}",
        _ => return None,
    };
    Some(template.to_string())
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn parse_query_params(query: &str) -> BTreeMap<String, String> {
    query
        .split('&')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((percent_decode(key), percent_decode(value)))
        })
        .collect()
}

fn percent_decode(input: &str) -> String {
    let mut output = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                output.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let hex = &input[index + 1..index + 3];
                if let Ok(value) = u8::from_str_radix(hex, 16) {
                    output.push(value);
                    index += 3;
                } else {
                    output.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn graphiql_html(endpoint: &str) -> String {
    const HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>UGraph GraphiQL</title>
    <link rel="stylesheet" href="https://unpkg.com/graphiql@2.4.7/graphiql.min.css" />
    <style>
      html, body, #graphiql { height: 100%; margin: 0; }
      body { font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
      .fallback {
        display: grid;
        grid-template-rows: auto 1fr;
        gap: 12px;
        height: 100%;
        box-sizing: border-box;
        padding: 16px;
        background: #f7f7f8;
        color: #111827;
      }
      .fallback-bar {
        display: flex;
        align-items: center;
        gap: 12px;
      }
      .fallback-title {
        font-size: 15px;
        font-weight: 650;
      }
      .fallback-note {
        color: #6b7280;
        font-size: 13px;
      }
      .fallback-grid {
        display: grid;
        grid-template-columns: minmax(280px, 1fr) minmax(280px, 1fr);
        gap: 12px;
        min-height: 0;
      }
      textarea, pre {
        width: 100%;
        height: 100%;
        box-sizing: border-box;
        margin: 0;
        border: 1px solid #d1d5db;
        border-radius: 6px;
        padding: 12px;
        background: #ffffff;
        color: #111827;
        font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        overflow: auto;
      }
      button {
        border: 1px solid #111827;
        border-radius: 6px;
        background: #111827;
        color: white;
        padding: 8px 12px;
        font: inherit;
        cursor: pointer;
      }
      @media (max-width: 760px) {
        .fallback-grid { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <div id="graphiql">Loading GraphiQL...</div>
    <script crossorigin src="https://unpkg.com/react@18.2.0/umd/react.production.min.js"></script>
    <script crossorigin src="https://unpkg.com/react-dom@18.2.0/umd/react-dom.production.min.js"></script>
    <script crossorigin src="https://unpkg.com/graphiql@2.4.7/graphiql.min.js"></script>
    <script>
      const endpoint = __UGRAPH_ENDPOINT_JSON__;
      const defaultQuery = '{\n  _meta { block { number hash } hasIndexingErrors }\n}';
      const memoryStorage = {
        getItem: () => null,
        setItem: () => undefined,
        removeItem: () => undefined,
        clear: () => undefined,
        key: () => null,
        get length() { return 0; }
      };

      function resetBrokenCachedQuery() {
        try {
          const storage = window.localStorage;
          if (!storage) return;
          const cached = storage.getItem('graphiql:query');
          const trimmed = cached && cached.trim();
          const looksLikeGraphQL = trimmed && /^(query|mutation|subscription|fragment|\{)/.test(trimmed);
          if (trimmed && !looksLikeGraphQL) {
            storage.setItem('graphiql:query', defaultQuery);
          }
        } catch (_) {}
      }

      async function graphQLFetcher(graphQLParams) {
        const headers = { 'content-type': 'application/json' };
        const apiKey = window.localStorage && window.localStorage.getItem('ugraph_api_key');
        if (apiKey) {
          headers.authorization = 'Bearer ' + apiKey;
        }
        const response = await fetch(endpoint, {
          method: 'post',
          headers,
          body: JSON.stringify(graphQLParams)
        });
        const body = await response.text();
        if (!body) {
          return { errors: [{ message: 'UGraph returned an empty response.' }] };
        }
        try {
          return JSON.parse(body);
        } catch (error) {
          return { errors: [{ message: String(error && error.message ? error.message : error) }] };
        }
      }

      function mountFallback(reason) {
        const root = document.getElementById('graphiql');
        root.innerHTML =
          '<main class="fallback">' +
            '<div class="fallback-bar">' +
              '<button id="run-query" type="button">Run</button>' +
              '<div><div class="fallback-title">UGraph GraphQL</div>' +
              '<div class="fallback-note">' + reason + '</div></div>' +
            '</div>' +
            '<div class="fallback-grid">' +
              '<textarea id="query-input" spellcheck="false"></textarea>' +
              '<pre id="query-output"></pre>' +
            '</div>' +
          '</main>';
        const input = document.getElementById('query-input');
        const output = document.getElementById('query-output');
        input.value = defaultQuery;
        async function runQuery() {
          output.textContent = 'Loading...';
          try {
            const json = await graphQLFetcher({ query: input.value });
            output.textContent = JSON.stringify(json, null, 2);
          } catch (error) {
            output.textContent = String(error && error.message ? error.message : error);
          }
        }
        document.getElementById('run-query').addEventListener('click', runQuery);
        runQuery();
      }

      try {
        if (!window.React || !window.ReactDOM || !window.GraphiQL) {
          mountFallback('GraphiQL assets did not load; using built-in fallback.');
        } else {
          resetBrokenCachedQuery();
          const element = React.createElement(GraphiQL, {
            fetcher: graphQLFetcher,
            defaultQuery,
            storage: memoryStorage
          });
          if (ReactDOM.createRoot) {
            ReactDOM.createRoot(document.getElementById('graphiql')).render(element);
          } else {
            ReactDOM.render(element, document.getElementById('graphiql'));
          }
        }
      } catch (error) {
        mountFallback('GraphiQL failed to start; using built-in fallback.');
      }
    </script>
  </body>
</html>"#;
    HTML.replace(
        "__UGRAPH_ENDPOINT_JSON__",
        &serde_json::to_string(endpoint).unwrap_or_else(|_| "\"/graphql\"".to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SyncCheckpoint;

    #[test]
    fn metrics_include_checkpoint_and_store_health() {
        let store = SnapshotStore::Postgres {
            url: "postgres://example".to_string(),
            deployment: "growfi".to_string(),
        };
        let status = StoreStatus {
            checkpoint: SyncCheckpoint {
                from_block: Some(10),
                to_block: 42,
                block_hash: Some("0xabc".to_string()),
                block_timestamp: None,
                scanned_logs: 3,
                executed_logs: 2,
                validation_errors: 1,
                complete: true,
            },
            entities: 0,
            dynamic_sources: 0,
            history_snapshots: 0,
            history_earliest_block: None,
            history_latest_block: None,
        };

        let metrics = metrics_text(&store, &status);

        assert!(metrics.contains(r#"ugraph_store_up{store="postgres:growfi"} 1"#));
        assert!(metrics.contains(r#"ugraph_checkpoint_to_block{store="postgres:growfi"} 42"#));
        assert!(metrics.contains(r#"ugraph_checkpoint_complete{store="postgres:growfi"} 1"#));
        assert!(metrics.contains(r#"ugraph_history_snapshots{store="postgres:growfi"} 0"#));
        assert!(metrics.contains(r#"ugraph_history_earliest_block{store="postgres:growfi"} 0"#));
        assert!(metrics.contains(r#"ugraph_history_latest_block{store="postgres:growfi"} 0"#));
        assert!(metrics.contains(r#"ugraph_validation_errors{store="postgres:growfi"} 1"#));
    }

    #[test]
    fn prometheus_labels_are_escaped() {
        assert_eq!(
            prometheus_label_value("a\"b\\c\nd"),
            r#"a\"b\\c\nd"#.to_string()
        );
    }

    #[test]
    fn client_disconnect_errors_are_ignored() {
        let broken_pipe =
            anyhow::Error::new(std::io::Error::from(ErrorKind::BrokenPipe)).context("write");
        let connection_reset =
            anyhow::Error::new(std::io::Error::from(ErrorKind::ConnectionReset)).context("write");
        let other = anyhow::anyhow!("database unavailable");

        assert!(is_client_disconnect(&broken_pipe));
        assert!(is_client_disconnect(&connection_reset));
        assert!(!is_client_disconnect(&other));
    }

    #[test]
    fn graphiql_html_uses_pinned_assets_and_fallback() {
        let html = graphiql_html("/subgraphs/growfi/latest/gn");

        assert!(html.contains("react@18.2.0"));
        assert!(html.contains("graphiql@2.4.7"));
        assert!(html.contains("GraphiQL assets did not load"));
        assert!(html.contains("graphQLFetcher"));
        assert!(html.contains("ugraph_api_key"));
        assert!(html.contains("memoryStorage"));
        assert!(html.contains("resetBrokenCachedQuery"));
        assert!(html.contains(r#"const endpoint = "/subgraphs/growfi/latest/gn";"#));
    }

    #[test]
    fn graphql_endpoint_accepts_hosted_provider_paths() {
        assert_eq!(
            graphql_endpoint("/graphql"),
            Some(GraphqlEndpoint {
                path: "/graphql",
                deployment: None,
                version: None
            })
        );
        assert_eq!(
            graphql_endpoint("/subgraphs/growfi/4.0.2/gn"),
            Some(GraphqlEndpoint {
                path: "/subgraphs/growfi/4.0.2/gn",
                deployment: Some("growfi"),
                version: Some("4.0.2")
            })
        );
        assert_eq!(
            graphql_endpoint("/subgraphs/growfi/latest/graphql"),
            Some(GraphqlEndpoint {
                path: "/subgraphs/growfi/latest/graphql",
                deployment: Some("growfi"),
                version: Some("latest")
            })
        );
        assert_eq!(graphql_endpoint("/subgraphs/growfi/4.0.2"), None);
        assert_eq!(graphql_endpoint("/subgraph/growfi/4.0.2/gn"), None);
    }

    #[test]
    fn home_html_renders_terminal_status_and_versioned_endpoint() {
        let store = SnapshotStore::Postgres {
            url: "postgres://example".to_string(),
            deployment: "growfi".to_string(),
        };
        let status = StoreStatus {
            checkpoint: SyncCheckpoint {
                from_block: Some(10),
                to_block: 42,
                block_hash: Some("0xabc".to_string()),
                block_timestamp: None,
                scanned_logs: 3,
                executed_logs: 2,
                validation_errors: 0,
                complete: true,
            },
            entities: 0,
            dynamic_sources: 0,
            history_snapshots: 0,
            history_earliest_block: None,
            history_latest_block: None,
        };
        let metadata = storage::DeploymentMetadataRecord {
            deployment: "growfi".to_string(),
            version_label: Some("4.0.2".to_string()),
            visibility: "public".to_string(),
            owner_user_id: None,
            owner_email: Some("ops@ugraph.local".to_string()),
            created_by_key_id: None,
            created_by_key_prefix: None,
            created_at: "now".to_string(),
            updated_at: "now".to_string(),
        };
        let version = storage::DeploymentVersionRecord {
            deployment: "growfi".to_string(),
            version_label: "4.0.2".to_string(),
            storage_deployment: "growfi@4.0.2".to_string(),
            visibility: "public".to_string(),
            owner_user_id: None,
            owner_email: Some("ops@ugraph.local".to_string()),
            created_by_key_id: None,
            created_by_key_prefix: None,
            promoted_at: Some("now".to_string()),
            created_at: "now".to_string(),
            updated_at: "now".to_string(),
        };
        let sync_activity = storage::SyncActivityPage {
            activities: vec![storage::SyncBlockActivity {
                block_number: 42,
                block_hash: Some("0xabc123456789def".to_string()),
                block_timestamp: Some(1_700_000_000),
                created: 1,
                updated: 2,
                removed: 0,
                changes: vec![storage::EntityChangeRecord {
                    entity: "Token".to_string(),
                    id: "0xabc".to_string(),
                    action: storage::EntityChangeAction::Updated,
                    previous_data: Some(BTreeMap::from([(
                        "amount".to_string(),
                        StoreValue::BigInt("1000000000000000000".to_string()),
                    )])),
                    data: BTreeMap::from([(
                        "amount".to_string(),
                        StoreValue::BigInt("2000000000000000000".to_string()),
                    )]),
                }],
            }],
            stats: storage::SyncActivityStats {
                change_blocks: 1,
                entity_changes: 3,
                indexed_checkpoints: 7,
                created: 1,
                updated: 2,
                removed: 0,
            },
            page: 1,
            limit: 8,
            has_previous: false,
            has_next: true,
            show_empty: false,
        };
        let sync_options = SyncActivityOptions {
            page: 1,
            limit: 8,
            show_empty: false,
        };
        let serve_options = ServeOptions {
            chain_id: Some(11155111),
            block_explorer_url: None,
        };

        let html = home_html(HomeHtmlInput {
            store: &store,
            runtime_store: &store,
            status: Some(&status),
            metadata: Some(&metadata),
            public_deployments: std::slice::from_ref(&version),
            sync_activity: Some(&sync_activity),
            sync_options: &sync_options,
            options: &serve_options,
        });

        assert!(html.contains("OPERATIONAL"));
        assert!(html.contains("/subgraphs/growfi/4.0.2/gn"));
        assert!(html.contains("/subgraphs/growfi/latest/gn"));
        assert!(html.contains("PUBLIC SUBGRAPHS"));
        assert!(html.contains("ENTITY CHANGES"));
        assert!(html.contains("amount: 1.000e18 -&gt; 2.000e18"));
        assert!(html.contains("ops@ugraph.local"));
        assert!(html.contains("updated"));
        assert!(html.contains("Token:0xabc"));
        assert!(html.contains("sepolia.etherscan.io/block/42"));
        assert!(html.contains("data-timestamp=\"1700000000\""));
        assert!(html.contains("show checkpoints"));
        assert!(html.contains("page 1 / entity changes"));
        assert!(html.contains("entity changes <b>3</b>"));
        assert!(html.contains("$ state_cache"));
        assert!(html.contains("$ chain"));
        assert!(!html.contains(">History<"));
        assert!(!html.contains("$ query"));
        assert!(!html.contains("$ api"));
        assert!(!html.contains("versioned endpoint active"));
        assert!(!html.contains("public HTTP interface"));
        assert!(!html.contains(">Range<"));
        assert!(!html.contains("goldsky"));
        assert!(!html.contains("graph_node"));
        assert_eq!(html.matches("made by turinglabs_").count(), 1);
        assert!(html.contains("made by turinglabs_"));
        assert!(html.contains("https://turinglabs.org"));
    }

    #[test]
    fn request_api_key_accepts_bearer_or_x_api_key() {
        let bearer_headers = parse_headers(["Authorization: Bearer ugraph_secret"].into_iter());
        assert_eq!(request_api_key(&bearer_headers), Some("ugraph_secret"));

        let direct_headers = parse_headers(["x-api-key: direct_secret"].into_iter());
        assert_eq!(request_api_key(&direct_headers), Some("direct_secret"));
    }

    #[test]
    fn remote_deploy_paths_reject_escape_attempts() {
        assert!(safe_relative_path("../subgraph.yaml").is_err());
        assert!(safe_relative_path("/tmp/subgraph.yaml").is_err());
        assert_eq!(
            safe_relative_path("./build/subgraph.yaml").unwrap(),
            PathBuf::from("build/subgraph.yaml")
        );
        assert!(safe_name_component("growfi@4.0.4").is_ok());
        assert!(safe_name_component("growfi/4.0.4").is_err());
    }
}
