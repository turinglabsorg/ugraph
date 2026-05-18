use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

use anyhow::Context;
use serde_json::json;

use crate::{
    query::{execute_graphql_with_context, query_needs_history, GraphqlHttpRequest},
    state::StoreSnapshot,
    storage::{self, SnapshotStore, StoreStatus},
};

const MAX_HTTP_REQUEST_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct GraphqlEndpoint<'a> {
    path: &'a str,
    deployment: Option<&'a str>,
    version: Option<&'a str>,
}

pub fn serve_store(store: SnapshotStore, bind: &str, once: bool) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind).with_context(|| format!("binding {bind}"))?;
    println!("UGraph status: http://{bind}/");
    println!("GraphiQL: http://{bind}/graphql");
    for stream in listener.incoming() {
        let stream = stream.with_context(|| format!("accepting connection on {bind}"))?;
        if once {
            log_connection_error(handle_store_connection(stream, &store), &store);
            break;
        }
        let store = store.clone();
        thread::spawn(move || {
            log_connection_error(handle_store_connection(stream, &store), &store);
        });
    }
    Ok(())
}

fn log_connection_error(result: anyhow::Result<()>, store: &SnapshotStore) {
    if let Err(error) = result {
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

fn handle_store_connection(mut stream: TcpStream, store: &SnapshotStore) -> anyhow::Result<()> {
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
        let (status, store_status) = match store.status() {
            Ok(store_status) => ("200 OK", Some(store_status)),
            Err(_) => ("503 Service Unavailable", None),
        };
        let metadata = deployment_metadata_for_store(store).ok().flatten();
        let public_deployments = public_deployments_for_store(store).unwrap_or_default();
        let sync_activity = sync_activity_for_store(store).unwrap_or_default();
        return write_response(
            &mut stream,
            status,
            "text/html; charset=utf-8",
            home_html(
                store,
                store_status.as_ref(),
                metadata.as_ref(),
                &public_deployments,
                &sync_activity,
            )
            .as_bytes(),
        );
    }
    if method == "GET" && path == "/healthz" {
        return write_healthz(&mut stream, store);
    }
    if method == "GET" && path == "/metrics" {
        return write_metrics(&mut stream, store);
    }
    if let Some(endpoint) = graphql_endpoint(path) {
        match graphql_endpoint_allowed(store, endpoint) {
            Ok(true) => {}
            Ok(false) => {
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
        }
        return match method {
            "GET" => handle_graphql_get(&mut stream, store, &headers, endpoint, query_string),
            "POST" => handle_graphql_post(&mut stream, store, &headers, body),
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

fn graphql_endpoint_allowed(
    store: &SnapshotStore,
    endpoint: GraphqlEndpoint<'_>,
) -> anyhow::Result<bool> {
    let Some(requested_deployment) = endpoint.deployment else {
        return Ok(true);
    };
    if requested_deployment != deployment_name(store) {
        return Ok(false);
    }
    let Some(requested_version) = endpoint.version else {
        return Ok(true);
    };
    if requested_version == "latest" {
        return Ok(true);
    }
    Ok(deployment_metadata_for_store(store)?
        .and_then(|metadata| metadata.version_label)
        .as_deref()
        == Some(requested_version))
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

fn handle_graphql_get(
    stream: &mut TcpStream,
    store: &SnapshotStore,
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
                        Some(deployment_name(store)),
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
                Some(deployment_name(store)),
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

fn public_deployments_for_store(
    store: &SnapshotStore,
) -> anyhow::Result<Vec<storage::DeploymentMetadataRecord>> {
    match store {
        SnapshotStore::Postgres { url, .. } => Ok(storage::list_deployment_metadata(url)?
            .into_iter()
            .filter(|metadata| metadata.visibility == "public")
            .collect()),
        SnapshotStore::Json { .. } => Ok(Vec::new()),
    }
}

fn sync_activity_for_store(
    store: &SnapshotStore,
) -> anyhow::Result<Vec<storage::SyncBlockActivity>> {
    match store {
        SnapshotStore::Postgres { url, deployment } => {
            storage::recent_sync_activity(url, deployment, 8, 8)
        }
        SnapshotStore::Json { .. } => Ok(Vec::new()),
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

fn home_html(
    store: &SnapshotStore,
    status: Option<&StoreStatus>,
    metadata: Option<&storage::DeploymentMetadataRecord>,
    public_deployments: &[storage::DeploymentMetadataRecord],
    sync_activity: &[storage::SyncBlockActivity],
) -> String {
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
    let history = status
        .map(|status| status.history_snapshots.to_string())
        .unwrap_or_else(|| "-".to_string());
    let validation_errors = status
        .map(|status| status.checkpoint.validation_errors.to_string())
        .unwrap_or_else(|| "-".to_string());
    let public_subgraphs = public_subgraphs_html(public_deployments);
    let sync_blocks = sync_blocks_html(sync_activity);
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
    .section-title {{ padding:8px 12px; background:var(--paper); color:var(--void); font-size:12px; font-weight:700; text-transform:uppercase; }}
    .subgraph-row {{ display:grid; grid-template-columns:1.1fr .7fr 1fr 1.7fr; gap:0; border-top:3px solid var(--line); }}
    .subgraph-cell {{ min-width:0; padding:12px; border-right:3px solid var(--line); }}
    .subgraph-cell:last-child {{ border-right:0; }}
    .subgraph-name {{ color:var(--acid); font-size:18px; font-weight:700; }}
    .subgraph-label {{ display:block; margin-bottom:6px; color:var(--muted); font-size:11px; font-weight:700; text-transform:uppercase; }}
    .sync-row {{ display:grid; grid-template-columns:170px minmax(0, 1fr); border-top:3px solid var(--line); }}
    .sync-block {{ padding:12px; border-right:3px solid var(--line); background:#101010; color:var(--acid); font-size:20px; font-weight:700; }}
    .sync-detail {{ min-width:0; padding:12px; }}
    .sync-counts {{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:10px; }}
    .sync-count {{ border:2px solid var(--line); padding:5px 7px; font-size:12px; font-weight:700; text-transform:uppercase; }}
    .sync-count.created {{ background:var(--acid); color:var(--void); }}
    .sync-count.updated {{ background:var(--blue); color:var(--void); }}
    .sync-count.removed {{ background:var(--hot); color:var(--void); }}
    .changes {{ display:flex; flex-wrap:wrap; gap:7px; }}
    .change {{ max-width:100%; display:inline-flex; align-items:center; gap:7px; border:1px solid rgba(243,240,230,.36); padding:5px 7px; font-size:12px; }}
    .change b {{ flex:0 0 auto; color:var(--muted); text-transform:uppercase; }}
    .change code {{ min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
    .change.created b {{ color:var(--acid); }}
    .change.updated b {{ color:var(--blue); }}
    .change.removed b {{ color:var(--hot); }}
    .empty-row {{ padding:14px 12px; border-top:3px solid var(--line); color:var(--muted); font-size:13px; font-weight:700; text-transform:uppercase; }}
    .footer {{ display:flex; flex-wrap:wrap; align-items:center; gap:0; border-top:3px solid var(--line); background:var(--void); }}
    .button {{ display:inline-flex; align-items:center; justify-content:center; min-height:46px; padding:10px 14px; border-right:3px solid var(--line); background:var(--void); color:var(--ink); text-decoration:none; font-size:13px; font-weight:700; text-transform:uppercase; }}
    .button:hover {{ background:var(--paper); color:var(--void); }}
    @media (max-width: 900px) {{ body::after {{ display:none; }} .shell {{ box-shadow:7px 7px 0 var(--acid); }} .topbar {{ flex-direction:column; }} header {{ grid-template-columns:86px 1fr; }} .mark {{ min-height:96px; font-size:25px; }} .brand {{ padding:13px; }} h1 {{ font-size:42px; }} .status {{ grid-column:1 / -1; border-left:0; border-top:3px solid var(--line); min-height:54px; }} .grid {{ grid-template-columns:1fr 1fr; }} .metric {{ border-bottom:3px solid var(--line); }} .metric:nth-child(2n) {{ border-right:0; }} .metric:first-child .value, .value {{ font-size:24px; }} .content {{ grid-template-columns:1fr; }} .panel {{ border-right:0; border-bottom:3px solid var(--line); }} .terminal li {{ grid-template-columns:1fr; gap:4px; }} .subgraph-row {{ grid-template-columns:1fr; }} .subgraph-cell {{ border-right:0; border-bottom:3px solid var(--line); }} .subgraph-cell:last-child {{ border-bottom:0; }} .sync-row {{ grid-template-columns:1fr; }} .sync-block {{ border-right:0; border-bottom:3px solid var(--line); }} .footer {{ display:grid; grid-template-columns:1fr; }} .button {{ margin-left:0; border-right:0; border-left:0; border-bottom:3px solid var(--line); width:100%; justify-content:flex-start; }} }}
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
        <div class="metric"><div class="label">History</div><div class="value">{history}</div></div>
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
            <li><span class="key">$ query</span><span>versioned endpoint active</span></li>
            <li><span class="key">$ api</span><span>public HTTP interface</span></li>
            <li><span class="key">$ refresh</span><span>10 seconds</span></li>
          </ul>
        </div>
      </section>
      <section class="section" aria-label="Public subgraphs">
        <div class="section-title">PUBLIC SUBGRAPHS</div>
        {public_subgraphs}
      </section>
      <section class="section" aria-label="Recent sync blocks">
        <div class="section-title">SYNC BLOCKS</div>
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
</body>
</html>"#,
        store = html_escape(&store.label()),
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
        history = history,
        validation_errors = validation_errors,
        public_subgraphs = public_subgraphs,
        sync_blocks = sync_blocks,
        health_class = if ok { "ok-text" } else { "warn-text" },
        health_text = if ok {
            "sync complete"
        } else {
            "attention required"
        }
    )
}

fn public_subgraphs_html(rows: &[storage::DeploymentMetadataRecord]) -> String {
    if rows.is_empty() {
        return r#"<div class="empty-row">no public subgraphs</div>"#.to_string();
    }
    rows.iter()
        .map(|metadata| {
            let version = metadata.version_label.as_deref().unwrap_or("latest");
            let endpoint = format!("/subgraphs/{}/{version}/gn", metadata.deployment);
            let deployed_by = metadata
                .owner_email
                .clone()
                .or_else(|| {
                    metadata
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
                name = html_escape(&metadata.deployment),
                version = html_escape(version),
                deployed_by = html_escape(&deployed_by),
                endpoint = html_escape(&endpoint)
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn sync_blocks_html(rows: &[storage::SyncBlockActivity]) -> String {
    if rows.is_empty() {
        return r#"<div class="empty-row">no retained sync activity</div>"#.to_string();
    }
    rows.iter()
        .map(|activity| {
            let changes = if activity.changes.is_empty() {
                r#"<span class="change"><b>idle</b><code>no entity changes</code></span>"#
                    .to_string()
            } else {
                activity
                    .changes
                    .iter()
                    .map(|change| {
                        let action = change.action.as_str();
                        let target = format!("{}:{}", change.entity, change.id);
                        format!(
                            r#"<span class="change {action}"><b>{action}</b><code>{target}</code></span>"#,
                            action = html_escape(action),
                            target = html_escape(&target)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("")
            };
            format!(
                concat!(
                    r#"<article class="sync-row">"#,
                    r#"<div class="sync-block">#{block}</div>"#,
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
                block = activity.block_number,
                created = activity.created,
                updated = activity.updated,
                removed = activity.removed,
                changes = changes
            )
        })
        .collect::<Vec<_>>()
        .join("")
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
        let sync_activity = vec![storage::SyncBlockActivity {
            block_number: 42,
            created: 1,
            updated: 2,
            removed: 0,
            changes: vec![storage::EntityChangeRecord {
                entity: "Token".to_string(),
                id: "0xabc".to_string(),
                action: storage::EntityChangeAction::Updated,
            }],
        }];

        let html = home_html(
            &store,
            Some(&status),
            Some(&metadata),
            std::slice::from_ref(&metadata),
            &sync_activity,
        );

        assert!(html.contains("OPERATIONAL"));
        assert!(html.contains("/subgraphs/growfi/4.0.2/gn"));
        assert!(html.contains("/subgraphs/growfi/latest/gn"));
        assert!(html.contains("PUBLIC SUBGRAPHS"));
        assert!(html.contains("SYNC BLOCKS"));
        assert!(html.contains("ops@ugraph.local"));
        assert!(html.contains("updated"));
        assert!(html.contains("Token:0xabc"));
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
}
