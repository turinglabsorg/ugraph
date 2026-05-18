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
    query::{execute_graphql_with_operation, GraphqlHttpRequest},
    state::StoreSnapshot,
    storage::SnapshotStore,
};

const MAX_HTTP_REQUEST_BYTES: usize = 1024 * 1024;

pub fn serve_store(store: SnapshotStore, bind: &str, once: bool) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind).with_context(|| format!("binding {bind}"))?;
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
    let (path, query_string) = target
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((target, None));

    match (method, path) {
        ("OPTIONS", _) => write_response(&mut stream, "204 No Content", "text/plain", b""),
        ("GET", "/") => write_response(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            graphiql_html().as_bytes(),
        ),
        ("GET", "/status") => {
            let (status, snapshot) = match store.load() {
                Ok(snapshot) => ("200 OK", Some(snapshot)),
                Err(_) => ("503 Service Unavailable", None),
            };
            write_response(
                &mut stream,
                status,
                "text/html; charset=utf-8",
                status_html(store, snapshot.as_ref()).as_bytes(),
            )
        }
        ("GET", "/graphql") => {
            if let Some(query_string) = query_string {
                let params = parse_query_params(query_string);
                if let Some(query) = params.get("query") {
                    return match store.load() {
                        Ok(snapshot) => {
                            let variables = params
                                .get("variables")
                                .and_then(|raw| serde_json::from_str(raw).ok())
                                .unwrap_or(serde_json::Value::Null);
                            let response = execute_graphql_with_operation(
                                &snapshot,
                                query,
                                &variables,
                                params.get("operationName").map(String::as_str),
                            );
                            write_json(&mut stream, &response)
                        }
                        Err(error) => write_json_status(
                            &mut stream,
                            "503 Service Unavailable",
                            &json!({ "errors": [{ "message": error.to_string() }] }),
                        ),
                    };
                }
            }
            write_response(
                &mut stream,
                "200 OK",
                "text/html; charset=utf-8",
                graphiql_html().as_bytes(),
            )
        }
        ("GET", "/healthz") => match store.load() {
            Ok(snapshot) => write_json(
                &mut stream,
                &json!({
                    "ok": true,
                    "store": store.label(),
                    "entities": snapshot.entities.len(),
                    "dynamicSources": snapshot.dynamic_sources.len(),
                    "historySnapshots": snapshot.history.len(),
                    "historyEarliestBlock": history_earliest_block(&snapshot),
                    "historyLatestBlock": history_latest_block(&snapshot),
                    "toBlock": snapshot.checkpoint.to_block,
                    "blockHash": snapshot.checkpoint.block_hash,
                    "complete": snapshot.checkpoint.complete,
                    "validationErrors": snapshot.checkpoint.validation_errors,
                }),
            ),
            Err(error) => write_json_status(
                &mut stream,
                "503 Service Unavailable",
                &json!({
                    "ok": false,
                    "store": store.label(),
                    "error": error.to_string(),
                }),
            ),
        },
        ("GET", "/metrics") => match store.load() {
            Ok(snapshot) => write_response(
                &mut stream,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                metrics_text(store, &snapshot).as_bytes(),
            ),
            Err(_) => write_response(
                &mut stream,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                unavailable_metrics_text(store).as_bytes(),
            ),
        },
        ("POST", "/graphql") => {
            let payload =
                serde_json::from_slice::<GraphqlHttpRequest>(body).unwrap_or(GraphqlHttpRequest {
                    query: String::new(),
                    variables: serde_json::Value::Null,
                    _operation_name: None,
                });
            match store.load() {
                Ok(snapshot) => {
                    let response = execute_graphql_with_operation(
                        &snapshot,
                        &payload.query,
                        &payload.variables,
                        payload._operation_name.as_deref(),
                    );
                    write_json(&mut stream, &response)
                }
                Err(error) => write_json_status(
                    &mut stream,
                    "503 Service Unavailable",
                    &json!({ "errors": [{ "message": error.to_string() }] }),
                ),
            }
        }
        _ => write_response(
            &mut stream,
            "404 Not Found",
            "application/json",
            br#"{"errors":[{"message":"not found"}]}"#,
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
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: content-type, authorization\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

fn metrics_text(store: &SnapshotStore, snapshot: &StoreSnapshot) -> String {
    let store = prometheus_label_value(&store.label());
    let complete = usize::from(snapshot.checkpoint.complete);
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
        entities = snapshot.entities.len(),
        dynamic_sources = snapshot.dynamic_sources.len(),
        history_snapshots = snapshot.history.len(),
        history_earliest_block = history_earliest_block(snapshot).unwrap_or(0),
        history_latest_block = history_latest_block(snapshot).unwrap_or(0),
        to_block = snapshot.checkpoint.to_block,
        complete = complete,
        validation_errors = snapshot.checkpoint.validation_errors
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

fn status_html(store: &SnapshotStore, snapshot: Option<&StoreSnapshot>) -> String {
    let ok = snapshot.is_some();
    let status = if ok { "operational" } else { "unavailable" };
    let badge = if ok { "ok" } else { "down" };
    let to_block = snapshot
        .map(|snapshot| snapshot.checkpoint.to_block.to_string())
        .unwrap_or_else(|| "-".to_string());
    let entities = snapshot
        .map(|snapshot| snapshot.entities.len().to_string())
        .unwrap_or_else(|| "-".to_string());
    let dynamic_sources = snapshot
        .map(|snapshot| snapshot.dynamic_sources.len().to_string())
        .unwrap_or_else(|| "-".to_string());
    let history = snapshot
        .map(|snapshot| snapshot.history.len().to_string())
        .unwrap_or_else(|| "-".to_string());
    let history_range = snapshot
        .and_then(|snapshot| {
            Some(format!(
                "{}-{}",
                history_earliest_block(snapshot)?,
                history_latest_block(snapshot)?
            ))
        })
        .unwrap_or_else(|| "-".to_string());
    let validation_errors = snapshot
        .map(|snapshot| snapshot.checkpoint.validation_errors.to_string())
        .unwrap_or_else(|| "-".to_string());
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>UGraph Status</title>
  <style>
    :root {{ color-scheme: light; --text:#171717; --muted:#666; --line:#ebebeb; --ok:#0070f3; --down:#c1121f; --bg:#fff; --soft:#fafafa; }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; min-height:100vh; background:var(--bg); color:var(--text); font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; letter-spacing:0; }}
    main {{ max-width: 860px; margin: 0 auto; padding: 56px 20px; }}
    header {{ display:flex; justify-content:space-between; align-items:flex-start; gap:24px; padding-bottom:28px; border-bottom:1px solid var(--line); }}
    h1 {{ margin:0; font-size:32px; line-height:1.15; font-weight:650; letter-spacing:0; }}
    .store {{ margin-top:8px; color:var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:13px; }}
    .badge {{ display:inline-flex; align-items:center; gap:8px; border-radius:999px; padding:6px 10px; font-size:13px; font-weight:600; box-shadow:0 0 0 1px rgba(0,0,0,.08); }}
    .dot {{ width:8px; height:8px; border-radius:50%; background:var(--down); }}
    .badge.ok .dot {{ background:var(--ok); }}
    .badge.ok {{ color:var(--ok); }}
    .badge.down {{ color:var(--down); }}
    .grid {{ display:grid; grid-template-columns: repeat(6, minmax(0,1fr)); gap:1px; margin-top:28px; background:var(--line); box-shadow:0 0 0 1px rgba(0,0,0,.08); border-radius:8px; overflow:hidden; }}
    .metric {{ background:var(--soft); padding:18px; min-width:0; }}
    .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }}
    .value {{ margin-top:12px; font-size:22px; line-height:1; font-weight:650; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    nav {{ display:flex; gap:14px; margin-top:22px; }}
    a {{ color:var(--text); text-decoration:none; font-size:14px; font-weight:500; }}
    a:hover {{ text-decoration:underline; }}
    @media (max-width: 760px) {{ header {{ flex-direction:column; }} .grid {{ grid-template-columns:1fr 1fr; }} }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>UGraph services</h1>
        <div class="store">{store}</div>
      </div>
      <div class="badge {badge}"><span class="dot"></span>{status}</div>
    </header>
    <section class="grid" aria-label="Service metrics">
      <div class="metric"><div class="label">Block</div><div class="value">{to_block}</div></div>
      <div class="metric"><div class="label">Entities</div><div class="value">{entities}</div></div>
      <div class="metric"><div class="label">Sources</div><div class="value">{dynamic_sources}</div></div>
      <div class="metric"><div class="label">History</div><div class="value">{history}</div></div>
      <div class="metric"><div class="label">Range</div><div class="value">{history_range}</div></div>
      <div class="metric"><div class="label">Errors</div><div class="value">{validation_errors}</div></div>
    </section>
    <nav>
      <a href="/">GraphiQL</a>
      <a href="/healthz">Health</a>
      <a href="/metrics">Metrics</a>
    </nav>
  </main>
</body>
</html>"#,
        store = html_escape(&store.label()),
        badge = badge,
        status = status,
        to_block = to_block,
        entities = entities,
        dynamic_sources = dynamic_sources,
        history = history,
        history_range = history_range,
        validation_errors = validation_errors
    )
}

fn history_earliest_block(snapshot: &StoreSnapshot) -> Option<u64> {
    snapshot
        .history
        .iter()
        .map(|entry| entry.checkpoint.to_block)
        .min()
}

fn history_latest_block(snapshot: &StoreSnapshot) -> Option<u64> {
    snapshot
        .history
        .iter()
        .map(|entry| entry.checkpoint.to_block)
        .max()
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

fn graphiql_html() -> &'static str {
    r#"<!doctype html>
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
      const endpoint = '/graphql';
      const defaultQuery = '{\n  _meta { block { number hash } hasIndexingErrors }\n}';

      function graphQLFetcher(graphQLParams) {
        return fetch(endpoint, {
          method: 'post',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(graphQLParams)
        }).then((response) => response.json());
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
          const fetcher = GraphiQL.createFetcher
            ? GraphiQL.createFetcher({ url: endpoint })
            : graphQLFetcher;
          const element = React.createElement(GraphiQL, { fetcher, defaultQuery });
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
</html>"#
}

#[cfg(test)]
mod tests {
    use ugraph_core::EntitySchema;

    use super::*;
    use crate::state::SyncCheckpoint;

    #[test]
    fn metrics_include_checkpoint_and_store_health() {
        let store = SnapshotStore::Postgres {
            url: "postgres://example".to_string(),
            deployment: "growfi".to_string(),
        };
        let snapshot = StoreSnapshot {
            version: 1,
            manifest: "subgraph.yaml".to_string(),
            checkpoint: SyncCheckpoint {
                from_block: Some(10),
                to_block: 42,
                block_hash: Some("0xabc".to_string()),
                scanned_logs: 3,
                executed_logs: 2,
                validation_errors: 1,
                complete: true,
            },
            schema: EntitySchema::default(),
            entities: Vec::new(),
            dynamic_sources: Vec::new(),
            processed_logs: Vec::new(),
            history: Vec::new(),
        };

        let metrics = metrics_text(&store, &snapshot);

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
        let html = graphiql_html();

        assert!(html.contains("react@18.2.0"));
        assert!(html.contains("graphiql@2.4.7"));
        assert!(html.contains("GraphiQL assets did not load"));
        assert!(html.contains("graphQLFetcher"));
    }
}
