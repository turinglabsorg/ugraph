use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use thiserror::Error;
use ugraph_core::{
    inspect_wasm_tree, DataSource, EntityField, EntitySchema, Manifest, ManifestError, MatchedLog,
    WasmInspectError, WasmInspection,
};
use wasmtime::{Engine, ExternType, Func, Instance, Linker, Module, Store, ValType};

mod asc;

pub use asc::{
    DataSourceCreateCall, EntityData, EntityStore, EthereumCallCache, EthereumCallReport,
    HandlerExecutionReport, StoreSetCall, StoreValue,
};

#[derive(Debug, Error)]
pub enum RuntimeCheckError {
    #[error(transparent)]
    Inspect(#[from] WasmInspectError),
    #[error("failed to compile wasm module {path}: {source}")]
    Compile {
        path: PathBuf,
        #[source]
        source: wasmtime::Error,
    },
    #[error("failed to link wasm module {path}: {source}")]
    Link {
        path: PathBuf,
        #[source]
        source: wasmtime::Error,
    },
    #[error("failed to instantiate wasm module {path}: {source}")]
    Instantiate {
        path: PathBuf,
        #[source]
        source: wasmtime::Error,
    },
    #[error("failed to start wasm module {path}: {source}")]
    Start {
        path: PathBuf,
        #[source]
        source: wasmtime::Error,
    },
}

#[derive(Debug, Clone)]
pub struct RuntimeModuleCheck {
    pub path: PathBuf,
    pub import_count: usize,
    pub host_imports: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeCheck {
    pub modules: Vec<RuntimeModuleCheck>,
}

#[derive(Debug, Default)]
pub struct RuntimeHostState {
    pub call_counts: BTreeMap<String, usize>,
    pub store: EntityStore,
    pub store_sets: Vec<StoreSetCall>,
    pub data_source_creates: Vec<DataSourceCreateCall>,
    pub ethereum_calls: Vec<EthereumCallReport>,
    pub ethereum_call_cache: EthereumCallCache,
    pub rpc_url: Option<String>,
    pub rpc_urls: Vec<String>,
    pub block_number: Option<u64>,
    pub data_source_address: Option<String>,
    pub data_source_network: Option<String>,
    pub data_source_context: EntityData,
    type_ids: BTreeMap<i32, i32>,
}

#[derive(Debug, Error)]
pub enum HandlerSignatureError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("failed to compile wasm module {path}: {source}")]
    Compile {
        path: PathBuf,
        #[source]
        source: wasmtime::Error,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HandlerSignatureReport {
    pub ok: bool,
    pub handlers: Vec<HandlerSignatureCheck>,
    pub invalid_handlers: Vec<HandlerSignatureCheck>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HandlerSignatureCheck {
    pub data_source: String,
    pub template: bool,
    pub wasm_path: PathBuf,
    pub handler: String,
    pub exported: bool,
    pub params: Vec<String>,
    pub results: Vec<String>,
    pub graph_node_event_handler: bool,
}

#[derive(Debug, Error)]
pub enum GraphTypeIdError {
    #[error(transparent)]
    Inspect(#[from] WasmInspectError),
    #[error(transparent)]
    Runtime(#[from] RuntimeCheckError),
    #[error("failed to call id_of_type in wasm module {path}: {source}")]
    Call {
        path: PathBuf,
        #[source]
        source: wasmtime::Error,
    },
}

#[derive(Debug, Error)]
pub enum HandlerExecutionError {
    #[error(transparent)]
    Runtime(#[from] RuntimeCheckError),
    #[error("failed to allocate Ethereum event for {handler}: {source}")]
    Allocate {
        handler: String,
        #[source]
        source: wasmtime::Error,
    },
    #[error("failed to call handler {handler}: {source}")]
    Call {
        handler: String,
        #[source]
        source: wasmtime::Error,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GraphTypeIdReport {
    pub modules: Vec<GraphTypeIdModule>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GraphTypeIdModule {
    pub path: PathBuf,
    pub type_ids: BTreeMap<String, i32>,
}

struct StubInstantiation {
    store: Store<RuntimeHostState>,
    instance: Instance,
    host_imports: Vec<String>,
}

pub struct RuntimeModuleCache {
    engine: Engine,
    modules: BTreeMap<PathBuf, Module>,
}

impl RuntimeModuleCache {
    pub fn new() -> Self {
        Self {
            engine: Engine::default(),
            modules: BTreeMap::new(),
        }
    }

    pub fn compiled_module_count(&self) -> usize {
        self.modules.len()
    }

    fn module_for_path(&mut self, path: &Path) -> Result<Module, RuntimeCheckError> {
        if let Some(module) = self.modules.get(path) {
            return Ok(module.clone());
        }
        let module =
            Module::from_file(&self.engine, path).map_err(|source| RuntimeCheckError::Compile {
                path: path.to_path_buf(),
                source,
            })?;
        self.modules.insert(path.to_path_buf(), module.clone());
        Ok(module)
    }
}

impl Default for RuntimeModuleCache {
    fn default() -> Self {
        Self::new()
    }
}

pub fn check_wasm_tree(root: impl AsRef<Path>) -> Result<RuntimeCheck, RuntimeCheckError> {
    let tree = inspect_wasm_tree(root)?;
    let engine = Engine::default();
    let mut modules = Vec::new();

    for inspection in tree.files {
        let host_imports = instantiate_with_stub_host(&engine, &inspection)?;
        modules.push(RuntimeModuleCheck {
            path: inspection.path,
            import_count: inspection.imports.len(),
            host_imports,
        });
    }

    Ok(RuntimeCheck { modules })
}

pub fn inspect_graph_type_ids(
    root: impl AsRef<Path>,
) -> Result<GraphTypeIdReport, GraphTypeIdError> {
    let tree = inspect_wasm_tree(root)?;
    let engine = Engine::default();
    let mut modules = Vec::new();

    for inspection in tree.files {
        let mut stub = instantiate_wasm_path(&engine, &inspection.path)?;
        let id_of_type = stub
            .instance
            .get_typed_func::<i32, i32>(&mut stub.store, "id_of_type")
            .map_err(|source| GraphTypeIdError::Call {
                path: inspection.path.clone(),
                source,
            })?;
        let mut type_ids = BTreeMap::new();
        for (name, discriminant) in GRAPH_TS_TYPE_IDS {
            let runtime_id = id_of_type
                .call(&mut stub.store, *discriminant)
                .map_err(|source| GraphTypeIdError::Call {
                    path: inspection.path.clone(),
                    source,
                })?;
            type_ids.insert((*name).to_string(), runtime_id);
        }
        modules.push(GraphTypeIdModule {
            path: inspection.path,
            type_ids,
        });
    }

    Ok(GraphTypeIdReport { modules })
}

pub fn check_handler_signatures(
    manifest_path: impl AsRef<Path>,
    build_dir: impl AsRef<Path>,
) -> Result<HandlerSignatureReport, HandlerSignatureError> {
    let manifest_path = manifest_path.as_ref();
    let build_dir = build_dir.as_ref();
    let manifest = Manifest::load(manifest_path)?;
    manifest.validate_files(manifest_path)?;

    let engine = Engine::default();
    let build_manifest = Manifest::load(build_dir.join("subgraph.yaml")).ok();
    let mut handlers = Vec::new();
    for source in &manifest.data_sources {
        let wasm_path = compiled_wasm_path(
            build_dir,
            build_manifest
                .as_ref()
                .and_then(|manifest| {
                    manifest
                        .data_sources
                        .iter()
                        .find(|item| item.name == source.name)
                })
                .unwrap_or(source),
            false,
        );
        handlers.extend(check_source_handler_signatures(
            &engine,
            &source.name,
            false,
            wasm_path,
            source.mapping.handler_names().into_iter(),
        )?);
    }
    for template in &manifest.templates {
        let wasm_path = compiled_wasm_path(
            build_dir,
            build_manifest
                .as_ref()
                .and_then(|manifest| {
                    manifest
                        .templates
                        .iter()
                        .find(|item| item.name == template.name)
                })
                .unwrap_or(template),
            true,
        );
        handlers.extend(check_source_handler_signatures(
            &engine,
            &template.name,
            true,
            wasm_path,
            template.mapping.handler_names().into_iter(),
        )?);
    }

    let invalid_handlers = handlers
        .iter()
        .filter(|handler| !handler.graph_node_event_handler)
        .cloned()
        .collect::<Vec<_>>();

    Ok(HandlerSignatureReport {
        ok: invalid_handlers.is_empty(),
        handlers,
        invalid_handlers,
    })
}

fn compiled_wasm_path(build_dir: &Path, source: &DataSource, template: bool) -> PathBuf {
    if source.mapping.file.ends_with(".wasm") {
        return build_dir.join(&source.mapping.file);
    }
    if template {
        build_dir
            .join("templates")
            .join(&source.name)
            .join(format!("{}.wasm", source.name))
    } else {
        build_dir
            .join(&source.name)
            .join(format!("{}.wasm", source.name))
    }
}

pub fn execute_matched_log_handler(
    wasm_path: impl AsRef<Path>,
    log: &MatchedLog,
) -> Result<HandlerExecutionReport, HandlerExecutionError> {
    let mut store = EntityStore::new();
    execute_matched_log_handler_with_store(wasm_path, log, &mut store)
}

pub fn execute_matched_log_handler_with_store(
    wasm_path: impl AsRef<Path>,
    log: &MatchedLog,
    entity_store: &mut EntityStore,
) -> Result<HandlerExecutionReport, HandlerExecutionError> {
    execute_matched_log_handler_with_context(wasm_path, log, entity_store, None)
}

pub fn execute_matched_log_handler_with_context(
    wasm_path: impl AsRef<Path>,
    log: &MatchedLog,
    entity_store: &mut EntityStore,
    rpc_url: Option<&str>,
) -> Result<HandlerExecutionReport, HandlerExecutionError> {
    let mut ethereum_call_cache = EthereumCallCache::new();
    execute_matched_log_handler_with_context_and_cache(
        wasm_path,
        log,
        entity_store,
        rpc_url,
        &mut ethereum_call_cache,
    )
}

pub fn execute_matched_log_handler_with_context_and_cache(
    wasm_path: impl AsRef<Path>,
    log: &MatchedLog,
    entity_store: &mut EntityStore,
    rpc_url: Option<&str>,
    ethereum_call_cache: &mut EthereumCallCache,
) -> Result<HandlerExecutionReport, HandlerExecutionError> {
    let mut runtime_cache = RuntimeModuleCache::new();
    execute_matched_log_handler_with_runtime_cache(
        wasm_path,
        log,
        entity_store,
        rpc_url,
        ethereum_call_cache,
        &mut runtime_cache,
    )
}

pub fn execute_matched_log_handler_with_runtime_cache(
    wasm_path: impl AsRef<Path>,
    log: &MatchedLog,
    entity_store: &mut EntityStore,
    rpc_url: Option<&str>,
    ethereum_call_cache: &mut EthereumCallCache,
    runtime_cache: &mut RuntimeModuleCache,
) -> Result<HandlerExecutionReport, HandlerExecutionError> {
    execute_matched_log_handler_with_runtime_cache_and_data_source_context(
        wasm_path,
        log,
        entity_store,
        rpc_url,
        ethereum_call_cache,
        runtime_cache,
        None,
    )
}

pub fn execute_matched_log_handler_with_runtime_cache_and_data_source_context(
    wasm_path: impl AsRef<Path>,
    log: &MatchedLog,
    entity_store: &mut EntityStore,
    rpc_url: Option<&str>,
    ethereum_call_cache: &mut EthereumCallCache,
    runtime_cache: &mut RuntimeModuleCache,
    data_source_context: Option<&EntityData>,
) -> Result<HandlerExecutionReport, HandlerExecutionError> {
    let rpc_urls = rpc_url
        .map(|rpc_url| vec![rpc_url.to_string()])
        .unwrap_or_default();
    execute_matched_log_handler_with_runtime_cache_data_source_context_and_rpc_urls(
        wasm_path,
        log,
        entity_store,
        &rpc_urls,
        ethereum_call_cache,
        runtime_cache,
        data_source_context,
    )
}

pub fn execute_matched_log_handler_with_runtime_cache_data_source_context_and_rpc_urls(
    wasm_path: impl AsRef<Path>,
    log: &MatchedLog,
    entity_store: &mut EntityStore,
    rpc_urls: &[String],
    ethereum_call_cache: &mut EthereumCallCache,
    runtime_cache: &mut RuntimeModuleCache,
    data_source_context: Option<&EntityData>,
) -> Result<HandlerExecutionReport, HandlerExecutionError> {
    let wasm_path = wasm_path.as_ref();
    let mut stub = instantiate_wasm_path_with_cache(runtime_cache, wasm_path)?;
    {
        let state = stub.store.data_mut();
        state.store = entity_store.clone();
        state.ethereum_call_cache = ethereum_call_cache.clone();
        state.rpc_url = rpc_urls.first().cloned();
        state.rpc_urls = rpc_urls.to_vec();
        state.block_number = log.block_number;
        state.data_source_address = Some(log.address.clone());
        state.data_source_network = log.network.clone();
        state.data_source_context = data_source_context.cloned().unwrap_or_default();
    }
    let event_ptr = {
        let mut allocator =
            asc::AscAllocator::new(&mut stub.store, &stub.instance).map_err(|source| {
                HandlerExecutionError::Allocate {
                    handler: log.handler.clone(),
                    source,
                }
            })?;
        allocator
            .alloc_event(log)
            .map_err(|source| HandlerExecutionError::Allocate {
                handler: log.handler.clone(),
                source,
            })?
    };
    let handler = stub
        .instance
        .get_typed_func::<i32, ()>(&mut stub.store, &log.handler)
        .map_err(|source| HandlerExecutionError::Call {
            handler: log.handler.clone(),
            source,
        })?;
    handler
        .call(&mut stub.store, event_ptr)
        .map_err(|source| HandlerExecutionError::Call {
            handler: log.handler.clone(),
            source,
        })?;

    let state = stub.store.data();
    *entity_store = state.store.clone();
    *ethereum_call_cache = state.ethereum_call_cache.clone();
    Ok(HandlerExecutionReport {
        wasm_path: wasm_path.display().to_string(),
        handler: log.handler.clone(),
        event_ptr,
        call_counts: state.call_counts.clone(),
        store_sets: state.store_sets.clone(),
        data_source_creates: state.data_source_creates.clone(),
        ethereum_calls: state.ethereum_calls.clone(),
    })
}

pub fn validate_store_sets(schema: &EntitySchema, sets: &mut [StoreSetCall]) -> usize {
    let mut count = 0;
    for set in sets {
        set.validation_errors = validate_store_set(schema, set);
        count += set.validation_errors.len();
    }
    count
}

pub fn validate_store_set(schema: &EntitySchema, set: &StoreSetCall) -> Vec<String> {
    let mut errors = Vec::new();
    let Some(entity_name) = set.entity.as_deref() else {
        errors.push("store.set is missing entity type".to_string());
        return errors;
    };
    let Some(entity_type) = schema.entities.get(entity_name) else {
        errors.push(format!("unknown entity type `{entity_name}`"));
        return errors;
    };
    let Some(data) = set.data.as_ref() else {
        errors.push(format!("store.set `{entity_name}` is missing entity data"));
        return errors;
    };

    if set.id.is_none() {
        errors.push(format!("store.set `{entity_name}` is missing id"));
    }

    for key in data.keys() {
        match entity_type.fields.get(key) {
            Some(field) if field.derived => {
                errors.push(format!(
                    "{}.{} is @derivedFrom and must not be stored directly",
                    entity_name, key
                ));
            }
            Some(_) => {}
            None => errors.push(format!("{}.{} is not declared in schema", entity_name, key)),
        }
    }

    for field in entity_type.fields.values() {
        if field.derived {
            continue;
        }
        match data.get(&field.name) {
            Some(StoreValue::Null) if field.required => {
                errors.push(format!("{}.{} is required", entity_name, field.name));
            }
            Some(StoreValue::Null) => {}
            Some(value) => validate_field_value(entity_name, field, value, &mut errors),
            None if field.required => {
                errors.push(format!("{}.{} is required", entity_name, field.name));
            }
            None => {}
        }
    }

    if let (Some(set_id), Some(value)) = (set.id.as_deref(), data.get("id")) {
        if let Some(data_id) = store_value_id(value) {
            if !ids_match(set_id, &data_id) {
                errors.push(format!(
                    "{}.id `{}` does not match store id `{}`",
                    entity_name, data_id, set_id
                ));
            }
        }
    }

    errors
}

fn validate_field_value(
    entity_name: &str,
    field: &EntityField,
    value: &StoreValue,
    errors: &mut Vec<String>,
) {
    if field.list {
        match value {
            StoreValue::Array(values) => {
                for (index, item) in values.iter().enumerate() {
                    if item == &StoreValue::Null {
                        errors.push(format!(
                            "{}.{}[{}] cannot contain null values",
                            entity_name, field.name, index
                        ));
                    } else if !scalar_matches(&field.kind, item) {
                        errors.push(format!(
                            "{}.{}[{}] expected {}, got {}",
                            entity_name,
                            field.name,
                            index,
                            field.kind,
                            store_value_kind(item)
                        ));
                    }
                }
            }
            other => errors.push(format!(
                "{}.{} expected [{}], got {}",
                entity_name,
                field.name,
                field.kind,
                store_value_kind(other)
            )),
        }
        return;
    }

    if !scalar_matches(&field.kind, value) {
        errors.push(format!(
            "{}.{} expected {}, got {}",
            entity_name,
            field.name,
            field.kind,
            store_value_kind(value)
        ));
    }
}

fn scalar_matches(kind: &str, value: &StoreValue) -> bool {
    match kind {
        "ID" => matches!(value, StoreValue::String(_) | StoreValue::Bytes(_)),
        "Bytes" => matches!(value, StoreValue::Bytes(_)),
        "String" => matches!(value, StoreValue::String(_)),
        "Boolean" | "Bool" => matches!(value, StoreValue::Bool(_)),
        "Int" => matches!(value, StoreValue::Int(_)),
        "Int8" => matches!(value, StoreValue::Int8(_) | StoreValue::Int(_)),
        "Timestamp" => matches!(value, StoreValue::Timestamp(_) | StoreValue::BigInt(_)),
        "BigInt" => matches!(value, StoreValue::BigInt(_)),
        "BigDecimal" => matches!(value, StoreValue::BigDecimal { .. }),
        _ => matches!(value, StoreValue::String(_) | StoreValue::Bytes(_)),
    }
}

fn store_value_id(value: &StoreValue) -> Option<String> {
    match value {
        StoreValue::String(value) | StoreValue::Bytes(value) | StoreValue::BigInt(value) => {
            Some(value.clone())
        }
        StoreValue::Int(value) => Some(value.to_string()),
        StoreValue::Int8(value) | StoreValue::Timestamp(value) => Some(value.to_string()),
        _ => None,
    }
}

fn ids_match(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left.starts_with("0x")
        || left.starts_with("0X")
        || right.starts_with("0x")
        || right.starts_with("0X")
    {
        return left.eq_ignore_ascii_case(right);
    }
    left == right
}

fn store_value_kind(value: &StoreValue) -> &'static str {
    match value {
        StoreValue::String(_) => "String",
        StoreValue::Int(_) => "Int",
        StoreValue::BigDecimal { .. } => "BigDecimal",
        StoreValue::Bool(_) => "Boolean",
        StoreValue::Array(_) => "Array",
        StoreValue::Null => "Null",
        StoreValue::Bytes(_) => "Bytes",
        StoreValue::BigInt(_) => "BigInt",
        StoreValue::Int8(_) => "Int8",
        StoreValue::Timestamp(_) => "Timestamp",
    }
}

fn instantiate_with_stub_host(
    engine: &Engine,
    inspection: &WasmInspection,
) -> Result<Vec<String>, RuntimeCheckError> {
    instantiate_wasm_path(engine, &inspection.path).map(|stub| stub.host_imports)
}

fn instantiate_wasm_path(
    engine: &Engine,
    path: impl AsRef<Path>,
) -> Result<StubInstantiation, RuntimeCheckError> {
    let path = path.as_ref();
    let module = Module::from_file(engine, path).map_err(|source| RuntimeCheckError::Compile {
        path: path.to_path_buf(),
        source,
    })?;
    instantiate_module_with_stub_host(engine, path, &module)
}

fn instantiate_wasm_path_with_cache(
    cache: &mut RuntimeModuleCache,
    path: impl AsRef<Path>,
) -> Result<StubInstantiation, RuntimeCheckError> {
    let path = path.as_ref();
    let engine = cache.engine.clone();
    let module = cache.module_for_path(path)?;
    instantiate_module_with_stub_host(&engine, path, &module)
}

fn instantiate_module_with_stub_host(
    engine: &Engine,
    path: &Path,
    module: &Module,
) -> Result<StubInstantiation, RuntimeCheckError> {
    let mut linker: Linker<RuntimeHostState> = Linker::new(engine);
    let mut store = Store::new(engine, RuntimeHostState::default());
    let mut host_imports = Vec::new();

    for import in module.imports() {
        let ExternType::Func(func_ty) = import.ty() else {
            continue;
        };
        let import_name = graph_host_name(import.module(), import.name());
        let result_types = func_ty.results().collect::<Vec<_>>();
        let func = Func::new(&mut store, func_ty, move |caller, params, results| {
            asc::handle_host_call(caller, &import_name, params, results, &result_types)
        });
        linker
            .define(&store, import.module(), import.name(), func)
            .map_err(|source| RuntimeCheckError::Link {
                path: path.to_path_buf(),
                source,
            })?;
        host_imports.push(graph_host_name(import.module(), import.name()));
    }

    let instance = linker.instantiate(&mut store, module).map_err(|source| {
        RuntimeCheckError::Instantiate {
            path: path.to_path_buf(),
            source,
        }
    })?;
    if let Ok(start) = instance.get_typed_func::<(), ()>(&mut store, "_start") {
        start
            .call(&mut store, ())
            .map_err(|source| RuntimeCheckError::Start {
                path: path.to_path_buf(),
                source,
            })?;
    }

    host_imports.sort();
    host_imports.dedup();
    Ok(StubInstantiation {
        store,
        instance,
        host_imports,
    })
}

const GRAPH_TS_TYPE_IDS: &[(&str, i32)] = &[
    ("String", 0),
    ("ArrayBuffer", 1),
    ("Uint8Array", 6),
    ("BigDecimal", 12),
    ("ArrayUint8Array", 14),
    ("ArrayEthereumValue", 15),
    ("ArrayStoreValue", 16),
    ("ArrayJsonValue", 17),
    ("ArrayString", 18),
    ("ArrayEventParam", 19),
    ("ArrayTypedMapEntryStringJsonValue", 20),
    ("ArrayTypedMapEntryStringStoreValue", 21),
    ("EventParam", 23),
    ("EthereumTransaction", 24),
    ("EthereumBlock", 25),
    ("EthereumCall", 26),
    ("WrappedBool", 28),
    ("WrappedJsonValue", 29),
    ("EthereumValue", 30),
    ("StoreValue", 31),
    ("JsonValue", 32),
    ("EthereumEvent", 33),
    ("TypedMapEntryStringStoreValue", 34),
    ("TypedMapEntryStringJsonValue", 35),
    ("TypedMapStringStoreValue", 36),
    ("TypedMapStringJsonValue", 37),
    ("ResultJsonValueBool", 40),
    ("ArrayU8", 41),
];

fn graph_host_name(module: &str, name: &str) -> String {
    if name.contains('.') || module.is_empty() || module == "env" {
        name.to_string()
    } else {
        format!("{module}.{name}")
    }
}

fn check_source_handler_signatures<'a>(
    engine: &Engine,
    data_source: &str,
    template: bool,
    wasm_path: PathBuf,
    handlers: impl Iterator<Item = &'a str>,
) -> Result<Vec<HandlerSignatureCheck>, HandlerSignatureError> {
    let module =
        Module::from_file(engine, &wasm_path).map_err(|source| HandlerSignatureError::Compile {
            path: wasm_path.clone(),
            source,
        })?;

    Ok(handlers
        .map(|handler| {
            let mut params = Vec::new();
            let mut results = Vec::new();
            let mut exported = false;
            for export in module.exports() {
                if export.name() == handler {
                    exported = true;
                    if let ExternType::Func(func_ty) = export.ty() {
                        params = func_ty.params().map(format_val_type).collect();
                        results = func_ty.results().map(format_val_type).collect();
                    }
                    break;
                }
            }
            let graph_node_event_handler = exported
                && params.len() == 1
                && params.first().is_some_and(|param| param == "i32")
                && results.is_empty();
            HandlerSignatureCheck {
                data_source: data_source.to_string(),
                template,
                wasm_path: wasm_path.clone(),
                handler: handler.to_string(),
                exported,
                params,
                results,
                graph_node_event_handler,
            }
        })
        .collect())
}

fn format_val_type(ty: ValType) -> String {
    ty.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ugraph_core::{DecodedEventParam, DecodedValue, EntityField, MatchedLog};

    #[test]
    fn growfi_wasm_modules_instantiate_with_known_import_shapes() -> Result<(), RuntimeCheckError> {
        let check = check_wasm_tree("../../examples/growfi/build")?;
        assert_eq!(check.modules.len(), 10);
        Ok(())
    }

    #[test]
    fn growfi_handlers_have_graph_node_event_signatures() -> Result<(), HandlerSignatureError> {
        let report = check_handler_signatures(
            "../../examples/growfi/subgraph.yaml",
            "../../examples/growfi/build",
        )?;
        assert!(report.ok, "invalid handlers: {:?}", report.invalid_handlers);
        assert_eq!(report.handlers.len(), 77);
        Ok(())
    }

    #[test]
    fn growfi_wasm_exposes_graph_ts_type_ids() -> Result<(), GraphTypeIdError> {
        let report = inspect_graph_type_ids("../../examples/growfi/build")?;
        assert_eq!(report.modules.len(), 10);
        assert!(report
            .modules
            .iter()
            .all(|module| module.type_ids.contains_key("EthereumEvent")));
        Ok(())
    }

    #[test]
    fn growfi_handler_executes_with_asc_ethereum_event() -> Result<(), HandlerExecutionError> {
        let log = growfi_contracts_set_log();

        let report = execute_matched_log_handler(
            "../../examples/growfi/build/CampaignFactory/CampaignFactory.wasm",
            &log,
        )?;
        let protocol = report
            .store_sets
            .iter()
            .find(|set| set.entity.as_deref() == Some("Protocol"))
            .expect("Protocol store.set was captured");
        let data = protocol.data.as_ref().expect("Protocol entity was decoded");
        assert_eq!(protocol.id.as_deref(), Some("0x70726f746f636f6c"));
        assert_eq!(
            data.get("growToken"),
            Some(&StoreValue::Bytes(
                "0x3740c0fcb8d71961b893743548e07b8d265b0a33".to_string()
            ))
        );
        assert_eq!(
            data.get("growMinter"),
            Some(&StoreValue::Bytes(
                "0x369fe004842a3b3fb5285a30c63c9ba60aa4f99f".to_string()
            ))
        );
        assert_eq!(
            data.get("growTreasury"),
            Some(&StoreValue::Bytes(
                "0x9b7898cd64741d7a5503e8092f04fef2106c2291".to_string()
            ))
        );
        assert_eq!(
            data.get("growFeeSplitter"),
            Some(&StoreValue::Bytes(
                "0x892bd2ab53c26b09cd874c5afeb331cac8851848".to_string()
            ))
        );
        assert!(report.call_counts.contains_key("store.get"));
        assert!(report.call_counts.contains_key("store.set"));
        assert!(report.call_counts.contains_key("typeConversion.bytesToHex"));
        Ok(())
    }

    #[test]
    fn growfi_store_get_rehydrates_existing_entity() -> Result<(), HandlerExecutionError> {
        let log = growfi_contracts_set_log();
        let protocol_id = "0x70726f746f636f6c".to_string();
        let mut entity = EntityData::new();
        entity.insert("id".to_string(), StoreValue::Bytes(protocol_id.clone()));
        entity.insert("legacy".to_string(), StoreValue::String("keep".to_string()));
        let mut store = EntityStore::new();
        store.insert(("Protocol".to_string(), protocol_id.clone()), entity);

        let report = execute_matched_log_handler_with_store(
            "../../examples/growfi/build/CampaignFactory/CampaignFactory.wasm",
            &log,
            &mut store,
        )?;
        let protocol = report
            .store_sets
            .iter()
            .find(|set| set.entity.as_deref() == Some("Protocol"))
            .expect("Protocol store.set was captured");
        let data = protocol.data.as_ref().expect("Protocol entity was decoded");
        assert_eq!(
            data.get("legacy"),
            Some(&StoreValue::String("keep".to_string()))
        );
        assert_eq!(
            store
                .get(&("Protocol".to_string(), protocol_id))
                .and_then(|entity| entity.get("legacy")),
            Some(&StoreValue::String("keep".to_string()))
        );
        Ok(())
    }

    #[test]
    fn growfi_campaign_created_captures_dynamic_data_sources() -> Result<(), HandlerExecutionError>
    {
        let log = growfi_campaign_created_log();
        let report = execute_matched_log_handler(
            "../../examples/growfi/build/CampaignFactory/CampaignFactory.wasm",
            &log,
        )?;
        let creates = report
            .data_source_creates
            .iter()
            .map(|create| (create.name.as_deref(), create.params.as_slice()))
            .collect::<Vec<_>>();
        assert_eq!(creates.len(), 3);
        assert!(creates.contains(&(
            Some("Campaign"),
            ["0x0000000000000000000000000000000000001001".to_string()].as_slice()
        )));
        assert!(creates.contains(&(
            Some("StakingVault"),
            ["0x0000000000000000000000000000000000001005".to_string()].as_slice()
        )));
        assert!(creates.contains(&(
            Some("HarvestManager"),
            ["0x0000000000000000000000000000000000001006".to_string()].as_slice()
        )));
        let campaign = report
            .store_sets
            .iter()
            .find(|set| set.entity.as_deref() == Some("Campaign"))
            .expect("Campaign store.set was captured");
        let data = campaign.data.as_ref().expect("Campaign entity was decoded");
        assert_eq!(
            data.get("currentYieldRate"),
            Some(&StoreValue::BigInt("5000000000000000000".to_string()))
        );
        Ok(())
    }

    #[test]
    fn runtime_module_cache_reuses_compiled_modules() -> Result<(), HandlerExecutionError> {
        let log = growfi_contracts_set_log();
        let mut store = EntityStore::new();
        let mut ethereum_call_cache = EthereumCallCache::new();
        let mut runtime_cache = RuntimeModuleCache::new();
        let wasm_path = "../../examples/growfi/build/CampaignFactory/CampaignFactory.wasm";

        execute_matched_log_handler_with_runtime_cache(
            wasm_path,
            &log,
            &mut store,
            None,
            &mut ethereum_call_cache,
            &mut runtime_cache,
        )?;
        execute_matched_log_handler_with_runtime_cache(
            wasm_path,
            &log,
            &mut store,
            None,
            &mut ethereum_call_cache,
            &mut runtime_cache,
        )?;

        assert_eq!(runtime_cache.compiled_module_count(), 1);
        Ok(())
    }

    #[test]
    fn schema_validation_rejects_unknown_and_wrongly_typed_fields() {
        let mut schema = EntitySchema::default();
        schema.entities.insert(
            "Protocol".to_string(),
            ugraph_core::EntityType {
                name: "Protocol".to_string(),
                fields: [
                    schema_field("id", "Bytes", false, true, false),
                    schema_field("growToken", "Bytes", false, true, false),
                    schema_field("campaigns", "Campaign", true, false, true),
                ]
                .into_iter()
                .map(|field| (field.name.clone(), field))
                .collect(),
            },
        );
        let mut data = EntityData::new();
        data.insert("id".to_string(), StoreValue::Bytes("0xabc".to_string()));
        data.insert(
            "growToken".to_string(),
            StoreValue::String("bad".to_string()),
        );
        data.insert("campaigns".to_string(), StoreValue::Array(Vec::new()));
        data.insert("extra".to_string(), StoreValue::Bool(true));

        let set = StoreSetCall {
            entity: Some("Protocol".to_string()),
            id: Some("0xabc".to_string()),
            data: Some(data),
            validation_errors: Vec::new(),
        };
        let errors = validate_store_set(&schema, &set);
        assert!(errors.iter().any(|error| error.contains("growToken")));
        assert!(errors.iter().any(|error| error.contains("campaigns")));
        assert!(errors.iter().any(|error| error.contains("extra")));
    }

    fn growfi_contracts_set_log() -> MatchedLog {
        MatchedLog {
            source: "CampaignFactory".to_string(),
            template: false,
            handler: "handleGrowfiContractsSet".to_string(),
            signature: "GrowfiContractsSet(address,address,address,address)".to_string(),
            network: Some("sepolia".to_string()),
            topic0: "0x36e18aa22e52d42a86fe43a8cb7b45bf9bfff7494c3ba986ce796afb35c45bbe"
                .to_string(),
            address: "0xb804de4d151e5a8a9eba61a9904ec3588c8efb56".to_string(),
            block_number: Some(10845480),
            block_hash: Some(
                "0x814cdf8c4c15539ab369ab1e591c07d6974b52f39b2cf999a6523db19b3db511".to_string(),
            ),
            block_timestamp: None,
            transaction_hash: Some(
                "0x55b0316329243a6053ffc86126d07c48cda3c667f2ea433578cd895d33145b3c".to_string(),
            ),
            transaction_index: Some(59),
            log_index: Some(206),
            topics: Vec::new(),
            data: String::new(),
            params: vec![
                address_param("growfiToken", "0x3740c0fcb8d71961b893743548e07b8d265b0a33"),
                address_param("growfiMinter", "0x369fe004842a3b3fb5285a30c63c9ba60aa4f99f"),
                address_param(
                    "growfiTreasury",
                    "0x9b7898cd64741d7a5503e8092f04fef2106c2291",
                ),
                address_param(
                    "growfiFeeSplitter",
                    "0x892bd2ab53c26b09cd874c5afeb331cac8851848",
                ),
            ],
        }
    }

    fn growfi_campaign_created_log() -> MatchedLog {
        MatchedLog {
            source: "CampaignFactory".to_string(),
            template: false,
            handler: "handleCampaignCreated".to_string(),
            signature: "CampaignCreated(indexed address,indexed address,address,address,address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256)".to_string(),
            network: Some("sepolia".to_string()),
            topic0: "0x0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            address: "0xb804de4d151e5a8a9eba61a9904ec3588c8efb56".to_string(),
            block_number: Some(10845481),
            block_hash: Some(
                "0x814cdf8c4c15539ab369ab1e591c07d6974b52f39b2cf999a6523db19b3db511".to_string(),
            ),
            block_timestamp: None,
            transaction_hash: Some(
                "0x65b0316329243a6053ffc86126d07c48cda3c667f2ea433578cd895d33145b3c".to_string(),
            ),
            transaction_index: Some(60),
            log_index: Some(207),
            topics: Vec::new(),
            data: String::new(),
            params: vec![
                address_param("campaign", "0x0000000000000000000000000000000000001001"),
                address_param("producer", "0x0000000000000000000000000000000000001002"),
                address_param("campaignToken", "0x0000000000000000000000000000000000001003"),
                address_param("yieldToken", "0x0000000000000000000000000000000000001004"),
                address_param("stakingVault", "0x0000000000000000000000000000000000001005"),
                address_param("harvestManager", "0x0000000000000000000000000000000000001006"),
                uint_param("pricePerToken", "1000000000000000000"),
                uint_param("minCap", "1"),
                uint_param("maxCap", "100"),
                uint_param("fundingDeadline", "2000000000"),
                uint_param("seasonDuration", "86400"),
                uint_param("minProductClaim", "1"),
                uint_param("createdAt", "1900000000"),
                uint_param("expectedAnnualHarvestUsd", "1000"),
                uint_param("expectedAnnualHarvest", "100"),
                uint_param("firstHarvestYear", "2026"),
                uint_param("coverageHarvests", "4"),
            ],
        }
    }

    fn address_param(name: &str, value: &str) -> DecodedEventParam {
        DecodedEventParam {
            name: Some(name.to_string()),
            kind: "address".to_string(),
            indexed: true,
            value: DecodedValue::Address(value.to_string()),
        }
    }

    fn uint_param(name: &str, value: &str) -> DecodedEventParam {
        DecodedEventParam {
            name: Some(name.to_string()),
            kind: "uint256".to_string(),
            indexed: false,
            value: DecodedValue::Uint(value.to_string()),
        }
    }

    fn schema_field(
        name: &str,
        kind: &str,
        list: bool,
        required: bool,
        derived: bool,
    ) -> EntityField {
        EntityField {
            name: name.to_string(),
            kind: kind.to_string(),
            list,
            required,
            derived,
            derived_from: None,
        }
    }
}
